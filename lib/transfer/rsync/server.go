package rsync

import (
	"bytes"
	"context"
	"fmt"
	"github.com/backube/volsync/lib/endpoint"
	"github.com/backube/volsync/lib/endpoint/loadbalancer"
	"github.com/backube/volsync/lib/endpoint/route"
	"github.com/backube/volsync/lib/transport"
	"github.com/backube/volsync/lib/transport/null"
	"github.com/backube/volsync/lib/transport/stunnel"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"text/template"
	"time"

	"github.com/backube/volsync/lib/transfer"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	rsyncServerConfTemplate = `syslog facility = local7
read only = no
list = yes
log file = /dev/stdout
max verbosity = 4
auth users = {{ $.Username }}
{{- if $.AllowLocalhostOnly }}
hosts allow = ::1, 127.0.0.1, localhost
{{- else }}
hosts allow = *.*.*.*, *
{{- end }}
uid = root
gid = root
{{ range $i, $pvc := .PVCList }}
[{{ $pvc.LabelSafeName }}]
    comment = archive for {{ $pvc.Claim.Namespace }}/{{ $pvc.Claim.Name }}
    path = /mnt/{{ $pvc.Claim.Namespace }}/{{ $pvc.LabelSafeName }}
    use chroot = no
    munge symlinks = no
    list = yes
    read only = false
    auth users = {{ $.Username }}
    secrets file = /etc/rsync-secret/rsyncd.secrets
{{ end }}
`
)

const (
	NullTransportIngressPort = 8080
	NullTransportBackendPort = 2222
)

type rsyncConfigData struct {
	Username           string
	PVCList            transfer.PVCList
	AllowLocalhostOnly bool
}

type TransferServer struct {
	//username        string
	password        string
	pvcList         transfer.PVCList
	transportServer transport.Transport
	endpoint        endpoint.Endpoint
	listenPort      int32
	options         TransferOptions
}

func (r *TransferServer) Endpoint() endpoint.Endpoint {
	return r.endpoint
}

func (r *TransferServer) Transport() transport.Transport {
	return r.transportServer
}

func (r *TransferServer) IsHealthy(c client.Client) (bool, error) {
	return transfer.IsPodHealthy(c, client.ObjectKey{Namespace: r.pvcList.GetNamespaces()[0], Name: "rsync-server"})
}

func (r *TransferServer) PVCs() []*corev1.PersistentVolumeClaim {
	pvcs := []*corev1.PersistentVolumeClaim{}
	for _, pvc := range r.pvcList.PVCs() {
		pvcs = append(pvcs, pvc.Claim())
	}
	return pvcs
}

func (r *TransferServer) ListenPort() int32 {
	return r.listenPort
}

// transferOptions returns options used for the transfer
func (r *TransferServer) transferOptions() TransferOptions {
	return r.options
}

func NewRsyncTransferServer(c client.Client,
	pvcList transfer.PVCList,
	t transport.Transport,
	e endpoint.Endpoint,
	options TransferOptions) (transfer.Server, error) {

	r := &TransferServer{
		pvcList:         pvcList,
		transportServer: t,
		endpoint:        e,
		options:         options,
		listenPort:      t.ConnectPort(),
	}
	// TODO: create a func to loop through all pvc and check if they are in 1 namespace
	namespace := pvcList.GetNamespaces()[0]
	err := r.createConfig(c, namespace)
	if err != nil {
		return nil, err
	}

	err = r.createSecret(c, namespace)
	if err != nil {
		return nil, err
	}

	err = r.createRsyncServer(c, namespace)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func NewRsyncTransferServerWithStunnel(c client.Client,
	pvcList transfer.PVCList,
	opts ...TransferOption) (transfer.Server, error) {

	// TODO: implement this for multiple namespaces
	namespace := pvcList.GetNamespaces()[0]

	options := TransferOptions{}
	err := options.Apply(opts...)
	if err != nil {
		return nil, err
	}

	e, err := route.NewEndpoint(c, types.NamespacedName{
		Namespace: namespace,
		Name:      *options.DestinationPodMeta.Name(),
	}, route.EndpointTypePassthrough, options.DestinationPodMeta)
	if err != nil {
		return nil, err
	}

	routeHealthy, err := e.IsHealthy(c)
	if !routeHealthy {
		return nil, fmt.Errorf("waiting for the route to be healthy")
	}

	t, err := stunnel.NewTransportServer(c, types.NamespacedName{
		Namespace: namespace,
		Name:      *options.DestinationPodMeta.Name(),
	}, e, &transport.Options{ObjMeta: options.DestinationPodMeta})
	if err != nil {
		return nil, err
	}

	return NewRsyncTransferServer(c, pvcList, t, e, options)
}

func NewRsyncTransferServerWithNull(c client.Client,
	pvcList transfer.PVCList,
	opts ...TransferOption) (transfer.Server, error) {
	// TODO: implement this for multiple namespaces
	namespace := pvcList.GetNamespaces()[0]

	options := TransferOptions{}
	err := options.Apply(opts...)
	if err != nil {
		return nil, err
	}

	e, err := loadbalancer.NewEndpoint(c, types.NamespacedName{
		Namespace: namespace,
		Name:      *options.DestinationPodMeta.Name(),
	}, options.DestinationPodMeta, NullTransportBackendPort, NullTransportIngressPort)
	if err != nil {
		return nil, err
	}

	loadBalancerHealthy, err := e.IsHealthy(c)
	if !loadBalancerHealthy {
		return nil, fmt.Errorf("waiting for the load balancer to be healthy")
	}

	t := null.NewTransport(e.Hostname(), e.BackendPort())

	return NewRsyncTransferServer(c, pvcList, t, e, options)
}

func (r *TransferServer) createConfig(c client.Client, namespace string) error {
	var rsyncConf bytes.Buffer
	rsyncConfTemplate, err := template.New("config").Parse(rsyncServerConfTemplate)
	if err != nil {
		return err
	}

	allowLocalhostOnly := r.Transport().Type() == stunnel.TransportTypeStunnel
	configdata := rsyncConfigData{
		Username:           r.options.username,
		PVCList:            r.pvcList.InNamespace(namespace),
		AllowLocalhostOnly: allowLocalhostOnly,
	}

	err = rsyncConfTemplate.Execute(&rsyncConf, configdata)
	if err != nil {
		return err
	}

	rsyncConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       namespace,
			Name:            rsyncConfig,
			Labels:          r.transferOptions().DestinationPodMeta.Labels(),
			OwnerReferences: r.transferOptions().DestinationPodMeta.OwnerReferences(),
		},
		Data: map[string]string{
			"rsyncd.conf": rsyncConf.String(),
		},
	}
	err = c.Create(context.TODO(), rsyncConfigMap, &client.CreateOptions{})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func (r *TransferServer) createSecret(c client.Client, ns string) error {
	if r.password != "" {
		var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
		rand.Seed(time.Now().UnixNano())
		password := make([]byte, 24)
		for i := range password {
			password[i] = letters[rand.Intn(len(letters))]
		}
		r.password = string(password)
	}

	rsyncSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			Name:            rsyncSecretPrefix,
			Labels:          r.transferOptions().DestinationPodMeta.Labels(),
			OwnerReferences: r.transferOptions().DestinationPodMeta.OwnerReferences(),
		},
		Data: map[string][]byte{
			"credentials": []byte(r.transferOptions().username + ":" + r.transferOptions().password),
		},
	}
	err := c.Create(context.TODO(), rsyncSecret, &client.CreateOptions{})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return err
	}

	return nil
}

func (r *TransferServer) createRsyncServer(c client.Client, ns string) error {
	transferOptions := r.transferOptions()
	podLabels := transferOptions.DestinationPodMeta.Labels()

	volumeMounts := []corev1.VolumeMount{}
	configVolumeMounts := getConfigVolumeMounts()
	pvcVolumeMounts := r.getPVCVolumeMounts(ns)

	volumeMounts = append(volumeMounts, configVolumeMounts...)
	volumeMounts = append(volumeMounts, pvcVolumeMounts...)
	containers := r.getPodTemplate(volumeMounts)

	containers = append(containers, r.Transport().Containers()...)
	// apply container mutations
	for i := range containers {
		c := &containers[i]
		applyContainerMutations(c, r.options.DestContainerMutations)
	}

	mode := int32(0600)

	configVolumes := getConfigVolumes(mode)
	pvcVolumes := r.getPVCVolumes(ns)

	volumes := append(pvcVolumes, configVolumes...)
	volumes = append(volumes, r.Transport().Volumes()...)

	podSpec := corev1.PodSpec{
		Containers: containers,
		Volumes:    volumes,
	}

	applyPodMutations(&podSpec, r.options.DestinationPodMutations)

	server := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "rsync-server",
			Namespace:       ns,
			Labels:          podLabels,
			OwnerReferences: r.options.DestinationPodMeta.OwnerReferences(),
		},
		Spec: podSpec,
	}

	err := c.Create(context.TODO(), server, &client.CreateOptions{})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func getConfigVolumes(mode int32) []corev1.Volume {
	return []corev1.Volume{
		{
			Name: rsyncConfig,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: rsyncConfig,
					},
				},
			},
		},
		{
			Name: rsyncSecretPrefix,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  rsyncSecretPrefix,
					DefaultMode: &mode,
					Items: []corev1.KeyToPath{
						{
							Key:  "credentials",
							Path: "rsyncd.secrets",
						},
					},
				},
			},
		},
	}
}

func (r *TransferServer) getPVCVolumeMounts(ns string) []corev1.VolumeMount {
	pvcVolumeMounts := []corev1.VolumeMount{}
	for _, pvc := range r.pvcList.InNamespace(ns).PVCs() {
		pvcVolumeMounts = append(
			pvcVolumeMounts,
			corev1.VolumeMount{
				Name:      pvc.LabelSafeName(),
				MountPath: fmt.Sprintf("/mnt/%s/%s", pvc.Claim().Namespace, pvc.LabelSafeName()),
			})
	}
	return pvcVolumeMounts
}

func (r *TransferServer) getPodTemplate(volumeMounts []corev1.VolumeMount) []corev1.Container {
	return []corev1.Container{
		{
			Name:  RsyncContainer,
			Image: rsyncImage,
			Command: []string{
				"/usr/bin/rsync",
				"--daemon",
				"--no-detach",
				fmt.Sprintf("--port=%d", r.ListenPort()),
				"-vvv",
			},
			Ports: []corev1.ContainerPort{
				{
					Name:          "rsyncd",
					Protocol:      corev1.ProtocolTCP,
					ContainerPort: r.ListenPort(),
				},
			},
			VolumeMounts: volumeMounts,
		},
	}
}

func (r *TransferServer) getPVCVolumes(ns string) []corev1.Volume {
	pvcVolumes := []corev1.Volume{}
	for _, pvc := range r.pvcList.InNamespace(ns).PVCs() {
		pvcVolumes = append(
			pvcVolumes,
			corev1.Volume{
				Name: pvc.LabelSafeName(),
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvc.Claim().Name,
					},
				},
			},
		)
	}
	return pvcVolumes
}

func getConfigVolumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      rsyncConfig,
			MountPath: "/etc/rsyncd.conf",
			SubPath:   "rsyncd.conf",
		},
		{
			Name:      rsyncSecretPrefix,
			MountPath: "/etc/rsync-secret",
		},
	}
}
