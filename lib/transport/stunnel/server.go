package stunnel

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"text/template"

	"github.com/backube/volsync/lib/endpoint"
	"github.com/backube/volsync/lib/transport"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	errorsutil "k8s.io/apimachinery/pkg/util/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	stunnelServerConfTemplate = `foreground = yes
pid =
socket = l:TCP_NODELAY=1
socket = r:TCP_NODELAY=1
debug = 7
sslVersion = TLSv1.2
[rsync]
accept = {{ $.acceptPort }}
connect = {{ $.connectPort }}
key = /etc/stunnel/certs/tls.key
cert = /etc/stunnel/certs/tls.crt
TIMEOUTclose = 0
`
	stunnelConnectPort = 8080
)

func (s *Server) createStunnelServerConfig(c client.Client) error {
	ports := map[string]string{
		// listenPort on which Stunnel service listens on, must connect with endpoint
		"acceptPort": strconv.Itoa(int(s.ListenPort())),
		// listenPort in the container on which Transfer is listening on
		"connectPort": strconv.Itoa(int(s.ConnectPort())),
	}

	var stunnelConf bytes.Buffer
	stunnelConfTemplate, err := template.New("config").Parse(stunnelServerConfTemplate)
	if err != nil {
		return err
	}

	err = stunnelConfTemplate.Execute(&stunnelConf, ports)
	if err != nil {
		return err
	}

	stunnelConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       s.NamespacedName().Namespace,
			Name:            stunnelConfig,
			Labels:          s.options.ObjMeta.Labels(),
			OwnerReferences: s.options.ObjMeta.OwnerReferences(),
		},
		Data: map[string]string{
			"stunnel.conf": stunnelConf.String(),
		},
	}

	err = c.Create(context.TODO(), stunnelConfigMap, &client.CreateOptions{})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func (s *Server) getServerConfig(c client.Client, namespace string) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{}
	err := c.Get(context.Background(), types.NamespacedName{
		Namespace: namespace,
		Name:      stunnelConfig,
	}, cm)
	return cm, err
}

func (s *Server) createStunnelServerSecret(c client.Client) error {
	_, crt, key, err := transport.GenerateSSLCert()
	if err != nil {
		return err
	}

	stunnelSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       s.NamespacedName().Namespace,
			Name:            stunnelSecret,
			Labels:          s.options.ObjMeta.Labels(),
			OwnerReferences: s.options.ObjMeta.OwnerReferences(),
		},
		Data: map[string][]byte{
			"tls.crt": crt.Bytes(),
			"tls.key": key.Bytes(),
		},
	}

	err = c.Create(context.TODO(), stunnelSecret, &client.CreateOptions{})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func (s *Server) getServerSecret(c client.Client, namespace string) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	err := c.Get(context.Background(), types.NamespacedName{
		Namespace: namespace,
		Name:      stunnelSecret,
	}, secret)
	return secret, err
}

func (s *Server) createStunnelServerContainers(listenPort int32) []corev1.Container {
	return []corev1.Container{
		{
			Name:  Container,
			Image: stunnelImage,
			Command: []string{
				"/bin/stunnel",
				"/etc/stunnel/stunnel.conf",
			},
			Ports: []corev1.ContainerPort{
				{
					Name:          "stunnel",
					Protocol:      corev1.ProtocolTCP,
					ContainerPort: listenPort,
				},
			},
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      stunnelConfig,
					MountPath: "/etc/stunnel/stunnel.conf",
					SubPath:   "stunnel.conf",
				},
				{
					Name:      stunnelSecret,
					MountPath: "/etc/stunnel/certs",
				},
			},
		},
	}
}

func (s *Server) createStunnelServerVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name: stunnelConfig,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: stunnelConfig,
					},
				},
			},
		},
		{
			Name: stunnelSecret,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: stunnelSecret,
					Items: []corev1.KeyToPath{
						{
							Key:  "tls.crt",
							Path: "tls.crt",
						},
						{
							Key:  "tls.key",
							Path: "tls.key",
						},
					},
				},
			},
		},
	}
}

type Server struct {
	crt         *bytes.Buffer
	key         *bytes.Buffer
	ca          *bytes.Buffer
	listenPort  int32
	connectPort int32

	containers []corev1.Container
	volumes    []corev1.Volume

	direct  bool
	options *transport.Options
	//noVerifyCA    bool
	//caVerifyLevel string

	namespacedName types.NamespacedName
}

func (s *Server) NamespacedName() types.NamespacedName {
	return s.namespacedName
}

func (s *Server) CA() *bytes.Buffer {
	return s.ca
}

func (s *Server) Crt() *bytes.Buffer {
	return s.crt
}

func (s *Server) Key() *bytes.Buffer {
	return s.key
}

func (s *Server) ListenPort() int32 {
	return s.listenPort
}

func (s *Server) ConnectPort() int32 {
	return s.connectPort
}

func (s *Server) Containers() []corev1.Container {
	return s.containers
}

func (s *Server) Volumes() []corev1.Volume {
	return s.volumes
}

func (s *Server) Direct() bool {
	return s.direct
}

func (s *Server) Options() *transport.Options {
	return s.options
}

func (s *Server) Type() transport.Type {
	return TransportTypeStunnel
}

func (s *Server) Credentials() types.NamespacedName {
	return types.NamespacedName{Name: stunnelSecret, Namespace: s.NamespacedName().Namespace}
}

func (s *Server) Hostname() string {
	return "localhost"
}

func NewTransportServer(c client.Client,
	namespacedName types.NamespacedName,
	e endpoint.Endpoint,
	options *transport.Options) (transport.Transport, error) {

	transferPort := e.BackendPort()

	s := &Server{
		namespacedName: namespacedName,
		options:        options,
		listenPort:     transferPort,
		connectPort:    stunnelConnectPort,
	}
	errs := []error{}

	err := s.createStunnelServerConfig(c)
	errs = append(errs, err)

	err = s.createStunnelServerSecret(c)
	errs = append(errs, err)

	err = s.setFields(c, e)
	errs = append(errs, err)

	return s, errorsutil.NewAggregate(errs)
}

// setFields checks if the required configmaps and secrets are created for the transport
// It populates the fields for the Server needed for transfer object.
func (s *Server) setFields(c client.Client,
	e endpoint.Endpoint) error {
	_, err := s.getServerConfig(c, s.NamespacedName().Namespace)
	switch {
	case k8serrors.IsNotFound(err):
		return err
	case err != nil:
		return err
	}

	serverSecret, err := s.getServerSecret(c, s.NamespacedName().Namespace)
	switch {
	case k8serrors.IsNotFound(err):
		fmt.Printf("transport: %s/%s Transport secret is not created", s.NamespacedName().Namespace, s.NamespacedName().Name)
		return err
	case err != nil:
		return err
	}

	key, ok := serverSecret.Data["tls.key"]
	if !ok {
		fmt.Printf("invalid secret for transport %s/%s, tls.key key not found", s.NamespacedName().Namespace, s.NamespacedName().Name)
		return fmt.Errorf("invalid secret for transport %s/%s, tls.key key not found", s.NamespacedName().Namespace, s.NamespacedName().Name)
	}

	crt, ok := serverSecret.Data["tls.crt"]
	if !ok {
		fmt.Printf("invalid secret for transport %s/%s, tls.crt key not found", s.NamespacedName().Namespace, s.NamespacedName().Name)
		return fmt.Errorf("invalid secret for transport %s/%s, tls.crt key not found", s.NamespacedName().Namespace, s.NamespacedName().Name)
	}

	s.key = bytes.NewBuffer(key)
	s.crt = bytes.NewBuffer(crt)

	s.volumes = s.createStunnelServerVolumes()
	s.containers = s.createStunnelServerContainers(s.ListenPort())
	s.direct = true

	return nil
}
