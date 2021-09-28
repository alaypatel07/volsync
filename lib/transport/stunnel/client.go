package stunnel

import (
	"bytes"
	"context"
	"fmt"
	"github.com/backube/volsync/lib/utils"
	"strconv"
	"text/template"

	"github.com/backube/volsync/lib/transport"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const ClientListenPort = 6443

const (
	stunnelClientConfTemplate = `
 pid =
 sslVersion = TLSv1.2
 client = yes
 syslog = no
 output = /dev/stdout
 [rsync]
 debug = 7
 accept = {{ .listenPort }}
 cert = /etc/stunnel/certs/tls.crt
 key = /etc/stunnel/certs/tls.key
{{- if not (eq .proxyHost "") }}
 protocol = connect
 connect = {{ .proxyHost }}
 protocolHost = {{ .hostname }}:{{ .listenPort }}
{{- if not (eq .proxyUsername "") }}
 protocolUsername = {{ .proxyUsername }}
{{- end }}
{{- if not (eq .proxyPassword "") }}
 protocolPassword = {{ .proxyPassword }}
{{- end }}
{{- else }}
 connect = {{ .hostname }}:{{ .connectPort }}
{{- end }}
{{- if not (eq .noVerifyCA "false") }}
 verify = {{ .caVerifyLevel }}
{{- end }}
`
)

type Client struct {
	crt        *bytes.Buffer
	key        *bytes.Buffer
	ca         *bytes.Buffer
	listenPort int32

	credentialsSecretName string

	containers []corev1.Container
	volumes    []corev1.Volume

	direct  bool
	options *transport.Options
	//noVerifyCA    bool
	//caVerifyLevel string

	serverHostname string
	ingressPort    int32

	namespacedName types.NamespacedName
}

func (t *Client) MarkForCleanup(c client.Client, key, value string) error {
	// update configmap
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      stunnelConfig,
			Namespace: t.NamespacedName().Namespace,
		},
	}
	return utils.UpdateWithLabel(c, cm, key, value)
}

func (t *Client) NamespacedName() types.NamespacedName {
	return t.namespacedName
}

func (t *Client) ConnectPort() int32 {
	return t.ingressPort
}

func (t *Client) ListenPort() int32 {
	return t.listenPort
}

func (t *Client) Containers() []corev1.Container {
	return t.containers
}

func (t *Client) Volumes() []corev1.Volume {
	return t.volumes
}

func (t *Client) Direct() bool {
	return t.direct
}

func (t *Client) Options() *transport.Options {
	return t.options
}

func (t *Client) Type() transport.Type {
	return TransportTypeStunnel
}

func (t *Client) Credentials() types.NamespacedName {
	return types.NamespacedName{
		Namespace: t.namespacedName.Namespace,
		Name:      stunnelSecret,
	}
}

func (t *Client) Hostname() string {
	return "localhost"
}

// NewTransportClient creates the container for the transfer
func NewTransportClient(c client.Client,
	namespacedName types.NamespacedName,
	hostname string,
	ingressPort int32,
	options *transport.Options) (transport.Transport, error) {
	transportClient := &Client{
		namespacedName: namespacedName,
		options:        options,
		ingressPort:    ingressPort,
		serverHostname: hostname,
		listenPort:     ClientListenPort,
	}

	err := transportClient.createClientConfig(c, hostname, transportClient.ListenPort(), transportClient.ConnectPort())
	if err != nil {
		return nil, err
	}

	err = transportClient.credentials(c)
	if err != nil {
		return nil, err
	}

	transportClient.clientContainers(transportClient.ListenPort())
	transportClient.clientVolumes()

	return transportClient, nil
}

func (t *Client) createClientConfig(c client.Client, hostname string, listenPort, connectPort int32) error {
	var caVerifyLevel string

	if t.Options().CAVerifyLevel == "" {
		caVerifyLevel = "2"
	} else {
		caVerifyLevel = t.Options().CAVerifyLevel
	}

	connections := map[string]string{
		"listenPort":    strconv.Itoa(int(listenPort)),
		"hostname":      hostname,
		"connectPort":   strconv.Itoa(int(connectPort)),
		"proxyHost":     t.Options().ProxyURL,
		"proxyUsername": t.Options().ProxyUsername,
		"proxyPassword": t.Options().ProxyPassword,
		"caVerifyLevel": caVerifyLevel,
		"noVerifyCA":    strconv.FormatBool(t.Options().NoVerifyCA),
	}

	var stunnelConf bytes.Buffer
	stunnelConfTemplate, err := template.New("config").Parse(stunnelClientConfTemplate)
	if err != nil {
		return err
	}

	err = stunnelConfTemplate.Execute(&stunnelConf, connections)
	if err != nil {
		return err
	}

	stunnelConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       t.NamespacedName().Namespace,
			Name:            stunnelConfig,
			Labels:          t.Options().ObjMeta.Labels(),
			OwnerReferences: t.Options().ObjMeta.OwnerReferences(),
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

func (t *Client) credentials(c client.Client) error {
	secret := &corev1.Secret{}
	err := c.Get(context.Background(), types.NamespacedName{
		Namespace: t.NamespacedName().Namespace,
		Name:      stunnelSecret,
	}, secret)

	if err != nil {
		return err
	}

	if key, ok := secret.Data["tls.key"]; ok {
		t.key = bytes.NewBuffer(key)
	} else {
		return fmt.Errorf("invalid credentaials secret, does not have the key")
	}

	if crt, ok := secret.Data["tls.crt"]; ok {
		t.key = bytes.NewBuffer(crt)
	} else {
		return fmt.Errorf("invalid credentaials secret, does not have the crt")
	}

	return nil
}

func (t *Client) clientContainers(listenPort int32) {
	t.containers = []corev1.Container{
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

func (t *Client) clientVolumes() {
	t.volumes = []corev1.Volume{
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
