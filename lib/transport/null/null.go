package null

import (
	"github.com/backube/volsync/lib/transport"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

const TypeTransportNull = "TransportNull"

type Null struct {
	listenPort  int32
	connectPort int32
	hostname    string
}

func (n *Null) NamespacedName() types.NamespacedName {
	return types.NamespacedName{}
}

func (n *Null) ListenPort() int32 {
	return n.listenPort
}

func (n *Null) ConnectPort() int32 {
	return n.connectPort
}

func (n *Null) Containers() []corev1.Container {
	return []corev1.Container{}
}

func (n *Null) Volumes() []corev1.Volume {
	return []corev1.Volume{}
}

func (n *Null) Options() *transport.Options {
	return nil
}

func (n *Null) Type() transport.Type {
	return TypeTransportNull
}

func (n *Null) Credentials() types.NamespacedName {
	return types.NamespacedName{}
}

func (n *Null) Hostname() string {
	return n.hostname
}

func NewTransport(hostname string, port int32) transport.Transport {
	n := &Null{
		listenPort:  port,
		connectPort: port,
		hostname:    hostname,
	}
	return n
}
