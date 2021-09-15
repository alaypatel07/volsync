package loadbalancer

import (
	"context"
	"github.com/backube/volsync/lib/endpoint"
	"github.com/backube/volsync/lib/meta"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Endpoint struct {
	hostname       string
	ingressPort    int32
	backendPort    int32
	namespacedName types.NamespacedName
	objMeta        meta.ObjectMetaMutation
}

func (e *Endpoint) NamespacedName() types.NamespacedName {
	return e.namespacedName
}

func (e *Endpoint) Hostname() string {
	return e.hostname
}

func (e *Endpoint) BackendPort() int32 {
	return e.backendPort
}

func (e *Endpoint) IngressPort() int32 {
	return e.ingressPort
}

func (e *Endpoint) IsHealthy(c client.Client) (bool, error) {
	svc := &corev1.Service{}
	err := c.Get(context.Background(), e.NamespacedName(), svc)
	if err != nil {
		return false, err
	}

	if len(svc.Status.LoadBalancer.Ingress) > 0 {
		if svc.Status.LoadBalancer.Ingress[0].Hostname != "" {
			e.hostname = svc.Status.LoadBalancer.Ingress[0].Hostname
		}
		if svc.Status.LoadBalancer.Ingress[0].IP != "" {
			e.hostname = svc.Status.LoadBalancer.Ingress[0].IP
		}
		return true, nil
	}
	return false, nil
}

func NewEndpoint(c client.Client,
	name types.NamespacedName,
	metaMutation meta.ObjectMetaMutation,
	backendPort, ingressPort int32) (endpoint.Endpoint, error) {
	s := &Endpoint{
		namespacedName: name,
		objMeta:        metaMutation,
		backendPort:    backendPort,
		ingressPort:    ingressPort,
	}

	err := s.createService(c)
	if err != nil {
		return nil, err
	}

	return s, err
}

func (e *Endpoint) createService(c client.Client) error {
	serviceSelector := e.objMeta.Labels()

	service := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            e.NamespacedName().Name,
			Namespace:       e.NamespacedName().Namespace,
			Labels:          e.objMeta.Labels(),
			OwnerReferences: e.objMeta.OwnerReferences(),
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:     e.NamespacedName().Name,
					Protocol: corev1.ProtocolTCP,
					Port:     e.ingressPort,
					TargetPort: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: e.BackendPort(),
					},
				},
			},
			Selector: serviceSelector,
			Type:     corev1.ServiceTypeLoadBalancer,
		},
	}

	err := c.Create(context.Background(), &service)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return nil
}
