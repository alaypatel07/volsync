package loadbalancer

import (
	"context"
	"github.com/backube/volsync/lib/endpoint"
	"github.com/backube/volsync/lib/meta"
	"github.com/backube/volsync/lib/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type Endpoint struct {
	hostname       string
	ingressPort    int32
	backendPort    int32
	namespacedName types.NamespacedName
	objMeta        meta.ObjectMetaMutation
}

func (e *Endpoint) MarkForCleanup(c client.Client, key, value string) error {
	// mark service for deletion
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      e.namespacedName.Name,
			Namespace: e.namespacedName.Namespace,
		},
	}
	return utils.UpdateWithLabel(c, svc, key, value)
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

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      e.NamespacedName().Name,
			Namespace: e.NamespacedName().Namespace,
		},
	}

	// TODO: log the return operation from CreateOrUpdate
	_, err := controllerutil.CreateOrUpdate(context.TODO(), c, service, func() error {
		if service.CreationTimestamp.IsZero() {
			service.Spec = corev1.ServiceSpec{
				Ports: []corev1.ServicePort{
					{
						Name:     e.NamespacedName().Name,
						Protocol: corev1.ProtocolTCP,
						Port:     e.IngressPort(),
						TargetPort: intstr.IntOrString{
							Type:   intstr.Int,
							IntVal: e.BackendPort(),
						},
					},
				},
				Selector: serviceSelector,
				Type:     corev1.ServiceTypeLoadBalancer,
			}
		}

		service.Labels = e.objMeta.Labels()
		service.OwnerReferences = e.objMeta.OwnerReferences()
		return nil
	})

	return err
}
