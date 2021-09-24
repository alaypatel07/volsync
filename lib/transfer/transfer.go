package transfer

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/backube/volsync/lib/endpoint"
	"github.com/backube/volsync/lib/transport"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	errorsutil "k8s.io/apimachinery/pkg/util/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Transfer knows how to transfer PV data from a source to a destination
// Server creates an rsync server on the destination
type Server interface {
	// Endpoint returns the endpoint used by the transfer
	Endpoint() endpoint.Endpoint
	// Transport returns the transport used by the transfer
	Transport() transport.Transport
	// ListenPort returns the port on which transfer server pod is listening on
	ListenPort() int32
	// IsHealthy returns whether or not all Kube resources used by endpoint are healthy
	IsHealthy(c client.Client) (bool, error)
	// Completed returns whether or not the current attempt of transfer is completed
	Completed(c client.Client) (bool, error)
	// PVCs returns the list of PVCs the transfer will migrate
	PVCs() []*corev1.PersistentVolumeClaim
	// MarkForCleanup add the required labels to all the resources for
	// cleaning up
	MarkForCleanup(c client.Client, key, value string) error
}

type Client interface {
	// Transport returns the transport used by the transfer
	Transport() transport.Transport
	// PVCs returns the list of PVCs the transfer will migrate
	PVCs() []*corev1.PersistentVolumeClaim
	// IsCompleted returns whether the client is done
	Status(c client.Client) (*Status, error)
	// MarkForCleanup adds a key-value label to all the resources to be cleaned up
	MarkForCleanup(c client.Client, key, value string) error
}

type Status struct {
	Running   *Running
	Completed *Completed
}

type Running struct {
	StartedAt *metav1.Time
}

type Completed struct {
	Successful bool
	Failure    bool
	FinishedAt *metav1.Time
}

// IsPodHealthy is a utility function that can be used by various
// implementations to check if the server pod deployed is healthy
func IsPodHealthy(c client.Client, pod client.ObjectKey) (bool, error) {
	p := &corev1.Pod{}

	err := c.Get(context.Background(), pod, p)
	if err != nil {
		return false, err
	}

	return areContainersReady(p)
}

// IsPodCompleted is a utility function that can be used by various
// implementations to check if the server pod deployed is completed.
// if containerName is empty string then it will check for completion of
// all the containers
func IsPodCompleted(c client.Client, podKey client.ObjectKey, containerName string) (bool, error) {
	pod := &corev1.Pod{}
	err := c.Get(context.Background(), podKey, pod)
	if err != nil {
		return false, err
	}

	if len(pod.Status.ContainerStatuses) != 2 {
		return false, fmt.Errorf("expected two contaier statuses found %d, for pod %s",
			len(pod.Status.ContainerStatuses), client.ObjectKey{Namespace: pod.Namespace, Name: pod.Name})
	}

	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerName != "" && containerStatus.Name == containerName {
			return containerStatus.State.Terminated != nil, nil
		} else {
			if containerStatus.State.Terminated == nil {
				return false, nil
			}
		}
	}
	return true, nil
}

func areContainersReady(pod *corev1.Pod) (bool, error) {
	if len(pod.Status.ContainerStatuses) != 2 {
		return false, fmt.Errorf("expected two contaier statuses found %d, for pod %s",
			len(pod.Status.ContainerStatuses), client.ObjectKey{Namespace: pod.Namespace, Name: pod.Name})
	}

	for _, containerStatus := range pod.Status.ContainerStatuses {
		if !containerStatus.Ready {
			return false, fmt.Errorf("container %s in pod %s is not ready",
				containerStatus.Name, client.ObjectKey{Namespace: pod.Namespace, Name: pod.Name})
		}
	}
	return true, nil
}

// AreFilteredPodsHealthy is a utility function that can be used by various
// implementations to check if the server pods deployed with some label selectors
// are healthy. If atleast 1 replica will be healthy the function will return true
func AreFilteredPodsHealthy(c client.Client, namespace string, labels fields.Set) (bool, error) {
	pList := &corev1.PodList{}

	err := c.List(context.Background(), pList, client.InNamespace(namespace), client.MatchingFields(labels))
	if err != nil {
		return false, err
	}

	errs := []error{}

	for i := range pList.Items {
		podReady, err := areContainersReady(&pList.Items[i])
		if err != nil {
			errs = append(errs, err)
		}
		if podReady {
			return true, nil
		}
	}

	return false, errorsutil.NewAggregate(errs)
}
