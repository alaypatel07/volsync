package rsync

import (
	"context"
	"fmt"
	"github.com/backube/volsync/lib/endpoint"
	"github.com/backube/volsync/lib/transfer"
	"github.com/backube/volsync/lib/transport"
	"github.com/backube/volsync/lib/transport/null"
	"github.com/backube/volsync/lib/transport/stunnel"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	errorsutil "k8s.io/apimachinery/pkg/util/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
)

type Client struct {
	pvcList transfer.PVCList

	username        string
	password        string
	transportClient transport.Transport
	endpoint        endpoint.Endpoint

	options TransferOptions
}

func (tc *Client) Transport() transport.Transport {
	return tc.transportClient
}

func (tc *Client) PVCs() []*corev1.PersistentVolumeClaim {
	pvcs := []*corev1.PersistentVolumeClaim{}
	for _, pvc := range tc.pvcList.PVCs() {
		pvcs = append(pvcs, pvc.Claim())
	}
	return pvcs
}

func NewRsyncTransferClient(c client.Client, transportClient transport.Transport, pvcList transfer.PVCList, opts ...TransferOption) (transfer.Client, error) {
	tc := &Client{
		pvcList:         pvcList,
		transportClient: transportClient,
	}

	tc.options = TransferOptions{}
	err := tc.options.Apply(opts...)
	if err != nil {
		return nil, err
	}

	err = tc.createRsyncClient(c, pvcList.GetNamespaces()[0])
	if err != nil {
		return nil, err
	}

	return tc, nil
}

func NewRsyncTransferClientWithStunnel(c client.Client, serverHostname string, serverPort int32, pvcList transfer.PVCList, opts ...TransferOption) (transfer.Client, error) {
	namespace := pvcList.GetNamespaces()
	// TODO: implement this for multiple namespaces

	tc := &Client{
		pvcList: pvcList,
	}
	tc.options = TransferOptions{}
	err := tc.options.Apply(opts...)
	if err != nil {
		return nil, err
	}

	tc.transportClient, err = stunnel.NewTransportClient(c, types.NamespacedName{Namespace: namespace[0]}, serverHostname, serverPort, &transport.Options{ObjMeta: tc.options.SourcePodMeta})
	if err != nil {
		return nil, err
	}

	err = tc.createRsyncClient(c, pvcList.GetNamespaces()[0])
	if err != nil {
		return nil, err
	}
	return tc, nil
}

func NewRsyncClientWithNullTransport(c client.Client, serverHostname string, serverPort int32, pvcList transfer.PVCList, opts ...TransferOption) (transfer.Client, error) {
	tc := &Client{
		pvcList:         pvcList,
		transportClient: null.NewTransport(serverHostname, serverPort),
	}
	tc.options = TransferOptions{}
	err := tc.options.Apply(opts...)
	if err != nil {
		return nil, err
	}

	err = tc.createRsyncClient(c, pvcList.GetNamespaces()[0])
	if err != nil {
		return nil, err
	}
	return tc, nil
}

func createRsyncClientResources(c client.Client, ns string) error {
	// no resource are created for rsync client side
	return nil
}

func (tc *Client) createRsyncClient(c client.Client, ns string) error {
	var errs []error

	podList := &corev1.PodList{}
	err := c.List(context.Background(), podList, client.MatchingLabels(tc.options.SourcePodMeta.Labels()))
	if err != nil {
		return err
	}

	if len(podList.Items) > 0 {
		return nil
	}

	transferOptions := tc.options
	rsyncOptions, err := transferOptions.AsRsyncCommandOptions()
	if err != nil {
		return err
	}
	for _, pvc := range tc.pvcList.InNamespace(ns).PVCs() {
		// create Rsync command for PVC
		rsyncContainerCommand := tc.getRsyncCommand(rsyncOptions, transferOptions, pvc)
		// create rsync container
		containers := []corev1.Container{
			{
				Name:    RsyncContainer,
				Image:   rsyncImage,
				Command: rsyncContainerCommand,
				Env: []corev1.EnvVar{
					{
						Name:  "RSYNC_PASSWORD",
						Value: transferOptions.password,
					},
				},

				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      "mnt",
						MountPath: getMountPathForPVC(pvc),
					},
					{
						Name:      "rsync-communication",
						MountPath: "/usr/share/rsync",
					},
				},
			},
		}
		// attach transport containers
		customizeTransportClientContainers(tc.Transport())
		containers = append(containers, tc.Transport().Containers()...)
		// apply container mutations
		for i := range containers {
			c := &containers[i]
			applyContainerMutations(c, tc.options.SourceContainerMutations)
		}

		volumes := []corev1.Volume{
			{
				Name: "mnt",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvc.Claim().Name,
					},
				},
			},
			{
				Name: "rsync-communication",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumDefault},
				},
			},
		}
		volumes = append(volumes, tc.Transport().Volumes()...)
		podSpec := corev1.PodSpec{
			Containers:    containers,
			Volumes:       volumes,
			RestartPolicy: corev1.RestartPolicyNever,
		}

		applyPodMutations(&podSpec, tc.options.SourcePodMutations)

		pod := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName:    "rsync-",
				Namespace:       pvc.Claim().Namespace,
				Labels:          tc.options.SourcePodMeta.Labels(),
				OwnerReferences: tc.options.SourcePodMeta.OwnerReferences(),
			},
			Spec: podSpec,
		}

		err := c.Create(context.TODO(), &pod, &client.CreateOptions{})
		errs = append(errs, err)
	}

	return errorsutil.NewAggregate(errs)
}

func (tc *Client) getRsyncCommand(rsyncOptions []string, transferOptions TransferOptions, pvc transfer.PVC) []string {
	if tc.Transport().Type() == null.TypeTransportNull {
		rsyncCommand := []string{"/usr/bin/rsync"}
		rsyncCommand = append(rsyncCommand, rsyncOptions...)
		rsyncCommand = append(rsyncCommand, fmt.Sprintf("%s/", getMountPathForPVC(pvc)))
		rsyncCommand = append(rsyncCommand,
			fmt.Sprintf("rsync://%s@%s/%s --port %d",
				transferOptions.username,
				tc.Transport().Hostname(),
				pvc.LabelSafeName(), tc.Transport().ListenPort()))
		rsyncContainerCommand := []string{
			"/bin/bash",
			"-c",
			strings.Join(rsyncCommand, " "),
		}
		return rsyncContainerCommand
	}
	rsyncCommand := []string{"/usr/bin/rsync"}
	rsyncCommand = append(rsyncCommand, rsyncOptions...)
	rsyncCommand = append(rsyncCommand, fmt.Sprintf("%s/", getMountPathForPVC(pvc)))
	rsyncCommand = append(rsyncCommand,
		fmt.Sprintf("rsync://%s@%s/%s --port %d",
			transferOptions.username,
			tc.Transport().Hostname(),
			pvc.LabelSafeName(), tc.Transport().ListenPort()))
	rsyncCommandBashScript := fmt.Sprintf(
		"trap \"touch /usr/share/rsync/rsync-client-container-done\" EXIT SIGINT SIGTERM; timeout=120; SECONDS=0; while [ $SECONDS -lt $timeout ]; do nc -z localhost %d; rc=$?; if [ $rc -eq 0 ]; then %s; rc=$?; break; fi; done; exit $rc;",
		tc.Transport().ListenPort(),
		strings.Join(rsyncCommand, " "))
	rsyncContainerCommand := []string{
		"/bin/bash",
		"-c",
		rsyncCommandBashScript,
	}
	return rsyncContainerCommand
}

// customizeTransportClientContainers customizes transport's client containers for specific rsync communication
func customizeTransportClientContainers(transportClient transport.Transport) {
	switch transportClient.Type() {
	case stunnel.TransportTypeStunnel:
		var stunnelContainer *corev1.Container
		for i := range transportClient.Containers() {
			c := &transportClient.Containers()[i]
			if c.Name == stunnel.Container {
				stunnelContainer = c
			}
		}
		stunnelContainer.Command = []string{
			"/bin/bash",
			"-c",
			`/bin/stunnel /etc/stunnel/stunnel.conf
while true
do test -f /usr/share/rsync/rsync-client-container-done
if [ $? -eq 0 ]
then
break
fi
done
exit 0`,
		}
		stunnelContainer.VolumeMounts = append(
			stunnelContainer.VolumeMounts,
			corev1.VolumeMount{
				Name:      "rsync-communication",
				MountPath: "/usr/share/rsync",
			})
	}
}
