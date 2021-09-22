package rsyncwithstunnel

import (
	"context"
	"errors"
	"fmt"
	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/controllers/mover"
	"github.com/backube/volsync/controllers/utils"
	"github.com/backube/volsync/controllers/volumehandler"
	"github.com/backube/volsync/lib/endpoint/route"
	"github.com/backube/volsync/lib/transfer"
	"github.com/backube/volsync/lib/transfer/rsync"
	"github.com/go-logr/logr"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1beta1"
	routev1 "github.com/openshift/api/route/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const cleanupLabelKey = "volsync.backube/cleanup"

// Mover is the reconciliation logic for the RsyncWithStunnel-based data mover.
type Mover struct {
	client      client.Client
	logger      logr.Logger
	ownerMeta   metav1.Object
	ownerType   metav1.TypeMeta
	vh          *volumehandler.VolumeHandler
	isSource    bool
	paused      bool
	mainPVCName *string

	// Source-only fields
	sourceStatus *volsyncv1alpha1.ReplicationSourceRsyncStatus
	sourceSpec   *volsyncv1alpha1.ReplicationSourceRsyncSpec

	// destination-only fields
	destinationStatus *volsyncv1alpha1.ReplicationDestinationRsyncStatus
	transport         string
}

// All object types that are temporary/per-iteration should be listed here. The
// individual objects to be cleaned up must also be marked.
var cleanupTypes = []client.Object{
	&corev1.PersistentVolumeClaim{},
	&corev1.ConfigMap{},
	&snapv1.VolumeSnapshot{},
	&corev1.Secret{},
	&routev1.Route{},
	&corev1.Pod{},
}

var iterativeCleanupTypes = []client.Object{
	&corev1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: corev1.SchemeGroupVersion.Version,
		},
	},
}

func (m *Mover) Name() string { return "rsync-with-stunnel" }

func (m *Mover) Synchronize(ctx context.Context) (mover.Result, error) {
	var err error
	m.logger.Info("running rsync stunnel synchronize", "obj", m.mainPVCName)
	// Allocate temporary data PVC
	var dataPVC *corev1.PersistentVolumeClaim
	if m.isSource {
		dataPVC, err = m.ensureSourcePVC(ctx)
	} else {
		dataPVC, err = m.ensureDestinationPVC(ctx)
	}
	if dataPVC == nil || err != nil {
		return mover.InProgress(), err
	}

	// create route endpoint on the destination
	if !m.isSource {
		dataTransferResult, err := m.reconcileRsyncStunnelDestination(m.client)
		if err != nil {
			m.logger.Error(err, "error reconciling stunnel destination")
			return dataTransferResult, err
		}
		if dataTransferResult.Completed {
			m.logger.Info("rsync transfer complete, saving snapshot")
		} else {
			return mover.InProgress(), nil
		}
		// On the destination, preserve the image and return it
		if !m.isSource {
			image, err := m.vh.EnsureImage(ctx, m.logger, dataPVC)
			if image == nil || err != nil {
				return mover.InProgress(), err
			}
			return mover.CompleteWithImage(image), nil
		}
	} else {
		dataTransferResult, err := m.reconcileRsyncStunnelSource(m.client)
		if err != nil {
			m.logger.Error(err, "error reconciling stunnel source")
		}
		return dataTransferResult, err
	}

	// On the source, just signal completion
	return mover.Complete(), nil
}

func (m *Mover) ensureSourcePVC(ctx context.Context) (*corev1.PersistentVolumeClaim, error) {
	srcPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      *m.mainPVCName,
			Namespace: m.ownerMeta.GetNamespace(),
		},
	}
	if err := m.client.Get(ctx, client.ObjectKeyFromObject(srcPVC), srcPVC); err != nil {
		return nil, err
	}
	dataName := "volsync-" + m.ownerMeta.GetName() + "-src"
	return m.vh.EnsurePVCFromSrc(ctx, m.logger, srcPVC, dataName, true)
}
func (m *Mover) ensureDestinationPVC(ctx context.Context) (*corev1.PersistentVolumeClaim, error) {
	if m.mainPVCName == nil {
		// Need to allocate the incoming data volume
		dataPVCName := "volsync-" + m.ownerMeta.GetName() + "-dest"
		m.mainPVCName = &dataPVCName
		return m.vh.EnsureNewPVC(ctx, m.logger, dataPVCName)
	}

	// use provided PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      *m.mainPVCName,
			Namespace: m.ownerMeta.GetNamespace(),
		},
	}
	err := m.client.Get(ctx, client.ObjectKeyFromObject(pvc), pvc)
	return pvc, err
}

func (m *Mover) Cleanup(ctx context.Context) (mover.Result, error) {
	err := utils.CleanupObjects(ctx, m.client, m.logger, m.ownerMeta, cleanupTypes, iterativeCleanupTypes)
	if err != nil {
		return mover.InProgress(), err
	}
	return mover.Complete(), nil
}

func (m *Mover) serviceSelector() map[string]string {
	dir := "src"
	if !m.isSource {
		dir = "dst"
	}
	return map[string]string{
		"app.kubernetes.io/name":      "volsync-" + dir + "-" + m.ownerMeta.GetName(),
		"app.kubernetes.io/component": "rsync-stunnel-mover",
		"app.kubernetes.io/part-of":   "volsync",
	}
}

func (m *Mover) reconcileRsyncStunnelDestination(c client.Client) (mover.Result, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	p := client.ObjectKey{Namespace: m.ownerMeta.GetNamespace(), Name: *m.mainPVCName}
	ownerObjectKey := client.ObjectKey{Name: m.ownerMeta.GetName(), Namespace: m.ownerMeta.GetNamespace()}
	err := c.Get(context.TODO(), p, pvc)
	if err != nil {
		m.logger.Error(err, "error getting pvc for rsync destination reconciliation", "pvc", p)
		return mover.InProgress(), err
	}

	// TODO: 1. inject proper labels
	// TODO: 2. inject ownerMeta refs
	// TODO: 3. convert the pod into job to get the completion information
	// TODO: 4. convert the New create methods into create or update for proper reconciliations
	// TODO: 5. propose the interface methods
	rsyncTransferOptions, err := m.getRsyncTransferOptions()
	if err != nil {
		m.logger.Error(err, "error getting rsync transfer options", "owner", ownerObjectKey)
		return mover.InProgress(), err
	}

	rsyncTransferOptions = append(rsyncTransferOptions, rsync.DestinationContainerMutation{C: m.getRsyncTransferContainerMutation()}, rsync.DestinationMetaObjectMutation{M: m.metaObject(pvc)})

	// create rsync server and its resources
	var rsyncServer transfer.Server
	switch m.transport {
	case RsyncWithStunnelAnnotation:
		rsyncServer, err = rsync.NewRsyncTransferServerWithStunnel(m.client, transfer.NewSingletonPVC(pvc), rsyncTransferOptions...)
		if err != nil {
			m.logger.Error(err, "error ensuring transfer on destination", "owner", ownerObjectKey)
			return mover.InProgress(), err
		}
	case RsyncWithNullAnnotation:
		rsyncServer, err = rsync.NewRsyncTransferServerWithNull(m.client, transfer.NewSingletonPVC(pvc), rsyncTransferOptions...)
		if err != nil {
			m.logger.Error(err, "error ensuring transfer on destination", "owner", ownerObjectKey)
			return mover.InProgress(), err
		}
	}

	healthy, err := rsyncServer.IsHealthy(m.client)
	status := apierrors.APIStatus(nil)
	// only catch apiserver errors
	if err != nil && errors.As(err, &status) {
		m.logger.Error(err, "error ensuring transfer health on destination", "owner", ownerObjectKey)
		return mover.InProgress(), err
	}
	var completed bool
	if !healthy {
		completed, err = rsyncServer.Completed(m.client)
		if err != nil {
			return mover.InProgress(), err
		}
		if !completed {
			m.logger.Error(nil, "rsync server is not healthy", "owner", ownerObjectKey)
			return mover.InProgress(), fmt.Errorf("rsync server is not healthy")
		}
	}

	hostname := rsyncServer.Endpoint().Hostname()
	port := rsyncServer.Endpoint().IngressPort()
	//sshKeys := rsyncServer.Transport().Credentials().Name

	m.destinationStatus.Address = &hostname
	m.destinationStatus.Port = &port
	//m.destinationStatus.SSHKeys = &sshKeys
	if !completed {
		return mover.InProgress(), nil
	}
	err = rsyncServer.MarkForCleanup(m.client, cleanupLabelKey, string(m.ownerMeta.GetUID()))
	if err != nil {
		return mover.InProgress(), nil
	}

	return mover.Complete(), nil
}

func (m *Mover) reconcileRsyncStunnelSource(c client.Client) (mover.Result, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	p := client.ObjectKey{Namespace: m.ownerMeta.GetNamespace(), Name: *m.mainPVCName}
	err := c.Get(context.TODO(), p, pvc)
	if err != nil {
		m.logger.Error(err, "error getting pvc for rsync destination reconciliation", "pvc", p)
		return mover.InProgress(), err
	}

	containerMutations := m.getRsyncTransferContainerMutation()

	rsyncOptions, err := m.getRsyncTransferOptions()
	if err != nil {
		return mover.InProgress(), err
	}

	rsyncOptions = append(rsyncOptions, rsync.SourceContainerMutation{C: containerMutations}, rsync.SourceMetaObjectMutation{M: m.metaObject(pvc)})

	var rsyncClient transfer.Client
	switch {
	case m.transport == RsyncWithStunnelAnnotation:
		rsyncClient, err = rsync.NewRsyncTransferClientWithStunnel(m.client, *m.sourceSpec.Address, route.IngressPort, transfer.NewSingletonPVC(pvc), rsyncOptions...)
		if err != nil {
			return mover.InProgress(), err
		}
	case m.transport == RsyncWithNullAnnotation:
		rsyncClient, err = rsync.NewRsyncClientWithNullTransport(m.client, *m.sourceSpec.Address, *m.sourceSpec.Port, transfer.NewSingletonPVC(pvc), rsyncOptions...)
		if err != nil {
			return mover.InProgress(), err
		}
	default:
		return mover.Complete(), fmt.Errorf("invalid transport annotation found")
	}

	complete, err := rsyncClient.IsCompleted(m.client)
	if err != nil {
		return mover.InProgress(), err
	}

	if !complete {
		return mover.InProgress(), nil
	}

	err = rsyncClient.MarkForCleanup(c, cleanupLabelKey, string(m.ownerMeta.GetUID()))
	if err != nil {
		return mover.InProgress(), err
	}

	return mover.Complete(), nil
}

// getRsyncTransferContainerMutation returns container mutation to be applied on Rsync tranfer pods
func (m *Mover) getRsyncTransferContainerMutation() *corev1.Container {
	isPrivileged := false
	runAsUser := int64(0)
	trueBool := bool(true)
	customSecurityContext := &corev1.SecurityContext{
		Privileged:             &isPrivileged,
		RunAsUser:              &runAsUser,
		ReadOnlyRootFilesystem: &trueBool,
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"MKNOD", "SETPCAP"},
		},
	}
	return &corev1.Container{
		SecurityContext: customSecurityContext,
	}
}

func (m *Mover) metaObject(pvc *corev1.PersistentVolumeClaim) *metav1.ObjectMeta {
	return &metav1.ObjectMeta{
		Name:      pvc.Name,
		Namespace: pvc.Namespace,
		Labels:    m.serviceSelector(),
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: m.ownerType.APIVersion,
			Kind:       m.ownerType.Kind,
			Name:       m.ownerMeta.GetName(),
			UID:        m.ownerMeta.GetUID(),
			// TODO: alpatel: probably need to revisit the following
			//BlockOwnerDeletion: true,
		}},
	}
}

func (m *Mover) getRsyncTransferOptions() ([]rsync.TransferOption, error) {
	// prepare rsync command options
	rsyncPassword, err := m.getRsyncPassword()
	if err != nil {
		return nil, err
	}
	transferOptions := []rsync.TransferOption{
		rsync.StandardProgress(true),
		rsync.ArchiveFiles(true),
		rsync.DeleteDestination(true),
		rsync.HardLinks(true),
		rsync.Partial(true),
		rsync.Username("root"),
		rsync.Password(rsyncPassword),
	}
	// TODO: alpatel: figure out a way to add bandwidth limit to rsync configuration
	//if o.BwLimit > 0 {
	//	transferOptions = append(transferOptions,
	//		RsyncBwLimit(o.BwLimit))
	//}
	return transferOptions, nil
}

func (m *Mover) getRsyncPassword() (string, error) {
	return "developmentPassword", nil
}
