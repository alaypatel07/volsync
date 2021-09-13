package rsync

import (
	"fmt"

	"github.com/backube/volsync/lib/meta"
	"github.com/backube/volsync/lib/transfer"
	v1 "k8s.io/api/core/v1"
)

const (
	RsyncContainer = "rsync"
)

const (
	rsyncUser         = "crane2"
	rsyncImage        = "quay.io/konveyor/rsync-transfer:latest"
	rsyncPort         = int32(1873)
	rsyncConfig       = "crane2-rsync-config"
	rsyncSecretPrefix = "crane2-rsync"
)

// getMountPathForPVC given a PVC, returns a path where PVC can be mounted within a transfer Pod
func getMountPathForPVC(p transfer.PVC) string {
	return fmt.Sprintf("/mnt/%s/%s", p.Claim().Namespace, p.LabelSafeName())
}

// applyPodMutations given a pod spec and a list of podSpecMutation, applies
// each mutation to the given podSpec, only merge type mutations are allowed here
// Following fields will be mutated:
// - spec.NodeSelector
// - spec.SecurityContext
// - spec.NodeName
// - spec.Containers[i].SecurityContext
func applyPodMutations(podSpec *v1.PodSpec, ms []meta.PodSpecMutation) {
	for _, m := range ms {
		switch m.Type() {
		case meta.MutationTypeReplace:
			podSpec.NodeSelector = m.NodeSelector()
			if m.PodSecurityContext() != nil {
				podSpec.SecurityContext = m.PodSecurityContext()
			}
			if m.NodeName() != nil {
				podSpec.NodeName = *m.NodeName()
			}
		}
	}
}

func applyContainerMutations(container *v1.Container, ms []meta.ContainerMutation) {
	for _, m := range ms {
		switch m.Type() {
		case meta.MutationTypeReplace:
			if m.SecurityContext() != nil {
				container.SecurityContext = m.SecurityContext()
			}
			if m.Resources() != nil {
				container.Resources = *m.Resources()
			}
		}
	}
}
