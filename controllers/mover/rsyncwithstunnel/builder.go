/*
Copyright 2021 The VolSync authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package rsyncwithstunnel

import (
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/controllers/mover"
	"github.com/backube/volsync/controllers/volumehandler"
)

// defaultResticContainerImage is the default container image for the restic
// data mover
//const defaultResticContainerImage = "quay.io/backube/volsync-mover-restic:latest"

// resticContainerImage is the container image name of the restic data mover
//var resticContainerImage string

const (
	RsyncWithStunnelAnnotation = "scribe.backube.dev/mover-rsync-with-stunnel"
	RsyncWithNullAnnotation    = "scribe.backube.dev/mover-rsync-with-null"
)

type Builder struct{}

var _ mover.Builder = &Builder{}

func Register() {
	mover.Register(&Builder{})
}

func (rb *Builder) FromSource(client client.Client, logger logr.Logger,
	source *volsyncv1alpha1.ReplicationSource) (mover.Mover, error) {
	// Only build if the CR belongs to us
	if source.Spec.Rsync == nil {
		return nil, nil
	}

	var transport = ""
	if _, ok := source.Annotations[RsyncWithStunnelAnnotation]; ok {
		transport = RsyncWithStunnelAnnotation
	} else if _, ok := source.Annotations[RsyncWithNullAnnotation]; ok {
		transport = RsyncWithNullAnnotation
	} else {
		return nil, nil
	}
	// Create ReplicationSourceRsyncStatus to write rsync status
	if source.Status.Rsync == nil {
		source.Status.Rsync = &volsyncv1alpha1.ReplicationSourceRsyncStatus{}
	}

	vh, err := volumehandler.NewVolumeHandler(
		volumehandler.WithClient(client),
		volumehandler.WithOwner(source),
		volumehandler.FromSource(&source.Spec.Rsync.ReplicationSourceVolumeOptions),
	)
	if err != nil {
		return nil, err
	}

	return &Mover{
		client:       client,
		logger:       logger.WithValues("method", "RsyncWithStunnel"),
		ownerMeta:    source,
		ownerType:    source.TypeMeta,
		vh:           vh,
		isSource:     true,
		paused:       source.Spec.Paused,
		mainPVCName:  &source.Spec.SourcePVC,
		sourceStatus: source.Status.Rsync,
		sourceSpec:   source.Spec.Rsync,
		transport:    transport,
	}, nil
}

func (rb *Builder) FromDestination(client client.Client, logger logr.Logger,
	destination *volsyncv1alpha1.ReplicationDestination) (mover.Mover, error) {
	// Only build if the CR belongs to us
	if destination.Spec.Rsync == nil {
		return nil, nil
	}

	var transport = ""
	if _, ok := destination.Annotations[RsyncWithStunnelAnnotation]; ok {
		transport = RsyncWithStunnelAnnotation
	} else if _, ok := destination.Annotations[RsyncWithNullAnnotation]; ok {
		transport = RsyncWithNullAnnotation
	} else {
		return nil, nil
	}

	// Create ReplicationSourceRsyncStatus to write rsync status
	if destination.Status.Rsync == nil {
		destination.Status.Rsync = &volsyncv1alpha1.ReplicationDestinationRsyncStatus{}
	}

	logger.Info("rsync stunnel mover selected, returning the mover")

	vh, err := volumehandler.NewVolumeHandler(
		volumehandler.WithClient(client),
		volumehandler.WithOwner(destination),
		volumehandler.FromDestination(&destination.Spec.Rsync.ReplicationDestinationVolumeOptions),
	)
	if err != nil {
		return nil, err
	}

	return &Mover{
		client:      client,
		logger:      logger.WithValues("method", "RsyncWithStunnel"),
		ownerMeta:   destination,
		ownerType:   destination.TypeMeta,
		vh:          vh,
		isSource:    false,
		paused:      destination.Spec.Paused,
		mainPVCName: destination.Spec.Rsync.DestinationPVC,

		destinationStatus: destination.Status.Rsync,
		transport:         transport,
	}, nil
}
