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

package utils

import (
	"context"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const cleanupLabelKey = "volsync.backube/cleanup"

// MarkForCleanup marks the provided "obj" to be deleted at the end of the
// synchronization iteration.
func MarkForCleanup(owner metav1.Object, obj metav1.Object) {
	uid := owner.GetUID()
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[cleanupLabelKey] = string(uid)
	obj.SetLabels(labels)
}

// CleanupObjects deletes all objects that have been marked. The objects to be
// cleaned up must have been previously marked via MarkForCleanup() and
// associated with "owner". The "types" array should contain one object of each
// type to clean up.
func CleanupObjects(ctx context.Context, c client.Client,
	logger logr.Logger, owner metav1.Object, types,
	iterativeTypes []client.Object) error {
	uid := owner.GetUID()
	l := logger.WithValues("owned-by", uid)
	deleteAllOfOptions := []client.DeleteAllOfOption{
		client.MatchingLabels{cleanupLabelKey: string(uid)},
		client.InNamespace(owner.GetNamespace()),
		client.PropagationPolicy(metav1.DeletePropagationBackground),
	}
	l.Info("deleting temporary objects")
	for _, obj := range types {
		err := c.DeleteAllOf(ctx, obj, deleteAllOfOptions...)
		if client.IgnoreNotFound(err) != nil {
			l.Error(err, "unable to delete object(s)")
			return err
		}
	}

	listOptions := []client.ListOption{
		client.MatchingLabels{cleanupLabelKey: string(uid)},
		client.InNamespace(owner.GetNamespace()),
	}
	errs := []error{}
	for _, objList := range iterativeTypes {
		ulist := &unstructured.UnstructuredList{}
		ulist.SetGroupVersionKind(objList.GetObjectKind().GroupVersionKind())
		err := c.List(ctx, ulist, listOptions...)
		if err != nil {
			// if we hit error with one api still try all others
			errs = append(errs, err)
			continue
		}
		for _, item := range ulist.Items {
			err = c.Delete(ctx, &item, client.PropagationPolicy(metav1.DeletePropagationBackground))
			if err != nil {
				// if we hit error deleting on continue delete others
				errs = append(errs, err)
			}
		}

	}
	return errors.NewAggregate(errs)
}
