package transfer

import (
	corev1 "k8s.io/api/core/v1"
)

// PVC knows how to return v1.PersistentVolumeClaim and an additional validated
// name which can be used by different transfers as per their own requirements
type PVC interface {
	// Claim returns the v1.PersistentVolumeClaim reference this PVC is associated with
	Claim() *corev1.PersistentVolumeClaim
	// LabelSafeName returns a name for the PVC that can be used as a label value
	// it may be validated differently by different transfers
	LabelSafeName() string
}

type PVCList interface {
	GetNamespaces() []string
	InNamespace(ns string) PVCList
	PVCs() []PVC
}
