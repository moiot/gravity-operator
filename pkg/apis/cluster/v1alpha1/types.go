package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/moiot/gravity-operator/pkg/utils"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Cluster is a specification for a Cluster resource
type Cluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterSpec   `json:"spec"`
	Status ClusterStatus `json:"status"`
}

// ClusterSpec is the spec for a cluster resource
type ClusterSpec struct {
	// The maximum number of pipelines that can be upgraded.
	// Value can be an absolute number (ex: 5) or a percentage of pipelines (ex: 10%).
	// Absolute number is calculated from percentage by rounding up.
	// Defaults to 25%.
	Rolling *intstr.IntOrString `json:"rolling"`

	DeploymentRules []DeploymentRule `json:"deploymentRules"`
}

type DeploymentRule struct {
	Group     string   `json:"group" yaml:"group"`
	Pipelines []string `json:"pipelines" yaml:"pipelines"`
	Image     string   `json:"image" yaml:"image"`
	Command   []string `json:"command" yaml:"command"`
}

func (dc *Cluster) FindDeploymentRule(pipelineName string) *DeploymentRule {
	for _, rule := range dc.Spec.DeploymentRules {
		for _, pipelineNameExpr := range rule.Pipelines {
			if utils.Glob(pipelineNameExpr, pipelineName) {
				return &rule
			}
		}
	}
	return nil
}

// ClusterStatus is the status for a cluster resource
type ClusterStatus struct {
	ObservedGeneration int64 `json:"observedGeneration"`

	// Total number of pipelines.
	// +optional
	Pipelines int32 `json:"pipelines"`

	// Total number of pipelines which version match current deployment rule.
	// +optional
	UpdatedPipelines int32 `json:"updatedPipelines"`

	// Total number of available pipelines (ready for at least minReadySeconds).
	// +optional
	AvailablePipelines int32 `json:"availablePipelines"`

	// Total number of unavailable pipelines. They may
	// either be pipelines that are running but not yet available or pipelines that still have not been created.
	// +optional
	UnavailablePipelines int32 `json:"unavailablePipelines"`

	Conditions []ClusterCondition `json:"conditions,omitempty"`
}

func (t ClusterStatus) Condition(condType ClusterConditionType) *ClusterCondition {
	for _, cond := range t.Conditions {
		if cond.Type == condType {
			return &cond
		}
	}

	return nil
}

type ClusterConditionType string

const (
	// Available means the deployment is available, ie. at least the minimum available
	// replicas required are up and running for at least minReadySeconds.
	ClusterAvailable ClusterConditionType = "Available"

	// UpToDate means every pipeline in cluster has been updated.
	ClusterUpToDate ClusterConditionType = "UpToDate"
)

type ClusterCondition struct {
	// Type of Cluster condition.
	Type ClusterConditionType `json:"type"`
	// Status of the condition, one of True, False, Unknown.
	Status corev1.ConditionStatus `json:"status"`
	// LastUpdateTime is the last time this condition was updated.
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
	// LastTransitionTime is the last time the condition transitioned from one status to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
	// Reason for the condition's last transition.
	Reason string `json:"reason,omitempty"`
	// Message which is human readable indicating details about the transition.
	Message string `json:"message,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PipelineList is a list of Pipeline resources
type ClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Cluster `json:"items"`
}
