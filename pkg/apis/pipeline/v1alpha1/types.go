package v1alpha1

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/robfig/cron"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

const (
	ConfigFileKey = "config.json"
)

// Standard parser without descriptors
var cronParser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.DowOptional | cron.Descriptor)


// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Pipeline is a specification for a Pipeline resource
type Pipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PipelineSpec   `json:"spec"`
	Status PipelineStatus `json:"status"`
}

type ScheduledPausePeriod struct {
	// StartTime, EndTime conforms to the format at here:
	// https://godoc.org/github.com/robfig/cron
	StartTime string `json:"start-time"`
	EndTime string `json:"end-time"`
	Location string `json:"location"`
}

func (period ScheduledPausePeriod) Validate() error {
	if period.Location == "" {
		return errors.Errorf("empty location in schedule pause period")
	}

	if _, err := time.LoadLocation(period.Location); err != nil {
		return err
	}

	if _, err := cronParser.Parse(period.StartTime); err != nil {
		return err
	}

	if _, err := cronParser.Parse(period.EndTime); err != nil {
		return err
	}
	return nil
}

// PipelineSpec is the spec for a Pipeline resource
type PipelineSpec struct {
	Task                  `json:",inline"`
	Paused                bool                   `json:"paused"`
	ScheduledPausePeriods []ScheduledPausePeriod `json:"scheduled-pause-periods"`
	LastUpdate            metav1.Time            `json:"lastUpdate"`
}


func (spec PipelineSpec) Validate() error {
	for _, period := range spec.ScheduledPausePeriods {
		if err := period.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func StartOfTheDay(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location())
}

func IsInScheduledPausePeriod(period ScheduledPausePeriod, currentTime time.Time) bool {
	if period.Location == "" {
		panic("invalid location")
	}

	location, err := time.LoadLocation(period.Location)
	if err != nil {
		panic("invalid location in ScheduledPausePeriod")
	}
	currentTimeWithLocation := currentTime.In(location)

	startSchedule, err := cronParser.Parse(period.StartTime)
	if err != nil {
		panic(fmt.Sprintf("scheduledPausePeriod start time not valid: startTime: %s, err: %v", period.StartTime, err))
	}

	endSchedule, err := cronParser.Parse(period.EndTime)
	if err != nil {
		panic("scheduledPausePeriod end time not valid")
	}

	t := StartOfTheDay(currentTimeWithLocation)

	minTime := startSchedule.Next(t)
	maxTime := endSchedule.Next(t)

	return minTime.Before(currentTimeWithLocation) && maxTime.After(currentTimeWithLocation)
}

func (spec PipelineSpec) IsInScheduledPausePeriods() bool {
	currentTime := time.Now()
	for _, period := range spec.ScheduledPausePeriods {
		if IsInScheduledPausePeriod(period, currentTime) {
			return true
		}
	}
	return false
}


// PipelineStatus is the status for a Pipeline resource
type PipelineStatus struct {
	ObservedGeneration int64 `json:"observedGeneration"`
	Task               `json:",inline"`
	Position           string              `json:"position"`
	PodName            string              `json:"podName"`
	Conditions         []PipelineCondition `json:"conditions,omitempty"`
}

func (t PipelineStatus) Available() bool {
	c := t.Condition(PipelineConditionRunning)
	return c != nil && c.Status == corev1.ConditionTrue
}

func (t PipelineStatus) Condition(condType PipelineConditionType) *PipelineCondition {
	for _, cond := range t.Conditions {
		if cond.Type == condType {
			return &cond
		}
	}

	return nil
}

type Task struct {
	ConfigHash string   `json:"configHash"`
	Image      string   `json:"image"`
	Command    []string `json:"command"`
}

type PipelineConditionType string

const (
	PipelineConditionRunning     PipelineConditionType = "Running"
	PipelineConditionIncremental PipelineConditionType = "Incremental"
)

type PipelineCondition struct {
	// Type of cluster condition.
	Type PipelineConditionType `json:"type"`
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
type PipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Pipeline `json:"items"`
}
