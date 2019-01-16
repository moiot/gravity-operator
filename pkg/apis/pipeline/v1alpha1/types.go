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
	// StartClock, EndClock is defined as the hour, minutes, seconds within 24 hours, for example:
	//
	// 02:00:00+08:00
	// 20:00:00+08:00
	//
	//
	// If EndClock is "less than" the StartClock, then StartClock should be
	// the day before the EndClock
	//
	// StartClock: 20:00:00+08:00
	// EndClock:   01:00:00+08:00
	//
	StartClock string `json:"start-clock"`
	EndClock   string `json:"end-clock"`
}

func addDateToClock(s string, year, month, date int) string {
	return fmt.Sprintf("%d-%02d-%02dT%s", year, month, date, s)
}

func validateClock(s string) (*time.Location, error) {
	if t, err := time.Parse(time.RFC3339, addDateToClock(s, 2019,1, 1)); err != nil {
		return nil, errors.Errorf("time should be in the format like '00:00:20+08:00'")
	} else {
		return t.Location(), nil
	}
}

func (period ScheduledPausePeriod) Validate() error {
	startLoc, err := validateClock(period.StartClock)
	if err != nil {
		return err
	}

	endLoc, err := validateClock(period.EndClock)
	if err != nil {
		return err
	}

	if startLoc.String() != endLoc.String() {
		return errors.Errorf("StartClock should have the same Timezone with EndClock")
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


func IsInScheduledPausePeriod(period ScheduledPausePeriod, currentTime time.Time) bool {
	// Use a random year, month, date to get the location in Spec
	startTS := addDateToClock(period.StartClock, 2019, 1, 1)
	startTime, err := time.Parse(time.RFC3339, startTS)
	if err != nil {
		panic(fmt.Sprintf("invalid StartClock: %v", err))
	}
	loc := startTime.Location()


	// calculate the current range
	currentTimeInSpecLocation := currentTime.In(loc)
	year, month, date := currentTimeInSpecLocation.Date()
	realStartTS := addDateToClock(period.StartClock, year, int(month), date)
	startTime, err = time.Parse(time.RFC3339, realStartTS)
	if err != nil {
		panic(fmt.Sprintf("invalid StartClock"))
	}

	realEndTS := addDateToClock(period.EndClock, year, int(month), date)
	endTime, err := time.Parse(time.RFC3339, realEndTS)
	if err != nil {
		panic(fmt.Sprintf("invalid EncClock"))
	}

	if endTime.Before(startTime) {
		startTime = startTime.AddDate(0, 0, -1)
	}

	return startTime.Before(currentTime) && endTime.After(currentTime)
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
