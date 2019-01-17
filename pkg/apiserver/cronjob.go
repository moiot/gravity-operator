package apiserver

import (
	"fmt"

	"github.com/juju/errors"
	pipeapi "github.com/moiot/gravity-operator/pkg/apis/pipeline/v1alpha1"
	"k8s.io/api/batch/v1"
	"k8s.io/api/batch/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ActionPause  = "pausePipe"
	ActionResume = "resumePipe"
)

type ApiCronJob struct {
	PipelineName string `json:"pipeline"`
	CronJobName  string `json:"name"`

	// Schedule takes a Cron format
	// https://kubernetes.io/docs/tasks/job/automated-tasks-with-cron-jobs/#schedule
	// THE TIMEZONE used by Schedule are based the timezone of the kubernetes master
	Schedule string `json:"schedule"`

	// Action is used to Pause and Resume a pipeline
	Action string `json:action`
}

func (apiCronJob *ApiCronJob) Validate() error {
	if err := ValidateOperatorEnv(); err != nil {
		return err
	}

	if apiCronJob.PipelineName == "" {
		return errors.Errorf("pipeline name is empty")
	}

	if apiCronJob.CronJobName == "" {
		return errors.Errorf("cronjob name is empty")
	}

	if apiCronJob.Schedule == "" {
		return errors.Errorf("schedule config is empty")
	}

	if apiCronJob.Action != ActionPause && apiCronJob.Action != ActionResume {
		return errors.Errorf("action config is not valid")
	}

	return nil
}

func pauseCommand(apiUrl string, pipelineName string) []string {
	return []string{"curl", "-X", "POST", fmt.Sprintf("%s/pipeline/%s/pause", apiUrl, pipelineName)}
}

func resumeCommand(apiUrl string, pipelineName string) []string {
	return []string{"curl", "-X", "POST", fmt.Sprintf("%s/pipeline/%s/resume", apiUrl, pipelineName)}
}

func containerCommand(action, apiUrl, pipelineName string) []string {
	if action == ActionPause {
		return pauseCommand(apiUrl, pipelineName)
	} else {
		return resumeCommand(apiUrl, pipelineName)
	}
}

func (apiCronJob *ApiCronJob) tok8(apiUrl string, pipeline *pipeapi.Pipeline, action string) *v1beta1.CronJob {
	command := containerCommand(action, apiUrl, pipeline.Name)

	jobMeta := metav1.ObjectMeta{
		Name:      apiCronJob.CronJobName,
		Namespace: pipeline.Namespace,
		Labels: map[string]string{
			"app.kubernetes.io/name": "gravity-cronjob",
			"pipeline":               pipeline.Name,
			"action":                 action,
		},
		Annotations: map[string]string{
			"action":   apiCronJob.Action,
			"schedule": apiCronJob.Schedule,
		},
		OwnerReferences: []metav1.OwnerReference{
			{
				APIVersion: pipeline.APIVersion,
				Kind:       pipeline.Kind,
				Name:       pipeline.Name,
				UID:        pipeline.UID,
			},
		},
	}

	return &v1beta1.CronJob{
		ObjectMeta: jobMeta,
		Spec: v1beta1.CronJobSpec{
			Schedule: apiCronJob.Schedule,
			JobTemplate: v1beta1.JobTemplateSpec{
				Spec: v1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:    action,
									Command: command,
									Image:   "byrnedo/alpine-curl",
								},
							},
							RestartPolicy: corev1.RestartPolicyOnFailure,
						},
					},
				},
			},
		},
	}
}
