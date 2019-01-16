package apiserver

import (
	"encoding/json"

	"github.com/juju/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/moiot/gravity-operator/pkg/apis/pipeline/v1alpha1"
	"github.com/moiot/gravity/pkg/app"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
)

type ApiPipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ApiPipelineSpec    `json:"spec"`
	Status api.PipelineStatus `json:"status"`
}

type ApiPipelineSpec struct {
	api.PipelineSpec
	Config *json.RawMessage `json:"config,omitempty"`
}

func (apiPipeline *ApiPipeline) toK8() *api.Pipeline {
	apiPipeline.Spec.LastUpdate = metav1.Now()
	return &api.Pipeline{
		TypeMeta:   apiPipeline.TypeMeta,
		ObjectMeta: apiPipeline.ObjectMeta,
		Spec:       apiPipeline.Spec.PipelineSpec,
		Status:     apiPipeline.Status,
	}
}

func (apiPipeline *ApiPipeline) fromK8(pipeline *api.Pipeline) {
	apiPipeline.TypeMeta = pipeline.TypeMeta
	apiPipeline.ObjectMeta = pipeline.ObjectMeta
	apiPipeline.Spec = ApiPipelineSpec{PipelineSpec: pipeline.Spec}
	apiPipeline.Status = pipeline.Status
}

func (apiPipeline *ApiPipeline) newConfigMap(pipeline *api.Pipeline) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      apiPipeline.Name,
			Namespace: pipeline.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":     "gravity",
				"app.kubernetes.io/instance": apiPipeline.Name,
			},
			Annotations: map[string]string{
				api.GroupName + "/hash": apiPipeline.Spec.ConfigHash,
			},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(pipeline, api.SchemeGroupVersion.WithKind(api.PipelineResourceKind))},
		},
		Data: map[string]string{
			api.ConfigFileKey: string(*apiPipeline.Spec.Config),
		},
	}
}

func (apiPipeline *ApiPipeline) validate() error {
	if err := apiPipeline.Spec.Validate(); err != nil {
		return errors.Annotatef(err, "invalid spec: %s", err.Error())
	}

	cfgV3 := &config.PipelineConfigV3{}
	err := json.Unmarshal(*apiPipeline.Spec.Config, cfgV3)
	if err != nil {
		return errors.Annotatef(err, "error unmarshal gravity config v3: %s", string(*apiPipeline.Spec.Config))
	}

	if cfgV3.Version != config.PipelineConfigV3Version {
		return errors.Errorf("ONLY support 1.0 configuration version")
	}

	cfgV3.PipelineName = apiPipeline.Name
	_, err = app.Parse(*cfgV3)
	if err != nil {
		return errors.Annotatef(err, "error parse gravity cfg: %s. %#v.", err, cfgV3)
	}
	updated, err := json.Marshal(cfgV3)
	if err != nil {
		return errors.Annotatef(err, "error marshal cfg: %#v. err: %s", cfgV3, err)
	}
	updatedRaw := json.RawMessage(updated)
	apiPipeline.Spec.Config = &updatedRaw

	apiPipeline.Spec.ConfigHash = core.HashConfig(string(*apiPipeline.Spec.Config))
	return nil
}
