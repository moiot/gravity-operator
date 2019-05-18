package controller

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	batchinformers "k8s.io/client-go/informers/batch/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	batch "k8s.io/client-go/listers/batch/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	api "github.com/moiot/gravity-operator/pkg/apis/pipeline/v1alpha1"
	client "github.com/moiot/gravity-operator/pkg/client/pipeline/clientset/versioned"
	"github.com/moiot/gravity-operator/pkg/client/pipeline/clientset/versioned/scheme"
	listers "github.com/moiot/gravity-operator/pkg/client/pipeline/listers/pipeline/v1alpha1"
	"github.com/moiot/gravity-operator/pkg/utils"
	"github.com/moiot/gravity/pkg/app"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
)

const (
	// ErrResourceExists is used as part of the Event 'reason' when a Pipeline fails
	// to sync due to a ConfigMap of the same name already existing.
	ErrResourceExists = "ErrResourceExists"

	// ErrResourceExists is used as part of the Event 'reason' when a Pipeline fails
	// to sync due to a ConfigMap of the same name already existing.
	ErrSyncFailed = "ErrSyncFailed"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a statefulSet already existing
	MessageResourceExists = "ConfigMap %s already exists and is not managed by Pipeline"

	// MessageResourceSynced is the message used for an Event fired when a Pipeline
	// is synced successfully
	MessageSyncFailed = "Pipeline sync failed, err: %s"
)

type PipelineManager struct {
	namespace string

	kubeclientset kubernetes.Interface
	pipeclientset client.Interface

	statefulSetLister appslisters.StatefulSetLister
	statefulSetSynced cache.InformerSynced

	jobLister batch.JobLister
	jobSynced cache.InformerSynced

	podLister corelisters.PodLister
	podSynced cache.InformerSynced

	pipelinesLister listers.PipelineLister
	configMapLister corelisters.ConfigMapLister

	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder
}

func newPipelineController(
	namespace string,
	kubeclientset kubernetes.Interface,
	pipeclientset client.Interface,
	statefulSetInformer appsinformers.StatefulSetInformer,
	jobInformer batchinformers.JobInformer,
	podInformer coreinformers.PodInformer,
	pipeLister listers.PipelineLister,
	configMapLister corelisters.ConfigMapLister) *PipelineManager {

	log.Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(log.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events(namespace)})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	controller := &PipelineManager{
		kubeclientset: kubeclientset,
		pipeclientset: pipeclientset,

		statefulSetLister: statefulSetInformer.Lister(),
		statefulSetSynced: statefulSetInformer.Informer().HasSynced,

		jobLister: jobInformer.Lister(),
		jobSynced: jobInformer.Informer().HasSynced,

		podLister: podInformer.Lister(),
		podSynced: podInformer.Informer().HasSynced,

		pipelinesLister: pipeLister,

		workqueue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Pipeline"),

		configMapLister: configMapLister,
		recorder:        recorder,
	}

	// Set up an event handler for when statefulSet resources change. This
	// handler will lookup the owner of the given statefulSet, and if it is
	// owned by a Pipeline resource will enqueue that Pipeline resource for
	// processing. This way, we don't need to implement custom logic for
	// handling statefulSet resources. More info on this pattern:
	// https://github.com/kubernetes/community/blob/8cafef897a22026d42f5e5bb3f104febe7e29830/contributors/devel/controllers.md
	statefulSetInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.handleObject,
		UpdateFunc: func(old, new interface{}) {
			// update position when re-sync
			controller.handleObject(new)
		},
		DeleteFunc: controller.handleObject,
	})

	jobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.handleObject,
		UpdateFunc: func(old, new interface{}) {
			// update position when re-sync
			controller.handleObject(new)
		},
		DeleteFunc: controller.handleObject,
	})

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (pm *PipelineManager) Run(threadiness int, stopCh <-chan struct{}) error {
	// Start the informer factories to begin populating the informer caches
	log.Info("[PipelineManager.Run] Starting...")

	// Wait for the caches to be synced before starting workers
	log.Info("[PipelineManager.Run] Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(stopCh, pm.statefulSetSynced, pm.jobSynced, pm.podSynced); !ok {
		return fmt.Errorf("[PipelineManager.Run] failed to wait for caches to sync")
	}

	log.Info("[PipelineManager.Run] Starting workers")
	go func() {
		<-stopCh
		log.Info("[PipelineManager] shutdown work queue")
		pm.workqueue.ShutDown()
	}()

	// Launch two workers to process Pipeline resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(pm.runWorker, time.Second, stopCh)
	}

	log.Info("[PipelineManager.Run] Started workers")
	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (pm *PipelineManager) runWorker() {
	for pm.processNextWorkItem() {
	}
}

func (pm *PipelineManager) processNextWorkItem() bool {
	obj, shutdown := pm.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer pm.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			pm.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Pipeline resource to be synced.
		if err := pm.syncHandler(key); err != nil {
			return errors.Annotatef(err, "error syncing '%s': %s", key, errors.ErrorStack(err))
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		pm.workqueue.Forget(obj)
		log.Infof("[PipelineManager.processNextWorkItem] Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

func syncMetrics(name string, start time.Time, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}
	syncCount.WithLabelValues(name, "pipeline", result).Add(1)
	scheduleHistogram.WithLabelValues(name, "pipeline").Observe(time.Since(start).Seconds())
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Pipeline resource
// with the current status of the resource.
func (pm *PipelineManager) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	start := time.Now()
	defer syncMetrics(name, start, err)

	// Get the pipeline resource with this namespace/name
	pipeline, err := pm.pipelinesLister.Pipelines(namespace).Get(name)
	if err != nil {
		// The pipeline resource may no longer exist, in which case we stop
		// processing.
		if apierrors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("pipeline '%s' in work queue no longer exists", key))
			pipelineUnavailable.WithLabelValues(name).Set(0)
			return nil
		}
		return errors.Trace(err)
	}
	pipeline = pipeline.DeepCopy()

	configMap, err := pm.configMapLister.ConfigMaps(pipeline.Namespace).Get(pipeline.Name)
	if err != nil {
		return errors.Trace(err)
	}
	rawConfig := configMap.Data[api.ConfigFileKey]
	cfg := &config.PipelineConfigV3{}
	err = json.Unmarshal([]byte(rawConfig), cfg)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.InputPlugin.Mode != config.Batch {
		return pm.syncNoneBatch(pipeline)
	} else {
		return pm.syncBatch(pipeline)
	}
}

func (pm *PipelineManager) syncNoneBatch(pipeline *api.Pipeline) error {
	err := pm.kubeclientset.BatchV1().Jobs(pipeline.Namespace).Delete(pipeline.Name, utils.ForegroundDeleteOptions)
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Trace(err)
	}

	statefulSet, err := pm.statefulSetLister.StatefulSets(pipeline.Namespace).Get(pipeline.Name)
	// If the resource doesn't exist, we'll create it
	if apierrors.IsNotFound(err) {
		_, err = pm.kubeclientset.CoreV1().Services(pipeline.Namespace).Create(pm.newHeadlessService(pipeline))
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return errors.Annotatef(err, "error create headless service for %s", pipeline.Name)
		}
		statefulSet, err = pm.kubeclientset.AppsV1().StatefulSets(pipeline.Namespace).Create(pm.newStatefulSet(pipeline))
	}

	// If an error occurs during Get/Create, we'll requeue the item so we can
	// attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		pipelineUnavailable.WithLabelValues(pipeline.Name).Set(1)
		pm.recorder.Eventf(pipeline, corev1.EventTypeWarning, ErrSyncFailed, MessageSyncFailed, err.Error())
		return errors.Trace(err)
	}

	originSpec := statefulSet.Spec
	statefulSet = statefulSet.DeepCopy()
	// If the statefulSet is not controlled by this Pipeline resource, we should log
	// a warning to the event recorder. No need to retry until pipeline updated
	if !metav1.IsControlledBy(statefulSet, pipeline) {
		msg := fmt.Sprintf(MessageResourceExists, statefulSet.Name)
		pm.recorder.Event(pipeline, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil
	}
	pipelineUnavailable.WithLabelValues(pipeline.Name).Set(float64(statefulSet.Status.Replicas - statefulSet.Status.ReadyReplicas))
	if pipeline.Spec.Paused {
		statefulSet.Spec.Replicas = int32Ptr(0)
	} else {
		statefulSet.Spec.Replicas = int32Ptr(1)
	}
	container := statefulSet.Spec.Template.Spec.Containers[0]
	if container.Image != pipeline.Spec.Image || !reflect.DeepEqual(container.Command, pipeline.Spec.Command) {
		statefulSet.Spec.Template.Spec.Containers[0].Image = pipeline.Spec.Image
		statefulSet.Spec.Template.Spec.Containers[0].Command = pipeline.Spec.Command
	}
	if !reflect.DeepEqual(originSpec, statefulSet.Spec) {
		statefulSet, err = pm.kubeclientset.AppsV1().StatefulSets(pipeline.Namespace).Update(statefulSet)
		if err != nil {
			pm.recorder.Eventf(pipeline, corev1.EventTypeWarning, ErrSyncFailed, MessageSyncFailed, err.Error())
			return errors.Trace(err)
		} else {
			pm.recorder.Eventf(pipeline, corev1.EventTypeNormal, "Upgraded", "Upgraded statefulSet %s to %s:%s", statefulSet.Name,
				pipeline.Spec.Image, pipeline.Spec.Command)
		}
	}

	status := &api.PipelineStatus{
		ObservedGeneration: pipeline.Generation,
		Task:               pipeline.Spec.Task,
		Position:           pipeline.Status.Position,
	}

	if statefulSet.Status.ReadyReplicas > 0 {
		setPipelineCondition(status, api.PipelineCondition{
			Type:               api.PipelineConditionRunning,
			Status:             corev1.ConditionTrue,
			Reason:             "PipelineRunning",
			Message:            "Pipeline has ready pod",
			LastUpdateTime:     metav1.Now(),
			LastTransitionTime: metav1.Now(),
		})
		pod, err := pm.getRunningPodFromSelector(statefulSet.Namespace, statefulSet.Spec.Selector)
		if err != nil {
			return errors.Trace(err)
		}
		err = pm.updateStatusByPod(status, pod)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		if pipeline.Spec.Paused {
			setPipelineCondition(status, api.PipelineCondition{
				Type:               api.PipelineConditionRunning,
				Status:             corev1.ConditionFalse,
				Reason:             "PipelinePaused",
				Message:            "Pipeline paused",
				LastUpdateTime:     metav1.Now(),
				LastTransitionTime: metav1.Now(),
			})
		} else {
			setPipelineCondition(status, api.PipelineCondition{
				Type:               api.PipelineConditionRunning,
				Status:             corev1.ConditionFalse,
				Reason:             "PipelineNotReady",
				Message:            "Pipeline has no ready pod",
				LastUpdateTime:     metav1.Now(),
				LastTransitionTime: metav1.Now(),
			})
		}
	}

	if !reflect.DeepEqual(status, pipeline.Status) {
		pipeline.Status = *status
		pipeline, err := pm.pipeclientset.GravityV1alpha1().Pipelines(pipeline.Namespace).UpdateStatus(pipeline)
		if err != nil {
			if pipeline != nil {
				pm.recorder.Eventf(pipeline, corev1.EventTypeWarning, ErrSyncFailed, MessageSyncFailed, err.Error())
			}
			return errors.Trace(err)
		}
	}
	return nil
}

func (pm *PipelineManager) syncBatch(pipeline *api.Pipeline) error {
	err := pm.kubeclientset.AppsV1().StatefulSets(pipeline.Namespace).Delete(pipeline.Name, utils.ForegroundDeleteOptions)
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Trace(err)
	}

	status := &api.PipelineStatus{
		ObservedGeneration: pipeline.Generation,
		Task:               pipeline.Spec.Task,
		Position:           pipeline.Status.Position,
	}

	if pipeline.Spec.Paused { // job has no replica, delete it
		err := pm.kubeclientset.BatchV1().Jobs(pipeline.Namespace).Delete(pipeline.Name, utils.ForegroundDeleteOptions)
		if err != nil && !apierrors.IsNotFound(err) {
			return errors.Trace(err)
		}
		setPipelineCondition(status, api.PipelineCondition{
			Type:               api.PipelineConditionRunning,
			Status:             corev1.ConditionFalse,
			Reason:             "PipelinePaused",
			Message:            "Pipeline paused",
			LastUpdateTime:     metav1.Now(),
			LastTransitionTime: metav1.Now(),
		})
	} else {
		job, err := pm.jobLister.Jobs(pipeline.Namespace).Get(pipeline.Name)
		if apierrors.IsNotFound(err) {
			_, err = pm.kubeclientset.CoreV1().Services(pipeline.Namespace).Create(pm.newHeadlessService(pipeline))
			if err != nil && !apierrors.IsAlreadyExists(err) {
				return errors.Annotatef(err, "error create headless service for %s", pipeline.Name)
			}
			job, err = pm.kubeclientset.BatchV1().Jobs(pipeline.Namespace).Create(pm.newJob(pipeline))
		}
		// If an error occurs during Get/Create, we'll requeue the item so we can
		// attempt processing again later. This could have been caused by a
		// temporary network failure, or any other transient reason.
		if err != nil {
			pipelineUnavailable.WithLabelValues(pipeline.Name).Set(1)
			pm.recorder.Eventf(pipeline, corev1.EventTypeWarning, ErrSyncFailed, MessageSyncFailed, err.Error())
			return errors.Trace(err)
		}
		if !metav1.IsControlledBy(job, pipeline) {
			msg := fmt.Sprintf(MessageResourceExists, job.Name)
			pm.recorder.Event(pipeline, corev1.EventTypeWarning, ErrResourceExists, msg)
			return nil
		}
		pipelineUnavailable.WithLabelValues(pipeline.Name).Set(float64(job.Status.Failed))
		container := job.Spec.Template.Spec.Containers[0]
		if container.Image != pipeline.Spec.Image || !reflect.DeepEqual(container.Command, pipeline.Spec.Command) {
			job.Spec.Template.Spec.Containers[0].Image = pipeline.Spec.Image
			job.Spec.Template.Spec.Containers[0].Command = pipeline.Spec.Command
			job, err = pm.kubeclientset.BatchV1().Jobs(pipeline.Namespace).Update(job)
			if err != nil {
				pm.recorder.Eventf(pipeline, corev1.EventTypeWarning, ErrSyncFailed, MessageSyncFailed, err.Error())
				return errors.Trace(err)
			} else {
				pm.recorder.Eventf(pipeline, corev1.EventTypeNormal, "Upgraded", "Upgraded job %s to %s:%s", job.Name,
					pipeline.Spec.Image, pipeline.Spec.Command)
			}
		}
		if job.Status.Succeeded > 0 {
			setPipelineCondition(status, api.PipelineCondition{
				Type:               api.PipelineConditionRunning,
				Status:             corev1.ConditionFalse,
				Reason:             "PipelineSucceed",
				Message:            "Pipeline succeed",
				LastUpdateTime:     metav1.Now(),
				LastTransitionTime: metav1.Now(),
			})
		} else if job.Status.Failed > 0 {
			setPipelineCondition(status, api.PipelineCondition{
				Type:               api.PipelineConditionRunning,
				Status:             corev1.ConditionFalse,
				Reason:             "PipelineFailed",
				Message:            "Pipeline failed",
				LastUpdateTime:     metav1.Now(),
				LastTransitionTime: metav1.Now(),
			})
		} else if job.Status.Active > 0 {
			pod, err := pm.getRunningPodFromSelector(job.Namespace, job.Spec.Selector)
			if err != nil {
				setPipelineCondition(status, api.PipelineCondition{
					Type:               api.PipelineConditionRunning,
					Status:             corev1.ConditionFalse,
					Reason:             "PipelineNotReady",
					Message:            "Pipeline has no ready pod",
					LastUpdateTime:     metav1.Now(),
					LastTransitionTime: metav1.Now(),
				})
			} else {
				setPipelineCondition(status, api.PipelineCondition{
					Type:               api.PipelineConditionRunning,
					Status:             corev1.ConditionTrue,
					Reason:             "PipelineRunning",
					Message:            "Pipeline has ready pod",
					LastUpdateTime:     metav1.Now(),
					LastTransitionTime: metav1.Now(),
				})
				err = pm.updateStatusByPod(status, pod)
				if err != nil {
					return errors.Trace(err)
				}
			}
		} else {
			setPipelineCondition(status, api.PipelineCondition{
				Type:               api.PipelineConditionRunning,
				Status:             corev1.ConditionFalse,
				Reason:             "PipelineUnknown",
				LastUpdateTime:     metav1.Now(),
				LastTransitionTime: metav1.Now(),
			})
		}
	}
	setPipelineCondition(status, api.PipelineCondition{
		Type:               api.PipelineConditionStream,
		Status:             corev1.ConditionFalse,
		Reason:             "BatchStage",
		Message:            "Pipeline in batch stage",
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
	})
	if !reflect.DeepEqual(status, pipeline.Status) {
		pipeline.Status = *status
		pipeline, err := pm.pipeclientset.GravityV1alpha1().Pipelines(pipeline.Namespace).UpdateStatus(pipeline)
		if err != nil {
			if pipeline != nil {
				pm.recorder.Eventf(pipeline, corev1.EventTypeWarning, ErrSyncFailed, MessageSyncFailed, err.Error())
			}
			return errors.Trace(err)
		}
	}
	return nil
}

const containerPort = 8080

func pipeLabels(pipeline *api.Pipeline) map[string]string {
	return map[string]string{
		// https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/#labels
		"app.kubernetes.io/name":     "gravity",
		"app.kubernetes.io/instance": pipeline.Name,
	}
}

func (pm *PipelineManager) newHeadlessService(pipeline *api.Pipeline) *corev1.Service {
	lbls := pipeLabels(pipeline)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pipeline.Name,
			Namespace: pipeline.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(pipeline, api.SchemeGroupVersion.WithKind(api.PipelineResourceKind)),
			},
			Labels: lbls,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name: "http",
					Port: containerPort,
				},
			},
			Selector:  lbls,
			ClusterIP: "None",
		},
	}
}

// newStatefulSet creates a new statefulSet for a task. It also sets
// the appropriate OwnerReferences on the resource so handleObject can discover
// the Pipeline resource that 'owns' it.
func (pm *PipelineManager) newStatefulSet(pipeline *api.Pipeline) *appsv1.StatefulSet {
	lbls := pipeLabels(pipeline)

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pipeline.Name,
			Namespace: pipeline.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(pipeline, api.SchemeGroupVersion.WithKind(api.PipelineResourceKind)),
			},
			Labels: lbls,
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: lbls,
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			ServiceName: pipeline.Name,
			Template:    podTemplate(pipeline, corev1.RestartPolicyAlways),
		},
	}

	if pipeline.Spec.Paused {
		statefulSet.Spec.Replicas = int32Ptr(0)
	} else {
		statefulSet.Spec.Replicas = int32Ptr(1)
	}
	return statefulSet
}

func (pm *PipelineManager) newJob(pipeline *api.Pipeline) *batchv1.Job {
	lbls := pipeLabels(pipeline)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pipeline.Name,
			Namespace: pipeline.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(pipeline, api.SchemeGroupVersion.WithKind(api.PipelineResourceKind)),
			},
			Labels: lbls,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: int32Ptr(50),
			Template:     podTemplate(pipeline, corev1.RestartPolicyOnFailure),
		},
	}
}

func podTemplate(pipeline *api.Pipeline, rp corev1.RestartPolicy) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: pipeLabels(pipeline),
		},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: int64Ptr(30),
			RestartPolicy:                 rp,
			Volumes: []corev1.Volume{
				{
					Name: "config",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: pipeline.Name},
						},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:    "gravity",
					Image:   pipeline.Spec.Image,
					Command: pipeline.Spec.Command,
					Ports: []corev1.ContainerPort{
						{
							Name:          "http",
							ContainerPort: containerPort,
						},
					},
					LivenessProbe: &corev1.Probe{
						Handler: corev1.Handler{
							HTTPGet: &corev1.HTTPGetAction{
								Port: intstr.FromString("http"),
								Path: "/healthz",
							},
						},
						InitialDelaySeconds: 10,
						TimeoutSeconds:      5,
						PeriodSeconds:       10,
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "config",
							MountPath: "/etc/gravity",
						},
					},
					Resources: corev1.ResourceRequirements{ //TODO from tps config or metrics
						Requests: corev1.ResourceList{
							"cpu":    resource.MustParse("100m"),
							"memory": resource.MustParse("150M"),
						},
					},
				},
			},
		},
	}
}

func (pm *PipelineManager) updateStatusByPod(status *api.PipelineStatus, pod *corev1.Pod) error {
	status.PodName = pod.Name
	status.Image = pod.Spec.Containers[0].Image
	status.Command = pod.Spec.Containers[0].Command

	url := fmt.Sprintf("http://%s:%d/status", pod.Status.PodIP, pod.Spec.Containers[0].Ports[0].ContainerPort)
	reportStatus, err := getReportStatus(url)
	if err != nil {
		return errors.Trace(err)
	}
	status.ConfigHash = reportStatus.ConfigHash
	status.Position = reportStatus.Position
	stageCond := api.PipelineCondition{
		Type:               api.PipelineConditionStream,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
	}
	switch reportStatus.Stage {
	case core.ReportStageIncremental:
		stageCond.Status = corev1.ConditionTrue
		stageCond.Reason = "StreamStage"
		stageCond.Message = "Pipeline in stream stage"
	case core.ReportStageFull:
		stageCond.Status = corev1.ConditionFalse
		stageCond.Reason = "BatchStage"
		stageCond.Message = "Pipeline in batch stage"
	default:
		return errors.Errorf("unknown report stage %s", reportStatus.Stage)
	}
	setPipelineCondition(status, stageCond)
	return nil
}

func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			if cond.Status == corev1.ConditionTrue {
				return true
			}
		}
	}
	return false
}

func getReportStatus(url string) (*core.TaskReportStatus, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, errors.Annotatef(err, "fail to get report status, url: %s", url)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Annotatef(err, "fail to read report status, url: %s", url)
	}

	reportStatus := &core.TaskReportStatus{}
	err = json.Unmarshal(body, reportStatus)
	if err != nil {
		return nil, errors.Annotatef(err, "fail to unmarshal task report status. url: %s, body: %s", url, string(body))
	}

	return reportStatus, nil
}

// enqueuePipeline takes a Pipeline resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Pipeline.
func (pm *PipelineManager) enqueuePipeline(obj *api.Pipeline) {
	var key string
	var err error
	if key, err = cache.DeletionHandlingMetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	pm.workqueue.AddRateLimited(key)
}

func (pm *PipelineManager) enqueuePipelineAfter(obj *api.Pipeline, duration time.Duration) {
	var key string
	var err error
	if key, err = cache.DeletionHandlingMetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	pm.workqueue.AddAfter(key, duration)
}

// handleObject will take any resource implementing metav1.Object and attempt
// to find the Pipeline resource that 'owns' it. It does this by looking at the
// objects metadata.ownerReferences field for an appropriate OwnerReference.
// It then enqueues that Pipeline resource to be processed. If the object does not
// have an appropriate OwnerReference, it will simply be skipped.
func (pm *PipelineManager) handleObject(obj interface{}) {
	var object metav1.Object
	var ok bool
	if object, ok = obj.(metav1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
		log.Infof("Recovered deleted object '%s' from tombstone", object.GetName())
	}
	if ownerRef := metav1.GetControllerOf(object); ownerRef != nil {
		// If this object is not owned by a Pipeline, we should not do anything more
		// with it.
		if ownerRef.Kind != api.PipelineResourceKind {
			return
		}

		pipeline, err := pm.pipelinesLister.Pipelines(object.GetNamespace()).Get(ownerRef.Name)
		if err != nil {
			log.Infof("ignoring orphaned object '%s' of pipeline '%s'", object.GetSelfLink(), ownerRef.Name)
			return
		}

		log.Infof("[PipelineManager.handleObject] enqueue pipeline %s due to %s", pipeline.Name, object.GetSelfLink())
		pm.enqueuePipeline(pipeline)
		return
	}
}

func (pm *PipelineManager) Reset(pipeline *api.Pipeline) error {
	if pipeline.Status.Available() {
		return pm.resetRunning(pipeline)
	} else {
		return pm.resetPaused(pipeline)
	}
}

func (pm *PipelineManager) needSync(old *api.Pipeline, newP *api.Pipeline) bool {
	if !reflect.DeepEqual(newP.Status.Task, newP.Spec.Task) {
		return true
	}

	if !reflect.DeepEqual(old.Spec, newP.Spec) {
		return true
	}

	return false
}

func (pm *PipelineManager) resetRunning(pipeline *api.Pipeline) error {
	pods, err := pm.podLister.Pods(pipeline.Namespace).List(labels.SelectorFromSet(pipeLabels(pipeline)))
	if err != nil {
		return errors.Annotatef(err, "fail to list pod for pipeline %s", pipeline.Name)
	}

	var readyPods []*corev1.Pod
	for _, pod := range pods {
		if isPodReady(pod) {
			readyPods = append(readyPods, pod)
		}
	}

	for _, pod := range readyPods {
		url := fmt.Sprintf("http://%s:%d/reset", pod.Status.PodIP, pod.Spec.Containers[0].Ports[0].ContainerPort)
		resp, err := http.Post(url, "application/json", nil)
		if err != nil {
			return errors.Annotatef(err, "fail to request %s", url)
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Annotatef(err, "fail to read body of %s", url)
		}

		if resp.StatusCode != 200 {
			return errors.Annotatef(err, "fail to request %s, response is %s", url, string(body))
		}

		log.Infof("reset full succeed for %s, response %s", pod.Name, string(body))
	}

	return nil
}

func (pm *PipelineManager) getRunningPodFromSelector(namespace string, labelSelector *metav1.LabelSelector) (*corev1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(labelSelector)
	if err != nil {
		return nil, err
	}
	pods, err := pm.podLister.Pods(namespace).List(selector)
	if err != nil {
		return nil, errors.Annotatef(err, "fail to list pod by selector %s", labelSelector.String())
	}

	if len(pods) != 1 {
		return nil, errors.Errorf("expect 1 pod for selector %s, actually %d", labelSelector.String(), len(pods))
	}

	pod := pods[0]

	if !isPodReady(pod) {
		return nil, errors.Errorf("pod %s not ready", pod.Name)
	}
	return pod, nil
}

func (pm *PipelineManager) resetPaused(pipeline *api.Pipeline) error {
	cm, err := pm.kubeclientset.CoreV1().ConfigMaps(pipeline.Namespace).Get(pipeline.Name, metav1.GetOptions{})
	if err != nil {
		return errors.Annotatef(err, "[PipelineManager.resetPaused] can't get config map for %s", pipeline.Name)
	}

	cfgV3 := config.PipelineConfigV3{}
	err = json.Unmarshal([]byte(cm.Data[api.ConfigFileKey]), &cfgV3)
	if err != nil {
		return errors.Annotatef(err, "[PipelineManager.resetPaused] error unmarshal gravity config v3: %s", cm.Data[api.ConfigFileKey])
	}
	server, err := app.NewServer(cfgV3)
	if err != nil {
		return errors.Annotatef(err, "[PipelineManager.resetPaused] error NewServer")
	}
	return server.PositionCache.Clear()
}

func int32Ptr(i int32) *int32 { return &i }
func int64Ptr(i int64) *int64 { return &i }

func setPipelineCondition(status *api.PipelineStatus, condition api.PipelineCondition) {
	curCond := status.Condition(condition.Type)
	if curCond != nil && curCond.Status == condition.Status && curCond.Reason == condition.Reason {
		return
	}

	if curCond != nil && curCond.Status == condition.Status {
		condition.LastTransitionTime = curCond.LastTransitionTime
	}

	for i := range status.Conditions {
		if status.Conditions[i].Type == condition.Type {
			status.Conditions[i] = condition
			return
		}
	}
	status.Conditions = append(status.Conditions, condition)
}
