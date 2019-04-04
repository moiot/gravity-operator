package apiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"time"

	"github.com/moiot/gravity/pkg/app"
	"github.com/moiot/gravity/pkg/config"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/moiot/gravity/pkg/utils/retry"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	clusterapi "github.com/moiot/gravity-operator/pkg/apis/cluster/v1alpha1"
	pipeapi "github.com/moiot/gravity-operator/pkg/apis/pipeline/v1alpha1"
	client "github.com/moiot/gravity-operator/pkg/client/pipeline/clientset/versioned"
	"github.com/moiot/gravity-operator/pkg/controller"
)

var (
	pauseErrorMsgCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gravity",
		Subsystem: "operator",
		Name:      "pause_msg_error_counter",
		Help:      "Number of pause msg error",
	}, []string{"pipeline"})

	resumeErrorCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gravity",
		Subsystem: "operator",
		Name:      "resume_error_counter",
		Help:      "Number of resume error",
	}, []string{"pipeline"})
)

func init() {
	prometheus.MustRegister(pauseErrorMsgCount)
	prometheus.MustRegister(resumeErrorCount)
}

func OperatorURI() string {
	return os.Getenv("MY_SERVICE_URI")
}

type ApiServer struct {
	srv           *http.Server
	kubeclientset kubernetes.Interface
	pipeclientset client.Interface
	controller    controller.Interface
	namespace     string
}

func (s *ApiServer) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	s.srv.Handler.ServeHTTP(resp, req)
}

func NewApiServer(
	namespace string,
	kubeclientset kubernetes.Interface,
	pipeclientset client.Interface,
	controller controller.Interface,
	port int) *ApiServer {

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: router,
	}
	apiServer := &ApiServer{
		namespace:     namespace,
		kubeclientset: kubeclientset,
		pipeclientset: pipeclientset,
		controller:    controller,
		srv:           srv,
	}

	router.Any("/metrics", gin.WrapH(promhttp.Handler()))

	router.GET("/debug/pprof/", gin.WrapF(pprof.Index))
	router.GET("/debug/pprof/cmdline", gin.WrapF(pprof.Cmdline))
	router.GET("/debug/pprof/profile", gin.WrapF(pprof.Profile))
	router.GET("/debug/pprof/symbol", gin.WrapF(pprof.Symbol))
	router.GET("/debug/pprof/trace", gin.WrapF(pprof.Trace))
	router.GET("/debug/pprof/block", gin.WrapH(pprof.Handler("block")))
	router.GET("/debug/pprof/goroutine", gin.WrapH(pprof.Handler("goroutine")))
	router.GET("/debug/pprof/heap", gin.WrapH(pprof.Handler("heap")))
	router.GET("/debug/pprof/mutex", gin.WrapH(pprof.Handler("mutex")))
	router.GET("/debug/pprof/threadcreate", gin.WrapH(pprof.Handler("threadcreate")))

	router.POST("/pipeline", apiServer.createPipe)
	router.PUT("/pipeline/:name", apiServer.updatePipe)
	router.GET("/pipeline/:name", apiServer.getPipe)
	router.GET("/pipeline", apiServer.listPipe)
	router.POST("/pipeline/:name/reset", apiServer.reset)
	router.DELETE("/pipeline/:name", apiServer.deletePipe)
	router.POST("/pipeline/:name/pause", apiServer.pausePipe)
	router.POST("/pipeline/:name/resume", apiServer.resumePipe)

	router.POST("/cronjobs", apiServer.createCronJob)
	router.GET("/cronjobs", apiServer.listCronJob)
	router.GET("/cronjobs/:name", apiServer.getCronJob)
	router.PUT("/cronjobs/:name", apiServer.updateCronJob)
	router.DELETE("/cronjobs/:name", apiServer.deleteCronJob)

	router.GET("/pipeline/:name/cronjobs", apiServer.listPipelineCronJob)
	return apiServer
}

func (s *ApiServer) Start() {
	go func() {
		if err := s.srv.ListenAndServe(); err != nil {
			log.Infof("[ApiServer] closed with %s", err)
		}
	}()
	log.Info("[ApiServer] Started")
}

func (s *ApiServer) Stop() {
	log.Info("[ApiServer] Stopping")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.srv.Shutdown(ctx); err != nil {
		log.Error("[ApiServer] error: ", err)
	}
	log.Info("[ApiServer] Stopped")
}

func (s *ApiServer) createPipe(c *gin.Context) {
	var request ApiPipeline
	if err := c.BindJSON(&request); err != nil {
		log.Errorf("[ApiServer.createPipe] bind json error. %s", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}

	if err := request.validate(); err != nil {
		log.Errorf("[ApiServer.createPipe] error Validate task spec: %s. %#v", err, request.Spec)
		c.JSON(http.StatusBadRequest, gin.H{"error": "error Validate task spec: " + err.Error()})
		return
	}

	pipeline := request.toK8()
	pipeline.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(s.controller.GetCluster(), pipeapi.SchemeGroupVersion.WithKind(clusterapi.ClusterResourceKind)),
	}
	cluster := s.controller.GetCluster()
	rule := cluster.FindDeploymentRule(pipeline.Name)
	if rule == nil {
		log.Errorf("[ApiServer.createPipe] can't find deployment rule for pipeline %s", request.Name)
		c.JSON(http.StatusBadRequest, gin.H{"error": "can't find deployment rule"})
		return
	}
	pipeline.Spec.Image = rule.Image
	pipeline.Spec.Command = rule.Command

	pipeline, err := s.pipeclientset.GravityV1alpha1().Pipelines(s.namespace).Create(pipeline)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			log.Errorf("[ApiServer.createPipe] error duplicate name %s. err: %s", request.Name, err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "pipeline " + request.Name + " already exists"})
			return
		}
		log.Errorf("[ApiServer.createPipe] error create pipeline. %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	config := request.newConfigMap(pipeline)
	_, err = s.kubeclientset.CoreV1().ConfigMaps(s.namespace).Create(config)
	if err != nil {
		_ = s.pipeclientset.GravityV1alpha1().Pipelines(pipeline.Namespace).Delete(pipeline.Name, &metav1.DeleteOptions{})
		log.Errorf("[ApiServer.createPipe] error create config. %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	log.Infof("[ApiServer.createPipe] successfully created %s", pipeline.Name)
	c.Header("Location", url.PathEscape(fmt.Sprintf("/pipeline/%s", pipeline.Name)))
	c.JSON(http.StatusCreated, gin.H{"pipelineId": pipeline.UID, "name": pipeline.Name, "msg": fmt.Sprintf("created pipeline %s", pipeline.Name)})
}

func (s *ApiServer) updatePipe(c *gin.Context) {
	request := &ApiPipeline{}
	if err := c.BindJSON(&request); err != nil {
		log.Errorf("[ApiServer.updatePipe] fail parse request. %s", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to parse parameter: " + err.Error()})
		return
	}

	if err := request.validate(); err != nil {
		log.Errorf("[ApiServer.updatePipe] error Validate task spec: %s. %#v", err, request.Spec)
		c.JSON(http.StatusBadRequest, gin.H{"error": "error Validate task spec: " + err.Error()})
		return
	}

	name := c.Param("name")
	pipeline, err := s.controller.GetK8Pipeline(s.namespace, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "no pipeline found for " + c.Param("name")})
			return
		} else {
			log.Errorf("[ApiServer.updatePipe] error get pipeline. %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err})
			return
		}
	}

	if pipeline.Name != request.Name {
		c.JSON(http.StatusBadRequest, gin.H{"error": "pipeline name is unmodifiable"})
		return
	}

	if pipeline.Generation != request.Generation {
		c.JSON(http.StatusConflict, gin.H{"error": "pipeline has been updated. please retry."})
		return
	}

	newPipeline := request.toK8()
	newPipeline.ResourceVersion = pipeline.ResourceVersion
	newPipeline, err = s.pipeclientset.GravityV1alpha1().Pipelines(s.namespace).Update(newPipeline)
	if err != nil {
		if apierrors.IsConflict(err) {
			log.Errorf("[ApiServer.updatePipe] pipeline %s has been updated. err: %s", request.Name, err)
			c.JSON(http.StatusConflict, gin.H{"error": "pipeline has been updated. please retry. "})
			return
		}
		log.Errorf("[ApiServer.updatePipe] error update %v. %v", newPipeline, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	if pipeline.Spec.ConfigHash != newPipeline.Spec.ConfigHash {
		configMap := request.newConfigMap(newPipeline)
		_, err := s.kubeclientset.CoreV1().ConfigMaps(s.namespace).Update(configMap)
		if err != nil {
			log.Errorf("[ApiServer.updatePipe] error update config. %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"msg": "update success"})
}

func (s *ApiServer) pausePipe(c *gin.Context) {
	name := c.Param("name")

	err := s.updatePauseSpecWithRetry(name, true)
	if err != nil {
		pauseErrorMsgCount.WithLabelValues(name).Add(1)
		log.Errorf("[ApiServer.pausePipe] error cannot pause: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"msg": "pause msg sent"})
}

func (s *ApiServer) resumePipe(c *gin.Context) {
	name := c.Param("name")
	err := s.updatePauseSpecWithRetry(name, false)
	if err != nil {
		resumeErrorCount.WithLabelValues(name).Add(1)
		log.Errorf("[ApiServer.pausePipe] error cannot resume: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"msg": "resume msg sent"})
}

func (s *ApiServer) updatePauseSpecWithRetry(name string, expected bool) error {
	// Ignore the error when pipeline cannot be found;
	// retry 6 times for other cases
	err := retry.Do(func() error {
		pipeline, err := s.controller.GetK8Pipeline(s.namespace, name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			} else {
				return err
			}
		}
		pipeline.Spec.Paused = expected
		_, err = s.pipeclientset.GravityV1alpha1().Pipelines(s.namespace).Update(pipeline)
		return err
	}, 6, 1)
	return err
}

func (s *ApiServer) getPipe(c *gin.Context) {
	name := c.Param("name")
	pipeline, err := s.controller.GetK8Pipeline(s.namespace, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "no pipeline found for " + c.Param("name")})
			return
		} else {
			log.Errorf("[ApiServer.getPipe] error get pipeline. %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err})
			return
		}
	}

	ret := &ApiPipeline{}
	ret.fromK8(pipeline)

	configMaps, err := s.kubeclientset.CoreV1().ConfigMaps(s.namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		log.Errorf("[ApiServer.getPipe] error get config maps. %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	raw := json.RawMessage(configMaps.Data[pipeapi.ConfigFileKey])
	ret.Spec.Config = &raw
	c.JSON(http.StatusOK, ret)
}

func (s *ApiServer) reset(c *gin.Context) {
	name := c.Param("name")
	pipeline, err := s.controller.GetK8Pipeline(s.namespace, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "no pipeline found for " + c.Param("name")})
			return
		} else {
			log.Errorf("[ApiServer.reset] error get pipeline. %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err})
			return
		}
	}

	if err := s.controller.Reset(pipeline); err != nil {
		log.Errorf("[ApiServer.reset] error reset. %s", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	c.JSON(http.StatusOK, gin.H{"msg": "succeed"})
}

func (s *ApiServer) listPipe(c *gin.Context) {
	pipelineList, err := s.controller.ListK8Pipelines(s.namespace)
	if err != nil {
		log.Errorf("[ApiServer.listPipe] error list pipeline. %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	ret := make([]ApiPipeline, len(pipelineList))
	for i, p := range pipelineList {
		vo := &ApiPipeline{}
		vo.fromK8(p)
		ret[i] = *vo
	}
	c.JSON(http.StatusOK, ret)
}

func (s *ApiServer) deletePipe(c *gin.Context) {
	name := c.Param("name")
	configMaps, err := s.kubeclientset.CoreV1().ConfigMaps(s.namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		log.Errorf("[ApiServer.deletePipe] error get config for %s. %v", name, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	cfgV3 := &config.PipelineConfigV3{}
	err = json.Unmarshal([]byte(configMaps.Data[pipeapi.ConfigFileKey]), cfgV3)
	if err != nil {
		log.Errorf("[ApiServer.deletePipe] error unmarshal gravity config: %s. err: %v", configMaps.Data[pipeapi.ConfigFileKey], err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	srv, err := app.NewServer(*cfgV3)
	if err != nil {
		log.Errorf("[ApiServer.deletePipe] error parse gravity cfg: %s. %v.", configMaps.Data[pipeapi.ConfigFileKey], err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	if err = srv.PositionCache.Clear(); err != nil {
		log.Errorf("[ApiServer.deletePipe] error clear gravity position: %v.", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	err = s.pipeclientset.GravityV1alpha1().Pipelines(s.namespace).Delete(name, &metav1.DeleteOptions{})
	if err != nil {
		log.Errorf("[ApiServer.deletePipe] %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	c.JSON(http.StatusOK, gin.H{"msg": "deleted"})
}

func (s *ApiServer) createCronJob(c *gin.Context) {
	var request ApiCronJob
	if err := c.BindJSON(&request); err != nil {
		log.Errorf("[ApiServer.createCronJob] bind json error: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}

	if err := request.Validate(); err != nil {
		log.Errorf("[ApiServer.createCronJob] error: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
	}

	pipeline, err := s.controller.GetK8Pipeline(s.namespace, request.PipelineName)
	if err != nil {
		log.Errorf("[ApiServer.createCronJob] failed to get pipeline: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}

	jobSpec := request.tok8(OperatorURI(), pipeline, request.Action)
	if _, err := s.kubeclientset.BatchV1beta1().CronJobs(s.namespace).Create(jobSpec); err != nil {
		log.Errorf("[ApiServer.createConJob] failed to create cronjob: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}

	c.JSON(http.StatusOK, gin.H{"msg": "created"})
}

func (s *ApiServer) listCronJob(c *gin.Context) {
	selector := labels.SelectorFromSet(map[string]string{"app.kubernetes.io/name": "gravity-cronjob"})
	jobList, err := s.kubeclientset.BatchV1beta1().CronJobs(s.namespace).List(metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		log.Errorf("[ApiServer.listCronJob] failed to list cronjob: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}

	apiCronJobList := make([]ApiCronJob, len(jobList.Items))
	for i, job := range jobList.Items {
		apiCronJobList[i] = ApiCronJob{
			CronJobName:  job.Name,
			PipelineName: job.Labels["pipeline"],
			Schedule:     job.Annotations["schedule"],
			Action:       job.Annotations["action"],
		}
	}

	c.JSON(http.StatusOK, apiCronJobList)
}

func (s *ApiServer) listPipelineCronJob(c *gin.Context) {
	pipelineName := c.Param("name")

	selector := labels.SelectorFromSet(map[string]string{"app.kubernetes.io/name": "gravity-cronjob", "pipeline": pipelineName})

	jobList, err := s.kubeclientset.BatchV1beta1().CronJobs(s.namespace).List(metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		log.Errorf("[ApiServer.listCronJob] failed to list cronjob: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}

	apiCronJobList := make([]ApiCronJob, len(jobList.Items))
	for i, job := range jobList.Items {
		apiCronJobList[i] = ApiCronJob{
			CronJobName:  job.Name,
			PipelineName: job.Labels["pipeline"],
			Schedule:     job.Annotations["schedule"],
			Action:       job.Annotations["action"],
		}
	}

	c.JSON(http.StatusOK, apiCronJobList)
}

func (s *ApiServer) getCronJob(c *gin.Context) {
	jobName := c.Param("name")

	job, err := s.kubeclientset.BatchV1beta1().CronJobs(s.namespace).Get(jobName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("[ApiServer.getCronJob] failed to get cronjob name: %v, err: %v", jobName, err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}

	c.JSON(
		http.StatusOK,
		ApiCronJob{
			CronJobName:  jobName,
			PipelineName: job.Labels["pipeline"],
			Schedule:     job.Annotations["schedule"],
			Action:       job.Annotations["action"],
		})
}

func (s *ApiServer) updateCronJob(c *gin.Context) {
	var request ApiCronJob
	if err := c.BindJSON(&request); err != nil {
		log.Errorf("[ApiServer.updateCronJob] bind json error: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := request.Validate(); err != nil {
		log.Errorf("[ApiServer.updateCronJob] bad parameter: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	jobName := c.Param("name")
	job, err := s.kubeclientset.BatchV1beta1().CronJobs(s.namespace).Get(jobName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("[ApiServer.getCronJob] failed to get cronjob name: %v, err: %v", jobName, err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	job.Spec.Schedule = request.Schedule
	job.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Command = containerCommand(request.Action, OperatorURI(), job.Labels["pipeline"])
	job.Annotations["action"] = request.Action
	job.Annotations["schedule"] = request.Schedule

	if _, err := s.kubeclientset.BatchV1beta1().CronJobs(s.namespace).Update(job); err != nil {
		log.Errorf("[ApiServer.updateCronJob] failed name: %v, err: %v", jobName, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"msg": "updated"})
}

func (s *ApiServer) deleteCronJob(c *gin.Context) {
	jobName := c.Param("name")

	err := s.kubeclientset.BatchV1beta1().CronJobs(s.namespace).Delete(jobName, nil)
	if err != nil {
		log.Errorf("[ApiServer.deleteCronJob] failed to delete cronjob name: %v, err: %v", jobName, err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}

	c.JSON(http.StatusOK, gin.H{"msg": "deleted"})
}
