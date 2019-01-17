package controller

import "github.com/prometheus/client_golang/prometheus"

var syncCount = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "gravity",
	Subsystem: "operator",
	Name:      "sync_counter",
	Help:      "Number of sync",
}, []string{"pipeline", "controller", "result"})

var pauseErrorCount = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "gravity",
	Subsystem: "operator",
	Name:      "pause_error_counter",
	Help:      "Number of pause error",
}, []string{"pipeline"})

var scheduleHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "gravity",
	Subsystem: "operator",
	Name:      "schedule_duration_seconds",
	Help:      "Bucketed histogram of processing time of schedule.",
	Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
}, []string{"pipeline", "controller"})

var pipelineUnavailable = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "gravity",
	Subsystem: "operator",
	Name:      "pipeline_unavailable",
	Help:      "Number of pipeline unavailable",
}, []string{"pipeline"})

var clusterStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "gravity",
	Subsystem: "operator",
	Name:      "cluster_status",
	Help:      "Number of pipelines in different status",
}, []string{"status"})

func init() {
	prometheus.MustRegister(syncCount)
	prometheus.MustRegister(pauseErrorCount)
	prometheus.MustRegister(scheduleHistogram)
	prometheus.MustRegister(pipelineUnavailable)
	prometheus.MustRegister(clusterStatus)
}
