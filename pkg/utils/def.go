package utils

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

var deleteFore = metav1.DeletePropagationForeground
var ForegroundDeleteOptions = metav1.NewDeleteOptions(10)

func init() {
	ForegroundDeleteOptions.PropagationPolicy = &deleteFore
}
