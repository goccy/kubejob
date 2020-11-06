package kubejob_test

import (
	"context"
	"testing"

	"github.com/goccy/kubejob"
	batchv1 "k8s.io/api/batch/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	cfg       *rest.Config
	clientset *kubernetes.Clientset
)

func init() {
	c, err := rest.InClusterConfig()
	if err != nil {
		panic(err)
	}
	set, err := kubernetes.NewForConfig(c)
	if err != nil {
		panic(err)
	}
	cfg = c
	clientset = set
}

func Test_Run(t *testing.T) {
	job, err := kubejob.NewJobBuilder(clientset, "default").BuildWithJob(&batchv1.Job{
		Spec: batchv1.JobSpec{
			Template: apiv1.PodTemplateSpec{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name:    "test",
							Image:   "golang:1.15",
							Command: []string{"echo", "hello"},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to build job: %+v", err)
	}
	if err := job.Run(context.Background()); err != nil {
		t.Fatalf("failed to run: %+v", err)
	}
}
