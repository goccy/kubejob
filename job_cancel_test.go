package kubejob

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typedbatchv1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	"k8s.io/client-go/rest"
)

const (
	goTestImageName = "golang:1.22.0-bookworm"
)

type jobClientWithBeforeCancel struct {
	typedbatchv1.JobInterface
	cancel func()
}

func (c *jobClientWithBeforeCancel) Create(ctx context.Context, job *batchv1.Job, opt metav1.CreateOptions) (*batchv1.Job, error) {
	if c.cancel != nil {
		c.cancel()
	}
	return c.JobInterface.Create(ctx, job, opt)
}

type jobClientWithAfterCancel struct {
	typedbatchv1.JobInterface
	cancel func()
}

func (c *jobClientWithAfterCancel) Create(ctx context.Context, job *batchv1.Job, opt metav1.CreateOptions) (*batchv1.Job, error) {
	j, err := c.JobInterface.Create(ctx, job, opt)
	if c.cancel != nil {
		c.cancel()
	}
	return j, err
}

func TestJobCancel(t *testing.T) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		t.Fatal(err)
	}

	ttl := int32(0)

	t.Run("before job creation", func(t *testing.T) {
		job, err := NewJobBuilder(cfg, "default").BuildWithJob(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "cancel-before-job-creation-",
			},
			Spec: batchv1.JobSpec{
				TTLSecondsAfterFinished: &ttl,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:    "main",
								Image:   goTestImageName,
								Command: []string{"sleep", "600"},
							},
							{
								Name:    "sidecar",
								Image:   "nginx:latest",
								Command: []string{"nginx"},
							},
						},
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("failed to build job: %+v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		job.jobClient = &jobClientWithBeforeCancel{
			JobInterface: job.jobClient,
			cancel:       cancel,
		}

		var calledFinalizer bool
		if err := job.RunWithExecutionHandler(ctx, func(ctx context.Context, executors []*JobExecutor) error {
			for _, exec := range executors {
				if exec.Container.Name == "sidecar" {
					exec.ExecAsync(ctx)
				} else {
					out, err := exec.Exec(ctx)
					if err != nil {
						t.Fatalf("%s: %+v", string(out), err)
					}
					t.Log(string(out))
				}
			}
			return nil
		}, &JobFinalizer{
			Container: corev1.Container{
				Name:    "finalizer",
				Image:   goTestImageName,
				Command: []string{"echo", "finalizer"},
			},
			Handler: func(ctx context.Context, exec *JobExecutor) error {
				out, err := exec.ExecOnly(ctx)
				if err != nil {
					t.Fatalf("%s: %+v", string(out), err)
				}
				if string(out) != "finalizer\n" {
					t.Fatalf("failed to get output from finalizer: %q", string(out))
				}
				calledFinalizer = true
				return nil
			},
		}); err != nil {
			t.Fatalf("failed to run: %+v", err)
		}
		if !calledFinalizer {
			t.Fatal("couldn't call finalizer")
		}
		if existsJob(t, cfg, "cancel-before-job-creation-") {
			t.Fatal("failed to delete job by ttl")
		}
	})

	t.Run("after job creation and before pod running", func(t *testing.T) {
		job, err := NewJobBuilder(cfg, "default").BuildWithJob(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "cancel-after-job-creation-",
			},
			Spec: batchv1.JobSpec{
				TTLSecondsAfterFinished: &ttl,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:    "main",
								Image:   goTestImageName,
								Command: []string{"sleep", "600"},
							},
							{
								Name:    "sidecar",
								Image:   "nginx:latest",
								Command: []string{"nginx"},
							},
						},
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("failed to build job: %+v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		job.jobClient = &jobClientWithAfterCancel{
			JobInterface: job.jobClient,
			cancel:       cancel,
		}

		var calledFinalizer bool
		if err := job.RunWithExecutionHandler(ctx, func(ctx context.Context, executors []*JobExecutor) error {
			for _, exec := range executors {
				if exec.Container.Name == "sidecar" {
					exec.ExecAsync(ctx)
				} else {
					out, err := exec.Exec(ctx)
					if err != nil {
						t.Fatalf("%s: %+v", string(out), err)
					}
					t.Log(string(out))
				}
			}
			return nil
		}, &JobFinalizer{
			Container: corev1.Container{
				Name:    "finalizer",
				Image:   goTestImageName,
				Command: []string{"echo", "finalizer"},
			},
			Handler: func(ctx context.Context, exec *JobExecutor) error {
				out, err := exec.ExecOnly(ctx)
				if err != nil {
					t.Fatalf("%s: %+v", string(out), err)
				}
				if string(out) != "finalizer\n" {
					t.Fatalf("failed to get output from finalizer: %q", string(out))
				}
				calledFinalizer = true
				return nil
			},
		}); err != nil {
			t.Fatalf("failed to run: %+v", err)
		}
		if !calledFinalizer {
			t.Fatal("couldn't call finalizer")
		}
		if existsJob(t, cfg, "cancel-after-job-creation-") {
			t.Fatal("failed to delete job by ttl")
		}
	})

	t.Run("after pod running", func(t *testing.T) {
		job, err := NewJobBuilder(cfg, "default").BuildWithJob(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "after-running-",
			},
			Spec: batchv1.JobSpec{
				TTLSecondsAfterFinished: &ttl,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:    "main",
								Image:   goTestImageName,
								Command: []string{"sleep", "600"},
							},
							{
								Name:    "sidecar",
								Image:   "nginx:latest",
								Command: []string{"nginx"},
							},
						},
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("failed to build job: %+v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var calledFinalizer bool
		if err := job.RunWithExecutionHandler(ctx, func(ctx context.Context, executors []*JobExecutor) error {
			cancel()
			for _, exec := range executors {
				if exec.Container.Name == "sidecar" {
					exec.ExecAsync(ctx)
				} else {
					out, err := exec.Exec(ctx)
					if err != nil {
						t.Fatalf("%s: %+v", string(out), err)
					}
					t.Log(string(out))
				}
			}
			return nil
		}, &JobFinalizer{
			Container: corev1.Container{
				Name:    "finalizer",
				Image:   goTestImageName,
				Command: []string{"echo", "finalizer"},
			},
			Handler: func(ctx context.Context, exec *JobExecutor) error {
				out, err := exec.ExecOnly(ctx)
				if err != nil {
					t.Fatalf("%s: %+v", string(out), err)
				}
				if string(out) != "finalizer\n" {
					t.Fatalf("failed to get output from finalizer: %q", string(out))
				}
				calledFinalizer = true
				return nil
			},
		}); err != nil {
			t.Fatalf("failed to run: %+v", err)
		}
		if !calledFinalizer {
			t.Fatal("couldn't call finalizer")
		}

		if existsJob(t, cfg, "after-running-") {
			t.Fatal("failed to delete job by ttl")
		}
	})
}

func existsJob(t *testing.T, cfg *rest.Config, prefix string) bool {
	t.Helper()
	time.Sleep(5 * time.Second)
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	list, err := clientset.BatchV1().Jobs("default").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range list.Items {
		if strings.HasPrefix(job.GetName(), prefix) {
			return true
		}
	}
	return false
}
