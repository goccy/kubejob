package kubejob_test

import (
	"context"
	"testing"

	"github.com/goccy/kubejob"
	"golang.org/x/xerrors"
	batchv1 "k8s.io/api/batch/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)

var (
	cfg *rest.Config
)

func init() {
	c, err := rest.InClusterConfig()
	if err != nil {
		panic(err)
	}
	cfg = c
}

func Test_Run(t *testing.T) {
	job, err := kubejob.NewJobBuilder(cfg, "default").BuildWithJob(&batchv1.Job{
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

func Test_RunnerWithExecutionHandler(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		job, err := kubejob.NewJobBuilder(cfg, "default").BuildWithJob(&batchv1.Job{
			Spec: batchv1.JobSpec{
				Template: apiv1.PodTemplateSpec{
					Spec: apiv1.PodSpec{
						Containers: []apiv1.Container{
							{
								Name:    "test",
								Image:   "golang:1.15",
								Command: []string{"echo", "$TEST"},
								Env: []apiv1.EnvVar{
									{
										Name:  "TEST",
										Value: "kubejob",
									},
								},
							},
						},
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("failed to build job: %+v", err)
		}
		if err := job.RunWithExecutionHandler(context.Background(), func(executors []*kubejob.JobExecutor) error {
			for _, exec := range executors {
				out, err := exec.Exec()
				if err != nil {
					t.Fatalf("%s: %+v", string(out), err)
				}
				if string(out) != "kubejob\n" {
					t.Fatalf("cannot get output %q", string(out))
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("failed to run: %+v", err)
		}
	})
	t.Run("failure", func(t *testing.T) {
		job, err := kubejob.NewJobBuilder(cfg, "default").BuildWithJob(&batchv1.Job{
			Spec: batchv1.JobSpec{
				Template: apiv1.PodTemplateSpec{
					Spec: apiv1.PodSpec{
						Containers: []apiv1.Container{
							{
								Name:    "test",
								Image:   "golang:1.15",
								Command: []string{"cat", "fuga"},
							},
						},
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("failed to build job: %+v", err)
		}
		if err := job.RunWithExecutionHandler(context.Background(), func(executors []*kubejob.JobExecutor) error {
			for _, exec := range executors {
				out, err := exec.Exec()
				if err == nil {
					t.Fatal("expect error")
				}
				var failedJob *kubejob.FailedJob
				if xerrors.As(err, &failedJob) {
					for _, container := range failedJob.FailedContainers() {
						if container.Name != "test" {
							t.Fatalf("cannot get valid container: %s", container.Name)
						}
					}
				} else {
					t.Fatal("cannot get FailedJob")
				}
				if string(out) != "cat: fuga: No such file or directory\n" {
					t.Fatalf("cannot get output %q", string(out))
				}
			}
			return nil
		}); err == nil {
			t.Fatal("expect error")
		}
	})
}

func Test_RunnerWithInitContainers(t *testing.T) {
	job, err := kubejob.NewJobBuilder(cfg, "default").BuildWithJob(&batchv1.Job{
		Spec: batchv1.JobSpec{
			Template: apiv1.PodTemplateSpec{
				Spec: apiv1.PodSpec{
					InitContainers: []apiv1.Container{
						{
							Name:    "init-touch",
							Image:   "golang:1.15",
							Command: []string{"touch", "/tmp/mnt/hello.txt"},
							VolumeMounts: []apiv1.VolumeMount{
								{
									Name:      "shared",
									MountPath: "/tmp/mnt",
								},
							},
						},
					},
					Containers: []apiv1.Container{
						{
							Name:    "confirm",
							Image:   "golang:1.15",
							Command: []string{"ls", "/tmp/mnt/hello.txt"},
							VolumeMounts: []apiv1.VolumeMount{
								{
									Name:      "shared",
									MountPath: "/tmp/mnt",
								},
							},
						},
					},
					Volumes: []apiv1.Volume{
						{
							Name: "shared",
							VolumeSource: apiv1.VolumeSource{
								EmptyDir: &apiv1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to build job: %+v", err)
	}
	if err := job.RunWithExecutionHandler(context.Background(), func(executors []*kubejob.JobExecutor) error {
		for _, exec := range executors {
			out, err := exec.Exec()
			if err != nil {
				t.Fatalf("%s: %+v", string(out), err)
			}
			if string(out) != "/tmp/mnt/hello.txt\n" {
				t.Fatalf("cannot get output %q", string(out))
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("failed to run: %+v", err)
	}
}
