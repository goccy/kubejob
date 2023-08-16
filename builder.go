package kubejob

import (
	"fmt"
	"io"

	"github.com/rs/xid"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type JobBuilder struct {
	config    *rest.Config
	namespace string
	image     string
	command   []string
}

func NewJobBuilder(config *rest.Config, namespace string) *JobBuilder {
	return &JobBuilder{
		config:    config,
		namespace: namespace,
	}
}

func (b *JobBuilder) labelID() string {
	return xid.New().String()
}

func (b *JobBuilder) SetImage(image string) *JobBuilder {
	b.image = image
	return b
}

func (b *JobBuilder) SetCommand(cmd []string) *JobBuilder {
	b.command = cmd
	return b
}

func (b *JobBuilder) Build() (*Job, error) {
	if b.image == "" {
		return nil, errRequiredParam("container.image")
	}
	if len(b.command) == 0 {
		return nil, errRequiredParam("container.command")
	}
	return b.BuildWithJob(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: DefaultJobName,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    DefaultContainerName,
							Image:   b.image,
							Command: b.command,
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
			BackoffLimit: new(int32),
		},
	})
}

func (b *JobBuilder) BuildWithReader(r io.Reader) (*Job, error) {
	var jobSpec batchv1.Job
	if err := yaml.NewYAMLOrJSONDecoder(r, 1024).Decode(&jobSpec); err != nil {
		return nil, errInvalidYAML(err)
	}
	return b.BuildWithJob(&jobSpec)
}

func (b *JobBuilder) BuildWithJob(jobSpec *batchv1.Job) (*Job, error) {
	clientset, err := kubernetes.NewForConfig(b.config)
	if err != nil {
		return nil, fmt.Errorf("job: failed to create clientset: %w", err)
	}
	jobClient := clientset.BatchV1().Jobs(b.namespace)
	podClient := clientset.CoreV1().Pods(b.namespace)
	restClient := clientset.CoreV1().RESTClient()
	if jobSpec.ObjectMeta.Name == "" && jobSpec.ObjectMeta.GenerateName == "" {
		return nil, errRequiredParam("job.name")
	}
	if jobSpec.Spec.Template.Spec.RestartPolicy == "" {
		jobSpec.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
	}
	if jobSpec.Spec.BackoffLimit == nil {
		jobSpec.Spec.BackoffLimit = new(int32)
	}
	for idx := range jobSpec.Spec.Template.Spec.Containers {
		if jobSpec.Spec.Template.Spec.Containers[idx].Name == "" {
			return nil, errRequiredParam("container.name")
		}
		if len(jobSpec.Spec.Template.Spec.Containers[idx].Command) == 0 {
			return nil, errRequiredParam("container.command")
		}
		if jobSpec.Spec.Template.Spec.Containers[idx].Image == "" {
			return nil, errRequiredParam("container.image")
		}
	}
	if jobSpec.Spec.Template.Labels == nil {
		jobSpec.Spec.Template.Labels = map[string]string{}
	}
	jobSpec.Spec.Template.Labels[SelectorLabel] = b.labelID()
	propagationPolicy := metav1.DeletePropagationOrphan

	return &Job{
		Job:               jobSpec,
		jobClient:         jobClient,
		podClient:         podClient,
		restClient:        restClient,
		config:            b.config,
		propagationPolicy: &propagationPolicy,
	}, nil
}
