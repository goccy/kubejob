package kubejob

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	batchv1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

const (
	containerDummyName = "kubejob-container"
	jobDummyLabel      = "kubejob-dummy-label"
)

type JobBuilder struct {
	clientset     *kubernetes.Clientset
	namespace     string
	image         string
	containerName string
	command       []string
}

func NewJobBuilder(clientset *kubernetes.Clientset, namespace string) *JobBuilder {
	return &JobBuilder{
		clientset:     clientset,
		namespace:     namespace,
		containerName: containerDummyName,
	}
}

func (b *JobBuilder) SetImage(image string) *JobBuilder {
	b.image = image
	return b
}

func (b *JobBuilder) SetCommand(cmd []string) *JobBuilder {
	b.command = cmd
	return b
}

func (b *JobBuilder) SetContainerName(container string) *JobBuilder {
	b.containerName = container
	return b
}

func (b *JobBuilder) Build() (*Job, error) {
	if b.image == "" {
		return nil, xerrors.Errorf("could not find container.image name")
	}
	return b.BuildWithJob(&batch.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: "kubejob",
		},
		Spec: batch.JobSpec{
			Template: core.PodTemplateSpec{
				Spec: core.PodSpec{
					Containers: []core.Container{
						{
							Name:    b.containerName,
							Image:   b.image,
							Command: b.command,
						},
					},
					RestartPolicy: core.RestartPolicyNever,
				},
			},
		},
	})
}

func (b *JobBuilder) BuildWithReader(r io.Reader) (*Job, error) {
	var jobSpec batch.Job
	if err := yaml.NewYAMLOrJSONDecoder(r, 1024).Decode(&jobSpec); err != nil {
		return nil, xerrors.Errorf("failed to decode YAML: %w", err)
	}
	return b.BuildWithJob(&jobSpec)
}

func (b *JobBuilder) BuildWithJob(jobSpec *batch.Job) (*Job, error) {
	jobClient := b.clientset.BatchV1().Jobs(b.namespace)
	podClient := b.clientset.CoreV1().Pods(b.namespace)
	restClient := b.clientset.CoreV1().RESTClient()
	if jobSpec.ObjectMeta.Name == "" {
		jobSpec.ObjectMeta.Name = "kubejob"
	}
	if jobSpec.Spec.Template.Spec.RestartPolicy == "" {
		jobSpec.Spec.Template.Spec.RestartPolicy = core.RestartPolicyNever
	}
	for idx := range jobSpec.Spec.Template.Spec.Containers {
		if jobSpec.Spec.Template.Spec.Containers[idx].Name == "" {
			jobSpec.Spec.Template.Spec.Containers[idx].Name = b.containerName
		}
	}
	if len(jobSpec.Spec.Template.Labels) == 0 {
		jobSpec.Spec.Template.Labels = map[string]string{
			jobDummyLabel: jobDummyLabel,
		}
	}
	return &Job{
		jobClient:  jobClient,
		podClient:  podClient,
		restClient: restClient,
		jobSpec:    jobSpec,
	}, nil
}

type Job struct {
	jobClient  batchv1.JobInterface
	podClient  v1.PodInterface
	restClient rest.Interface
	jobSpec    *batch.Job
	podLogs    chan *podLog
	w          io.Writer
}

type podLog struct {
	pod       *core.Pod
	container string
	log       string
}

func (j *Job) SetWriter(w io.Writer) {
	j.w = w
}

func (j *Job) Run(ctx context.Context) (e error) {
	if _, err := j.jobClient.Create(j.jobSpec); err != nil {
		return xerrors.Errorf("failed to create job: %w", err)
	}
	defer func() {
		if err := j.jobClient.Delete(j.jobSpec.Name, nil); err != nil {
			e = xerrors.Errorf("failed to delete job: %w", err)
		}
	}()

	j.podLogs = make(chan *podLog)
	go func() {
		for podLog := range j.podLogs {
			w := j.w
			if w == nil {
				w = os.Stdout
			}
			fmt.Fprintf(w, "%s", podLog.log)
		}
	}()

	if err := j.wait(ctx); err != nil {
		return xerrors.Errorf("failed to wait: %w", err)
	}
	return nil
}

func (j *Job) wait(ctx context.Context) error {
	watcher, err := j.podClient.Watch(metav1.ListOptions{
		LabelSelector: j.labelSelector(),
		Watch:         true,
	})
	if err != nil {
		return xerrors.Errorf("failed to start watch pod: %w", err)
	}
	defer watcher.Stop()

	if err := j.watchLoop(ctx, watcher); err != nil {
		return xerrors.Errorf("failed to watching: %w", err)
	}
	return nil
}

func (j *Job) labelSelector() string {
	labels := j.jobSpec.Spec.Template.Labels
	keys := []string{}
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	sels := []string{}
	for _, k := range keys {
		sels = append(sels, fmt.Sprintf("%s=%s", k, labels[k]))
	}
	return strings.Join(sels, ",")
}

func (j *Job) watchLoop(ctx context.Context, watcher watch.Interface) (e error) {
	var (
		eg   errgroup.Group
		once sync.Once
		pod  *core.Pod
	)
	eg.Go(func() error {
		var phase core.PodPhase
		for event := range watcher.ResultChan() {
			pod = event.Object.(*core.Pod)
			if ctx.Err() != nil {
				return xerrors.Errorf("context error: %w", ctx.Err())
			}
			if pod.Status.Phase == phase {
				continue
			}
			switch pod.Status.Phase {
			case core.PodRunning:
				once.Do(func() {
					eg.Go(func() error {
						if err := j.logStreamPod(ctx, pod); err != nil {
							return xerrors.Errorf("failed to log stream pod: %w", err)
						}
						return nil
					})
				})
			case core.PodSucceeded, core.PodFailed:
				once.Do(func() {
					eg.Go(func() error {
						if err := j.logStreamPod(ctx, pod); err != nil {
							return xerrors.Errorf("failed to log stream pod: %w", err)
						}
						return nil
					})
				})
				return nil
			}
			phase = pod.Status.Phase
		}
		return nil
	})

	defer func() {
		if err := j.podClient.Delete(pod.Name, &metav1.DeleteOptions{}); err != nil {
			err = xerrors.Errorf("failed to delete pod: %w", err)
			if e == nil {
				e = err
			} else {
				e = xerrors.Errorf(strings.Join([]string{e.Error(), err.Error()}, "\n"))
			}
		}
	}()

	if err := eg.Wait(); err != nil {
		return xerrors.Errorf("failed to wait in watchLoop: %w", err)
	}
	return nil
}

func (j *Job) commandLog(pod *core.Pod, container core.Container) *podLog {
	cmd := []string{}
	cmd = append(cmd, container.Command...)
	cmd = append(cmd, container.Args...)
	return &podLog{
		pod:       pod,
		container: container.Name,
		log:       fmt.Sprintf("%s\n", strings.Join(cmd, " ")),
	}
}

func (j *Job) logStreamPod(ctx context.Context, pod *core.Pod) error {
	var eg errgroup.Group
	for _, container := range pod.Spec.InitContainers {
		j.podLogs <- j.commandLog(pod, container)
		if err := j.logStreamContainer(ctx, pod, container.Name); err != nil {
			return xerrors.Errorf("failed to log stream container: %w", err)
		}
	}
	for _, container := range pod.Spec.Containers {
		container := container
		j.podLogs <- j.commandLog(pod, container)
		eg.Go(func() error {
			if err := j.logStreamContainer(ctx, pod, container.Name); err != nil {
				return xerrors.Errorf("failed to log stream container: %w", err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return xerrors.Errorf("failed to wait: %w", err)
	}
	return nil
}

func (j *Job) logStreamContainer(ctx context.Context, pod *core.Pod, container string) error {
	stream, err := j.restClient.Get().
		Namespace(pod.Namespace).
		Resource("pods").
		Name(pod.Name).
		SubResource("log").
		VersionedParams(&core.PodLogOptions{
			Follow:    true,
			Container: container,
		}, scheme.ParameterCodec).Stream()
	if err != nil {
		return xerrors.Errorf("failed to create log stream: %w", err)
	}
	defer stream.Close()

	errchan := make(chan error, 1)

	go func() {
		reader := bufio.NewReader(stream)
		for {
			line, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				errchan <- err
			}
			if err == nil {
				j.podLogs <- &podLog{
					pod:       pod,
					container: container,
					log:       line,
				}
			}
			if err == io.EOF {
				errchan <- nil
			}
		}
	}()

	select {
	case <-ctx.Done():
		if err := ctx.Err(); err != nil {
			return xerrors.Errorf("context error: %w", err)
		}
		return nil
	case err := <-errchan:
		if err != nil {
			return xerrors.Errorf("failed to log stream: %w", err)
		}
		return nil
	}
	return nil
}
