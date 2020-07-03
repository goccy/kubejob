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

	"github.com/rs/xid"
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

type FailedJob struct {
	Pod *core.Pod
}

func (j *FailedJob) errorContainerNamesFromStatuses(containerStatuses []core.ContainerStatus) []string {
	containerNames := []string{}
	for _, status := range containerStatuses {
		terminated := status.State.Terminated
		if terminated == nil {
			continue
		}
		if terminated.Reason == "Error" {
			containerNames = append(containerNames, status.Name)
		}
	}
	return containerNames
}

func (j *FailedJob) FailedContainerNames() []string {
	containerNames := []string{}
	containerNames = append(containerNames,
		j.errorContainerNamesFromStatuses(j.Pod.Status.InitContainerStatuses)...,
	)
	containerNames = append(containerNames,
		j.errorContainerNamesFromStatuses(j.Pod.Status.ContainerStatuses)...,
	)
	return containerNames
}

func (j *FailedJob) FailedContainers() []core.Container {
	nameToContainerMap := map[string]core.Container{}
	for _, container := range j.Pod.Spec.InitContainers {
		nameToContainerMap[container.Name] = container
	}
	for _, container := range j.Pod.Spec.Containers {
		nameToContainerMap[container.Name] = container
	}
	containers := []core.Container{}
	for _, name := range j.FailedContainerNames() {
		containers = append(containers, nameToContainerMap[name])
	}
	return containers
}

func (j *FailedJob) Error() string {
	return "failed to job"
}

type JobBuilder struct {
	clientset *kubernetes.Clientset
	namespace string
	image     string
	command   []string
}

func NewJobBuilder(clientset *kubernetes.Clientset, namespace string) *JobBuilder {
	return &JobBuilder{
		clientset: clientset,
		namespace: namespace,
	}
}

func (b *JobBuilder) jobName() string {
	return b.generateName("kubejob")
}

func (b *JobBuilder) containerName() string {
	return b.generateName("kubejob-container")
}

func (b *JobBuilder) labelName() string {
	return b.generateName("kubejob-label")
}

func (b *JobBuilder) generateName(name string) string {
	return fmt.Sprintf("%s-%s", name, xid.New())
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
		return nil, xerrors.Errorf("could not find container.image name")
	}
	return b.BuildWithJob(&batch.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: b.jobName(),
		},
		Spec: batch.JobSpec{
			Template: core.PodTemplateSpec{
				Spec: core.PodSpec{
					Containers: []core.Container{
						{
							Name:    b.containerName(),
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
		jobSpec.ObjectMeta.Name = b.jobName()
	}
	if jobSpec.Spec.Template.Spec.RestartPolicy == "" {
		jobSpec.Spec.Template.Spec.RestartPolicy = core.RestartPolicyNever
	}
	for idx := range jobSpec.Spec.Template.Spec.Containers {
		if jobSpec.Spec.Template.Spec.Containers[idx].Name == "" {
			jobSpec.Spec.Template.Spec.Containers[idx].Name = b.containerName()
		}
	}
	labelName := b.labelName()
	if jobSpec.Spec.Template.Labels == nil {
		jobSpec.Spec.Template.Labels = map[string]string{}
	}
	jobSpec.Spec.Template.Labels[labelName] = labelName
	return &Job{
		jobClient:  jobClient,
		podClient:  podClient,
		restClient: restClient,
		jobSpec:    jobSpec,
	}, nil
}

type Job struct {
	jobClient                batchv1.JobInterface
	podClient                v1.PodInterface
	restClient               rest.Interface
	jobSpec                  *batch.Job
	containerLogs            chan *ContainerLog
	logger                   Logger
	disabledInitContainerLog bool
	disabledInitCommandLog   bool
	disabledContainerLog     bool
	disabledCommandLog       bool
}

type Logger func(*ContainerLog)

type ContainerLog struct {
	Pod        *core.Pod
	Container  core.Container
	Log        string
	IsFinished bool
}

func (j *Job) SetLogger(logger Logger) {
	j.logger = logger
}

func (j *Job) DisableInitContainerLog() {
	j.disabledInitContainerLog = true
}

func (j *Job) DisableInitCommandLog() {
	j.disabledInitCommandLog = true
}

func (j *Job) DisableContainerLog() {
	j.disabledContainerLog = true
}

func (j *Job) DisableCommandLog() {
	j.disabledCommandLog = true
}

func (j *Job) Run(ctx context.Context) (e error) {
	if _, err := j.jobClient.Create(j.jobSpec); err != nil {
		return xerrors.Errorf("failed to create job: %w", err)
	}
	defer func() {
		if err := j.jobClient.Delete(j.jobSpec.Name, nil); err != nil {
			e = xerrors.Errorf("failed to delete job: %w", err)
		}
		podList, _ := j.podClient.List(metav1.ListOptions{
			LabelSelector: j.labelSelector(),
		})
		if podList == nil {
			return
		}
		if len(podList.Items) == 0 {
			return
		}
		for _, pod := range podList.Items {
			if err := j.podClient.Delete(pod.Name, &metav1.DeleteOptions{}); err != nil {
				err = xerrors.Errorf("failed to delete pod: %w", err)
				if e == nil {
					e = err
				} else {
					e = xerrors.Errorf(strings.Join([]string{e.Error(), err.Error()}, "\n"))
				}
			}
		}
	}()

	j.containerLogs = make(chan *ContainerLog)
	go func() {
		for containerLog := range j.containerLogs {
			if j.logger != nil {
				j.logger(containerLog)
			} else if !containerLog.IsFinished {
				fmt.Fprintf(os.Stderr, "%s", containerLog.Log)
			}
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
				if pod.Status.Phase == core.PodFailed {
					return &FailedJob{Pod: pod}
				}
				return nil
			}
			phase = pod.Status.Phase
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return xerrors.Errorf("failed to wait in watchLoop: %w", err)
	}
	return nil
}

func (j *Job) enabledInitCommandLog() bool {
	if j.disabledInitContainerLog {
		return false
	}
	if j.disabledInitCommandLog {
		return false
	}
	return true
}

func (j *Job) enabledCommandLog() bool {
	if j.disabledContainerLog {
		return false
	}
	if j.disabledCommandLog {
		return false
	}
	return true
}

func (j *Job) commandLog(pod *core.Pod, container core.Container) *ContainerLog {
	cmd := []string{}
	cmd = append(cmd, container.Command...)
	cmd = append(cmd, container.Args...)
	return &ContainerLog{
		Pod:       pod,
		Container: container,
		Log:       fmt.Sprintf("%s\n", strings.Join(cmd, " ")),
	}
}

func (j *Job) logStreamPod(ctx context.Context, pod *core.Pod) error {
	var eg errgroup.Group
	for _, container := range pod.Spec.InitContainers {
		enabledLog := !j.disabledInitContainerLog
		if err := j.logStreamContainer(
			ctx,
			pod,
			container,
			j.enabledInitCommandLog(),
			enabledLog,
		); err != nil {
			return xerrors.Errorf("failed to log stream container: %w", err)
		}
	}
	for _, container := range pod.Spec.Containers {
		container := container
		eg.Go(func() error {
			enabledLog := !j.disabledContainerLog
			if err := j.logStreamContainer(
				ctx,
				pod,
				container,
				j.enabledCommandLog(),
				enabledLog,
			); err != nil {
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

func (j *Job) logStreamContainer(ctx context.Context, pod *core.Pod, container core.Container, enabledCommandLog, enabledLog bool) error {
	stream, err := j.restClient.Get().
		Namespace(pod.Namespace).
		Resource("pods").
		Name(pod.Name).
		SubResource("log").
		VersionedParams(&core.PodLogOptions{
			Follow:    true,
			Container: container.Name,
		}, scheme.ParameterCodec).Stream()
	if err != nil {
		return xerrors.Errorf("failed to create log stream: %w", err)
	}
	defer stream.Close()

	if enabledCommandLog {
		j.containerLogs <- j.commandLog(pod, container)
	}

	errchan := make(chan error, 1)

	go func() {
		reader := bufio.NewReader(stream)
		for {
			line, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				errchan <- err
			}
			if err == nil {
				if enabledLog {
					j.containerLogs <- &ContainerLog{
						Pod:       pod,
						Container: container,
						Log:       line,
					}
				}
			}
			if err == io.EOF {
				j.containerLogs <- &ContainerLog{
					Pod:        pod,
					Container:  container,
					Log:        "",
					IsFinished: true,
				}
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
