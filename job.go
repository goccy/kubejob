package kubejob

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/lestrrat-go/backoff"
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
	"k8s.io/client-go/tools/remotecommand"
	executil "k8s.io/client-go/util/exec"
)

const (
	KubejobLabel = "kubejob.io/id"
)

var (
	ExecRetryCount = 8
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

func (b *JobBuilder) jobName() string {
	return b.generateName("kubejob")
}

func (b *JobBuilder) containerName() string {
	return b.generateName("kubejob-container")
}

func (b *JobBuilder) labelID() string {
	return xid.New().String()
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
			BackoffLimit: new(int32),
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
	clientset, err := kubernetes.NewForConfig(b.config)
	if err != nil {
		return nil, xerrors.Errorf("failed to create clientset: %w", err)
	}
	jobClient := clientset.BatchV1().Jobs(b.namespace)
	podClient := clientset.CoreV1().Pods(b.namespace)
	restClient := clientset.CoreV1().RESTClient()
	if jobSpec.ObjectMeta.Name == "" {
		jobSpec.ObjectMeta.Name = b.jobName()
	}
	if jobSpec.Spec.Template.Spec.RestartPolicy == "" {
		jobSpec.Spec.Template.Spec.RestartPolicy = core.RestartPolicyNever
	}
	if jobSpec.Spec.BackoffLimit == nil {
		jobSpec.Spec.BackoffLimit = new(int32)
	}
	for idx := range jobSpec.Spec.Template.Spec.Containers {
		if jobSpec.Spec.Template.Spec.Containers[idx].Name == "" {
			jobSpec.Spec.Template.Spec.Containers[idx].Name = b.containerName()
		}
	}
	if jobSpec.Spec.Template.Labels == nil {
		jobSpec.Spec.Template.Labels = map[string]string{}
	}
	jobSpec.Spec.Template.Labels[KubejobLabel] = b.labelID()

	return &Job{
		Job:        jobSpec,
		jobClient:  jobClient,
		podClient:  podClient,
		restClient: restClient,
		config:     b.config,
	}, nil
}

type Job struct {
	*batch.Job
	jobClient                batchv1.JobInterface
	podClient                v1.PodInterface
	restClient               rest.Interface
	containerLogs            chan *ContainerLog
	logger                   Logger
	containerLogger          ContainerLogger
	disabledInitContainerLog bool
	disabledInitCommandLog   bool
	disabledContainerLog     bool
	disabledCommandLog       bool
	verboseLog               bool
	config                   *rest.Config
	podRunningCallback       func(*core.Pod) error
}

type ContainerLogger func(*ContainerLog)
type Logger func(string)

type ContainerLog struct {
	Pod        *core.Pod
	Container  core.Container
	Log        string
	IsFinished bool
}

func (j *Job) SetVerboseLog(verboseLog bool) {
	j.verboseLog = verboseLog
}

func (j *Job) SetContainerLogger(logger ContainerLogger) {
	j.containerLogger = logger
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

type JobExecutor struct {
	Container core.Container
	Pod       *core.Pod
	command   []string
	args      []string
	job       *Job
	isRunning bool
	err       error
}

func (e *JobExecutor) exec(cmd []string) ([]byte, error) {
	pod := e.Pod
	req := e.job.restClient.Post().
		Namespace(pod.Namespace).
		Resource("pods").
		Name(pod.Name).
		SubResource("exec").
		VersionedParams(&core.PodExecOptions{
			Container: e.Container.Name,
			Command:   []string{"sh", "-c", strings.Join(cmd, " ")},
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	url := req.URL()
	exec, err := remotecommand.NewSPDYExecutor(e.job.config, "POST", url)
	if err != nil {
		return nil, xerrors.Errorf("failed to create spdy executor: %w", err)
	}
	r, w := io.Pipe()
	var streamErr error
	go func() {
		streamErr = exec.Stream(remotecommand.StreamOptions{
			Stdin:  nil,
			Stdout: w,
			Stderr: w,
			Tty:    false,
		})
		w.Close()
	}()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r); err != nil {
		if streamErr != nil {
			return buf.Bytes(), xerrors.Errorf("%s. failed to read buffer: %w", streamErr, err)
		}
		return buf.Bytes(), xerrors.Errorf("failed to read buffer: %w", err)
	}
	return buf.Bytes(), streamErr
}

func (e *JobExecutor) execWithRetry(cmd []string) ([]byte, error) {
	var (
		out []byte
		err error
	)
	policy := backoff.NewExponential(
		backoff.WithInterval(1*time.Second),
		backoff.WithMaxRetries(ExecRetryCount),
	)
	b, cancel := policy.Start(context.Background())
	defer cancel()

	retryCount := 0
	for backoff.Continue(b) {
		out, err = e.exec(cmd)
		if err != nil {
			if _, ok := err.(executil.ExitError); ok {
				break
			}
			// cannot connect to Pod. retry....
			e.job.logf("[WARN] %s. retry: %d/%d", err, retryCount, ExecRetryCount)
			retryCount++
			continue
		}
		break
	}
	return out, err
}

func (e *JobExecutor) Exec() ([]byte, error) {
	defer func() {
		if err := e.Stop(); err != nil {
			e.job.logf("[WARN]: %s", err)
		}
	}()
	return e.ExecOnly()
}

func (e *JobExecutor) ExecOnly() ([]byte, error) {
	if e.isRunning {
		return nil, xerrors.Errorf("failed to exec: job is already running")
	}
	if !e.job.disabledCommandLog {
		fmt.Println(strings.Join(append(e.command, e.args...), " "))
	}
	e.isRunning = true
	out, err := e.execWithRetry(append(e.command, e.args...))
	e.isRunning = false
	e.err = err
	if err != nil {
		return out, xerrors.Errorf("%s: %w", err.Error(), &FailedJob{Pod: e.Pod})
	}
	return out, nil
}

func (e *JobExecutor) ExecAsync() error {
	if e.isRunning {
		return xerrors.Errorf("failed to exec: job is already running")
	}
	if !e.job.disabledCommandLog {
		fmt.Println(strings.Join(append(e.command, e.args...), " "))
	}
	e.isRunning = true
	go func() {
		_, err := e.execWithRetry(append(e.command, e.args...))
		e.isRunning = false
		e.err = err
		if err := e.Stop(); err != nil {
			e.job.logf("[WARN] failed to stop async executor: %s", err)
		}
	}()
	return nil
}

func (e *JobExecutor) Stop() error {
	var status int
	if e.err != nil {
		status = 1
	}
	if _, err := e.execWithRetry([]string{"echo", fmt.Sprint(status), ">", "/tmp/kubejob-status"}); err != nil {
		return xerrors.Errorf("failed to stop process: %w", err)
	}
	return nil
}

type JobExecutionHandler func([]*JobExecutor) error

const jobCommandTemplate = `
while [ ! -f /tmp/kubejob-status ]
do
    sleep 1;
done

exit $(cat /tmp/kubejob-status)
`

func (j *Job) RunWithExecutionHandler(ctx context.Context, handler JobExecutionHandler) error {
	executorMap := map[string]*JobExecutor{}
	for idx := range j.Job.Spec.Template.Spec.Containers {
		container := j.Job.Spec.Template.Spec.Containers[idx]
		command := container.Command
		args := container.Args
		executorMap[container.Name] = &JobExecutor{
			Container: container,
			command:   command,
			args:      args,
			job:       j,
		}
		j.Job.Spec.Template.Spec.Containers[idx].Command = []string{"sh"}
		j.Job.Spec.Template.Spec.Containers[idx].Args = []string{"-c", jobCommandTemplate}
	}
	j.DisableCommandLog()
	existsErrContainer := false
	var callbackPod *core.Pod
	ctx, cancel := context.WithCancel(ctx)
	j.podRunningCallback = func(pod *core.Pod) error {
		callbackPod = pod
		forceStop := false
		executors := []*JobExecutor{}
		for _, container := range pod.Spec.Containers {
			if executor, exists := executorMap[container.Name]; exists {
				executor.Pod = pod
				executors = append(executors, executor)
			} else {
				// found injected container.
				// Since kubejob cannot handle termination of this container, use forceStop logic
				forceStop = true
			}
		}
		if err := handler(executors); err != nil {
			return xerrors.Errorf("failed to handle executors: %w", err)
		}
		for _, executor := range executors {
			if executor.err != nil {
				existsErrContainer = true
			}
			if executor.isRunning {
				if err := executor.Stop(); err != nil {
					log.Printf("failed to stop: %+v", err)
					forceStop = true
				}
			}
		}
		if forceStop {
			cancel()
		}
		return nil
	}
	if err := j.Run(ctx); err != nil {
		return xerrors.Errorf("failed to run job: %w", err)
	}

	// if call cancel() to stop all containers, return `nil` error from Run() loop.
	// So, existsErrContainer check whether exists stopped container with failed status.
	if existsErrContainer {
		return &FailedJob{Pod: callbackPod}
	}
	return nil
}

func (j *Job) cleanup(ctx context.Context) error {
	j.logf("cleanup job %s", j.Name)
	multierr := []string{}
	if err := j.jobClient.Delete(ctx, j.Name, metav1.DeleteOptions{
		GracePeriodSeconds: new(int64), // assign zero value as GracePeriodSeconds to delete immediately.
	}); err != nil {
		multierr = append(multierr, xerrors.Errorf("failed to delete job %s: %w", j.Name, err).Error())
	}
	j.logf("search by %s", j.labelSelector())
	podList, err := j.podClient.List(ctx, metav1.ListOptions{
		LabelSelector: j.labelSelector(),
	})
	if err != nil {
		multierr = append(multierr, xerrors.Errorf("failed to list pod: %w", err).Error())
	}
	if podList == nil || len(podList.Items) == 0 {
		j.logf("[WARN] could not find pod to remove")
		if len(multierr) > 0 {
			return xerrors.Errorf(strings.Join(multierr, ":"))
		}
		return nil
	}
	j.logf("%d pods found", len(podList.Items))
	for _, pod := range podList.Items {
		j.logf("delete pod: %s job-id: %s", pod.Name, pod.Labels[KubejobLabel])
		if err := j.podClient.Delete(ctx, pod.Name, metav1.DeleteOptions{
			GracePeriodSeconds: new(int64), // assign zero value as GracePeriodSeconds to delete immediately.
		}); err != nil {
			multierr = append(multierr, xerrors.Errorf("failed to delete pod %s: %w", pod.Name, err).Error())
		}
	}
	if len(multierr) > 0 {
		return xerrors.Errorf(strings.Join(multierr, ":"))
	}
	return nil
}

func (j *Job) Run(ctx context.Context) (e error) {
	if _, err := j.jobClient.Create(ctx, j.Job, metav1.CreateOptions{}); err != nil {
		return xerrors.Errorf("failed to create job: %w", err)
	}
	defer func() {
		// we wouldn't like to cancel cleanup process by cancelled context,
		// so create new context and use it.
		if err := j.cleanup(context.Background()); err != nil {
			if e == nil {
				e = err
			} else {
				e = xerrors.Errorf(strings.Join([]string{e.Error(), err.Error()}, ":"))
			}
		}
	}()

	j.containerLogs = make(chan *ContainerLog)
	go func() {
		for containerLog := range j.containerLogs {
			j.containerLog(containerLog)
		}
	}()

	if err := j.wait(ctx); err != nil {
		return xerrors.Errorf("failed to wait: %w", err)
	}

	return nil
}

func (j *Job) containerLog(log *ContainerLog) {
	if j.containerLogger != nil {
		j.containerLogger(log)
	} else if !log.IsFinished {
		fmt.Fprintf(os.Stderr, "%s", log.Log)
	}
}

func (j *Job) logf(format string, args ...interface{}) {
	if !j.verboseLog {
		return
	}
	if format == "" {
		return
	}
	log := fmt.Sprintf(format, args...)
	if j.logger != nil {
		j.logger(log)
	} else {
		fmt.Fprintf(os.Stderr, "%s\n", log)
	}
}

func (j *Job) wait(ctx context.Context) error {
	watcher, err := j.podClient.Watch(ctx, metav1.ListOptions{
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
	labels := j.Spec.Template.Labels
	value := labels[KubejobLabel]
	return fmt.Sprintf("%s=%s", KubejobLabel, value)
}

func (j *Job) watchLoop(ctx context.Context, watcher watch.Interface) (e error) {
	var (
		eg   errgroup.Group
		once sync.Once
	)
	eg.Go(func() error {
		var phase core.PodPhase
		for event := range watcher.ResultChan() {
			pod, ok := event.Object.(*core.Pod)
			if !ok {
				// if event.Object will be not core.Pod, we expect that it was executed cancel to the context.Context.
				// In this case, we should stop watch loop, so return instantly.
				return nil
			}
			if ctx.Err() != nil && pod.Status.Phase == core.PodRunning {
				return nil
			}
			if pod.Status.Phase == phase {
				continue
			}
			switch pod.Status.Phase {
			case core.PodRunning:
				once.Do(func() {
					eg.Go(func() error {
						if err := j.logStreamInitContainers(ctx, pod); err != nil {
							return xerrors.Errorf("failed to log stream init container: %w", err)
						}
						if j.podRunningCallback != nil {
							if err := j.podRunningCallback(pod); err != nil {
								return xerrors.Errorf("failed to callback for pod running: %w", err)
							}
						} else {
							if err := j.logStreamPod(ctx, pod); err != nil {
								return xerrors.Errorf("failed to log stream pod: %w", err)
							}
						}
						return nil
					})
				})
			case core.PodSucceeded, core.PodFailed:
				once.Do(func() {
					eg.Go(func() error {
						if err := j.logStreamInitContainers(ctx, pod); err != nil {
							return xerrors.Errorf("failed to log stream init container: %w", err)
						}
						if j.podRunningCallback == nil {
							if err := j.logStreamPod(ctx, pod); err != nil {
								return xerrors.Errorf("failed to log stream pod: %w", err)
							}
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

func (j *Job) logStreamInitContainers(ctx context.Context, pod *core.Pod) error {
	for _, container := range pod.Spec.InitContainers {
		enabledLog := !j.disabledInitContainerLog
		if err := j.logStreamContainer(
			ctx,
			pod,
			container,
			j.enabledInitCommandLog(),
			enabledLog,
		); err != nil {
			return xerrors.Errorf("failed to log stream: %w", err)
		}
	}
	return nil
}

func (j *Job) logStreamPod(ctx context.Context, pod *core.Pod) error {
	var eg errgroup.Group
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
		}, scheme.ParameterCodec).Stream(ctx)
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
		return nil
	case err := <-errchan:
		if err != nil {
			return xerrors.Errorf("failed to log stream: %w", err)
		}
		return nil
	}
	return nil
}
