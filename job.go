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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedbatchv1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	executil "k8s.io/client-go/util/exec"
)

const (
	SelectorLabel        = "kubejob.io/id"
	DefaultJobName       = "kubejob-"
	DefaultContainerName = "kubejob"
)

var (
	ExecRetryCount = 8
)

type FailedJob struct {
	Pod *corev1.Pod
}

func (j *FailedJob) errorContainerNamesFromStatuses(containerStatuses []corev1.ContainerStatus) []string {
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

func (j *FailedJob) FailedContainers() []corev1.Container {
	nameToContainerMap := map[string]corev1.Container{}
	for _, container := range j.Pod.Spec.InitContainers {
		nameToContainerMap[container.Name] = container
	}
	for _, container := range j.Pod.Spec.Containers {
		nameToContainerMap[container.Name] = container
	}
	containers := []corev1.Container{}
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
		return nil, xerrors.Errorf("container image must be specified")
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
		return nil, xerrors.Errorf("failed to decode YAML: %w", err)
	}
	return b.BuildWithJob(&jobSpec)
}

func (b *JobBuilder) BuildWithJob(jobSpec *batchv1.Job) (*Job, error) {
	clientset, err := kubernetes.NewForConfig(b.config)
	if err != nil {
		return nil, xerrors.Errorf("failed to create clientset: %w", err)
	}
	jobClient := clientset.BatchV1().Jobs(b.namespace)
	podClient := clientset.CoreV1().Pods(b.namespace)
	restClient := clientset.CoreV1().RESTClient()
	if jobSpec.ObjectMeta.Name == "" && jobSpec.ObjectMeta.GenerateName == "" {
		return nil, xerrors.Errorf("job name must be specified")
	}
	if jobSpec.Spec.Template.Spec.RestartPolicy == "" {
		jobSpec.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
	}
	if jobSpec.Spec.BackoffLimit == nil {
		jobSpec.Spec.BackoffLimit = new(int32)
	}
	for idx := range jobSpec.Spec.Template.Spec.Containers {
		if jobSpec.Spec.Template.Spec.Containers[idx].Name == "" {
			return nil, xerrors.Errorf("%s job container name is empty")
		}
		if jobSpec.Spec.Template.Spec.Containers[idx].Image == "" {
			return nil, xerrors.Errorf("%s job container image is empty")
		}
	}
	if jobSpec.Spec.Template.Labels == nil {
		jobSpec.Spec.Template.Labels = map[string]string{}
	}
	jobSpec.Spec.Template.Labels[SelectorLabel] = b.labelID()

	return &Job{
		Job:        jobSpec,
		jobClient:  jobClient,
		podClient:  podClient,
		restClient: restClient,
		config:     b.config,
	}, nil
}

type Job struct {
	*batchv1.Job
	jobClient                typedbatchv1.JobInterface
	podClient                typedcorev1.PodInterface
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
	podRunningCallback       func(*corev1.Pod) error
	preInit                  *preInit
}

type preInit struct {
	exec      *JobExecutor
	container corev1.Container
	callback  func(*JobExecutor) error
	done      bool
}

func (i *preInit) needsToRun(status corev1.PodStatus) bool {
	if i == nil {
		return false
	}
	if i.done {
		return false
	}
	if status.Phase != corev1.PodPending {
		return false
	}
	for _, status := range status.InitContainerStatuses {
		if status.Name != i.container.Name {
			continue
		}
		if status.State.Running != nil {
			return true
		}
	}
	return false
}

func (i *preInit) run(pod *corev1.Pod) error {
	if i.done {
		return nil
	}
	i.exec.Pod = pod
	logger := i.exec.job.containerLogger
	i.exec.job.containerLogger = func(log *ContainerLog) {
		if log.Container.Name == i.container.Name {
			return
		}
		if logger != nil {
			logger(log)
		}
	}
	if i.callback != nil {
		if err := i.callback(i.exec); err != nil {
			return xerrors.Errorf("failed to run preinit: %w", err)
		}
	}
	i.done = true
	if err := i.exec.Stop(); err != nil {
		return xerrors.Errorf("failed to stop preinit container: %w", err)
	}
	return nil
}

type ContainerLogger func(*ContainerLog)
type Logger func(string)

type ContainerLog struct {
	Pod        *corev1.Pod
	Container  corev1.Container
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
	Container   corev1.Container
	Pod         *corev1.Pod
	command     []string
	args        []string
	job         *Job
	isRunning   bool
	stopped     bool
	isRunningMu sync.Mutex
	err         error
}

// If a command like `sh -c "x; y; z" is passed as a cmd,
// there is no quote for `x; y; z`, so if you wrap the command with `sh -c`, it will occur unexpectedly behavior.
// To prevent that, executes using variables.
func (e *JobExecutor) normalizeCmd(cmd []string) string {
	const whiteSpace = " "

	normalizedCmd := make([]string, 0, len(cmd))
	vars := []string{}
	for idx, c := range cmd {
		c = strings.Trim(c, whiteSpace)
		if strings.Contains(c, whiteSpace) {
			// contains multiple command
			vars = append(vars, fmt.Sprintf("VAR%d=$(cat <<-EOS\n%s\nEOS\n)", idx, c))
			normalizedCmd = append(normalizedCmd, fmt.Sprintf(`"$VAR%d"`, idx))
		} else {
			normalizedCmd = append(normalizedCmd, c)
		}
	}
	cmdText := strings.Join(normalizedCmd, " ")
	if len(vars) == 0 {
		// simple command
		return cmdText
	}
	return fmt.Sprintf("%s; %s", strings.Join(vars, ";"), cmdText)
}

func (e *JobExecutor) exec(cmd []string) ([]byte, error) {
	pod := e.Pod
	req := e.job.restClient.Post().
		Namespace(pod.Namespace).
		Resource("pods").
		Name(pod.Name).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: e.Container.Name,
			Command:   []string{"sh", "-c", e.normalizeCmd(cmd)},
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
			e.job.logf(
				"[WARN] %s at %s. retry: %d/%d",
				err,
				e.Container.Name,
				retryCount,
				ExecRetryCount,
			)
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

func (e *JobExecutor) setIsRunning(isRunning bool) {
	e.isRunningMu.Lock()
	defer e.isRunningMu.Unlock()
	e.isRunning = isRunning
}

func (e *JobExecutor) IsRunning() bool {
	e.isRunningMu.Lock()
	defer e.isRunningMu.Unlock()
	return e.isRunning
}

func (e *JobExecutor) ExecPrepareCommand(cmd []string) ([]byte, error) {
	if e.IsRunning() {
		return nil, xerrors.Errorf("failed to exec: job is already running")
	}
	if !e.job.disabledCommandLog {
		fmt.Println(strings.Join(cmd, " "))
	}

	out, err := e.execWithRetry(cmd)
	if err != nil {
		return out, xerrors.Errorf("%s: %w", err.Error(), &FailedJob{Pod: e.Pod})
	}
	return out, nil
}

func (e *JobExecutor) ExecOnly() ([]byte, error) {
	if e.IsRunning() {
		return nil, xerrors.Errorf("failed to exec: job is already running")
	}
	if !e.job.disabledCommandLog {
		fmt.Println(strings.Join(append(e.command, e.args...), " "))
	}
	e.setIsRunning(true)
	out, err := e.execWithRetry(append(e.command, e.args...))
	e.err = err
	if err != nil {
		return out, xerrors.Errorf("%s: %w", err.Error(), &FailedJob{Pod: e.Pod})
	}
	return out, nil
}

func (e *JobExecutor) ExecAsync() error {
	if e.IsRunning() {
		return xerrors.Errorf("failed to exec: job is already running")
	}
	if !e.job.disabledCommandLog {
		fmt.Println(strings.Join(append(e.command, e.args...), " "))
	}
	e.setIsRunning(true)
	go func() {
		_, err := e.execWithRetry(append(e.command, e.args...))
		e.err = err
		if err := e.Stop(); err != nil {
			e.job.logf("[WARN] failed to stop async executor: %s", err)
		}
	}()
	return nil
}

func (e *JobExecutor) Stop() error {
	if e.stopped {
		return nil
	}
	defer func() {
		e.setIsRunning(false)
	}()
	var status int
	if e.err != nil {
		status = 1
	}
	if _, err := e.execWithRetry([]string{"echo", fmt.Sprint(status), ">", "/tmp/kubejob-status"}); err != nil {
		return xerrors.Errorf("failed to stop process: %w", err)
	}
	e.stopped = true
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

func (j *Job) jobTemplateCommandContainer(c corev1.Container) corev1.Container {
	copied := c.DeepCopy()
	copied.Command = []string{"sh", "-c"}
	copied.Args = []string{jobCommandTemplate}
	return *copied
}

// PreInit can define the process you want to execute before the process of init container specified at the time of starting Job.
// It is mainly intended to be used when you use `(*JobExecutor).CopyToFile` to copy an arbitrary file to pod before init processing.
func (j *Job) PreInit(c corev1.Container, cb func(exec *JobExecutor) error) {
	j.preInit = &preInit{
		container: j.jobTemplateCommandContainer(c),
		callback:  cb,
		exec: &JobExecutor{
			Container: c,
			command:   c.Command,
			args:      c.Args,
			job:       j,
		},
	}
}

func (j *Job) RunWithExecutionHandler(ctx context.Context, handler JobExecutionHandler) error {
	ctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- j.runWithExecutionHandler(ctx, cancel, handler)
	}()
	select {
	case <-ctx.Done():
		// stop runWithExecutionHandler safely.
		cancel()
		return <-errCh
	case err := <-errCh:
		return err
	}
	return nil
}

func (j *Job) runWithExecutionHandler(ctx context.Context, cancelFn func(), handler JobExecutionHandler) error {
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
	var callbackPod *corev1.Pod
	j.podRunningCallback = func(pod *corev1.Pod) error {
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
		defer func() {
			for _, executor := range executors {
				if executor.err != nil {
					existsErrContainer = true
				}
				if executor.IsRunning() {
					if err := executor.Stop(); err != nil {
						log.Printf("failed to stop: %+v", err)
						forceStop = true
					}
				}
			}
			if forceStop {
				cancelFn()
			}
		}()
		if err := handler(executors); err != nil {
			return xerrors.Errorf("failed to handle executors: %w", err)
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
		j.logf("delete pod: %s job-id: %s", pod.Name, pod.Labels[SelectorLabel])
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
	if j.preInit != nil {
		initContainers := j.Job.Spec.Template.Spec.InitContainers
		j.Job.Spec.Template.Spec.InitContainers = append([]corev1.Container{j.preInit.container}, initContainers...)
	}
	job, err := j.jobClient.Create(ctx, j.Job, metav1.CreateOptions{})
	if err != nil {
		return xerrors.Errorf("failed to create job: %w", err)
	}
	j.Name = job.Name
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
	return fmt.Sprintf("%s=%s", SelectorLabel, j.Spec.Template.Labels[SelectorLabel])
}

func (j *Job) watchLoop(ctx context.Context, watcher watch.Interface) (e error) {
	var (
		eg   errgroup.Group
		once sync.Once
	)
	eg.Go(func() error {
		var phase corev1.PodPhase
		for event := range watcher.ResultChan() {
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				// if event.Object will be not corev1.Pod, we expect that it was executed cancel to the context.Context.
				// In this case, we should stop watch loop, so return instantly.
				return nil
			}
			if ctx.Err() != nil && pod.Status.Phase != corev1.PodPending {
				return nil
			}
			if j.preInit.needsToRun(pod.Status) {
				if err := j.preInit.run(pod); err != nil {
					return xerrors.Errorf("failed to run preinit: %w", err)
				}
			}
			if pod.Status.Phase == phase {
				continue
			}
			switch pod.Status.Phase {
			case corev1.PodRunning:
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
			case corev1.PodSucceeded, corev1.PodFailed:
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
				if pod.Status.Phase == corev1.PodFailed {
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

func (j *Job) commandLog(pod *corev1.Pod, container corev1.Container) *ContainerLog {
	cmd := []string{}
	cmd = append(cmd, container.Command...)
	cmd = append(cmd, container.Args...)
	return &ContainerLog{
		Pod:       pod,
		Container: container,
		Log:       fmt.Sprintf("%s\n", strings.Join(cmd, " ")),
	}
}

func (j *Job) logStreamInitContainers(ctx context.Context, pod *corev1.Pod) error {
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

func (j *Job) logStreamPod(ctx context.Context, pod *corev1.Pod) error {
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

func (j *Job) logStreamContainer(ctx context.Context, pod *corev1.Pod, container corev1.Container, enabledCommandLog, enabledLog bool) error {
	stream, err := j.restClient.Get().
		Namespace(pod.Namespace).
		Resource("pods").
		Name(pod.Name).
		SubResource("log").
		VersionedParams(&corev1.PodLogOptions{
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
