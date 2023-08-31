package kubejob

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	typedbatchv1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

const (
	SelectorLabel        = "kubejob.io/id"
	DefaultJobName       = "kubejob-"
	DefaultContainerName = "kubejob"
)

type LogLevel int

const (
	LogLevelNone LogLevel = iota
	LogLevelError
	LogLevelWarn
	LogLevelInfo
	LogLevelDebug
)

func (l LogLevel) String() string {
	switch l {
	case LogLevelNone:
		return "none"
	case LogLevelWarn:
		return "warn"
	case LogLevelInfo:
		return "info"
	case LogLevelDebug:
		return "debug"
	}
	return ""
}

var (
	ExecRetryCount = 8
)

type Job struct {
	*batchv1.Job
	jobClient                typedbatchv1.JobInterface
	podClient                typedcorev1.PodInterface
	restClient               rest.Interface
	containerLogs            chan *ContainerLog
	logger                   Logger
	containerLogger          ContainerLogger
	enableJobDeletion        bool
	disabledInitContainerLog bool
	disabledInitCommandLog   bool
	disabledContainerLog     bool
	disabledCommandLog       bool
	logLevel                 LogLevel
	config                   *rest.Config
	podRunningCallback       func(*corev1.Pod) error
	preInit                  *preInit
	jobInit                  *jobInit
	pendingTimeout           *time.Duration
	watchTimeoutSecond       *int64
	agentCfg                 *AgentConfig
	propagationPolicy        *metav1.DeletionPropagation
}

type ContainerLogger func(*ContainerLog)
type Logger func(string)

type ContainerLog struct {
	Pod        *corev1.Pod
	Container  corev1.Container
	Log        string
	IsFinished bool
}

// SetPendingPhaseTimeout set the timeout when the process in the init container is finished
// but it does not switch to the Running phase ( PodInitializing state for a long time ).
func (j *Job) SetPendingPhaseTimeout(timeout time.Duration) {
	j.pendingTimeout = &timeout
}

func (j *Job) SetLogLevel(level LogLevel) {
	j.logLevel = level
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
func (j *Job) SetWatchTimeoutSecond(watchTimeoutSecond int64) {
	j.watchTimeoutSecond = &watchTimeoutSecond
}

func (j *Job) SetDeletePropagationPolicy(propagation metav1.DeletionPropagation) {
	j.enableJobDeletion = true
	j.propagationPolicy = &propagation
}

func (j *Job) cleanup(ctx context.Context) error {
	j.logDebug("cleanup job %s", j.Name)
	if !j.enableJobDeletion {
		return nil
	}
	if err := j.jobClient.Delete(ctx, j.Name, metav1.DeleteOptions{
		GracePeriodSeconds: new(int64), // assign zero value as GracePeriodSeconds to delete immediately.
		PropagationPolicy:  j.propagationPolicy,
	}); err != nil {
		return err
	}
	return nil
}

func (j *Job) Run(ctx context.Context) (e error) {
	if j.jobInit != nil {
		if err := j.setupInitContainers(); err != nil {
			return err
		}
		j.Job.Spec.Template.Spec.InitContainers = j.jobInit.containers
		if j.preInit != nil {
			// ignore preinit container
			j.jobInit.executedContainerNameMap[j.preInit.container.Name] = struct{}{}
		}
	}
	if j.preInit != nil {
		if err := j.setupPreInitContainer(); err != nil {
			return err
		}
		initContainers := j.Job.Spec.Template.Spec.InitContainers
		j.Job.Spec.Template.Spec.InitContainers = append([]corev1.Container{j.preInit.container}, initContainers...)
	}
	job, err := j.jobClient.Create(ctx, j.Job, metav1.CreateOptions{})
	if err != nil {
		return errJobCreation(j.Name, j.GenerateName, err)
	}
	j.Name = job.Name
	defer func() {
		// we wouldn't like to cancel cleanup process by cancelled context,
		// so create new context and use it.
		if err := j.cleanup(context.Background()); err != nil {
			if e == nil {
				e = err
			} else {
				e = errMulti(e, err)
			}
		}
	}()

	j.containerLogs = make(chan *ContainerLog)
	go func() {
		for containerLog := range j.containerLogs {
			j.containerLog(containerLog)
		}
	}()

	errCh := make(chan error)
	go func() {
		errCh <- j.wait(ctx)
	}()
	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
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

func (j *Job) logWarn(format string, args ...interface{}) {
	if j.logLevel < LogLevelWarn {
		return
	}
	j.logf(fmt.Sprintf("[WARN] %s", format), args...)
}

func (j *Job) logDebug(format string, args ...interface{}) {
	if j.logLevel < LogLevelDebug {
		return
	}
	j.logf(fmt.Sprintf("[DEBUG] %s", format), args...)
}

func (j *Job) logf(format string, args ...interface{}) {
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

func (j *Job) getPod(ctx context.Context, name string) (*corev1.Pod, error) {
	pod, err := j.podClient.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("job: failed to get pod by name %s: %w", name, err)
	}
	return pod, nil
}

func (j *Job) wait(ctx context.Context) error {
	listOptions := metav1.ListOptions{
		LabelSelector: j.labelSelector(),
		Watch:         true,
	}
	if j.watchTimeoutSecond != nil {
		listOptions.TimeoutSeconds = j.watchTimeoutSecond
	}
	watcher, err := j.podClient.Watch(ctx, metav1.ListOptions{
		LabelSelector: j.labelSelector(),
		Watch:         true,
	})
	if err != nil {
		return errJobWatch(j.Name, err)
	}
	defer watcher.Stop()

	if err := j.watchLoop(ctx, watcher); err != nil {
		return err
	}
	return nil
}

func (j *Job) labelSelector() string {
	return fmt.Sprintf("%s=%s", SelectorLabel, j.Spec.Template.Labels[SelectorLabel])
}

func (j *Job) isPodInitializing(pod *corev1.Pod) bool {
	const waitingReasonPodInitializing = "PodInitializing"

	if pod.Status.Phase != corev1.PodPending {
		return false
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Waiting != nil {
			if status.State.Waiting.Reason == waitingReasonPodInitializing {
				return true
			}
		}
	}
	return false
}

func (j *Job) watchPodPendingPhase(ctx context.Context, name string) error {
	if j.pendingTimeout == nil {
		return nil
	}

	var (
		onceForPodInitializing sync.Once
		startedPodInitializing time.Time
	)
	for {
		time.Sleep(1 * time.Second)
		curPod, err := j.getPod(ctx, name)
		if err != nil {
			return err
		}
		if curPod == nil {
			continue
		}
		switch curPod.Status.Phase {
		case corev1.PodRunning, corev1.PodSucceeded, corev1.PodFailed:
			return nil
		}
		if j.isPodInitializing(curPod) {
			onceForPodInitializing.Do(func() {
				startedPodInitializing = time.Now()
			})
			elapsedTime := time.Since(startedPodInitializing)
			if *j.pendingTimeout < elapsedTime {
				return errPendingPhase(startedPodInitializing, *j.pendingTimeout)
			}
		}
	}
	return nil
}

func (j *Job) isReadyAllContainers(status corev1.PodStatus) bool {
	for _, s := range status.ContainerStatuses {
		if !s.Ready {
			return false
		}
	}
	return true
}

func (j *Job) watchLoop(ctx context.Context, watcher watch.Interface) (e error) {
	var (
		eg                     errgroup.Group
		finishedRunningPhase   bool
		finishedRunningPhaseMu sync.Mutex
		once                   sync.Once
		onceWatchPendingPhase  sync.Once
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
			onceWatchPendingPhase.Do(func() {
				name := pod.Name
				eg.Go(func() error {
					return j.watchPodPendingPhase(ctx, name)
				})
			})
			if j.preInit.needsToRun(pod.Status) {
				if err := j.preInit.run(pod); err != nil {
					return err
				}
			}
			if j.jobInit.needsToRun(pod.Status) {
				if j.preInit != nil && !j.preInit.done {
					continue
				}
				if err := j.jobInit.run(pod); err != nil {
					return err
				}
			}
			if pod.Status.Phase == phase {
				continue
			}
			switch pod.Status.Phase {
			case corev1.PodRunning:
				if j.preInit != nil && !j.preInit.done {
					return fmt.Errorf("job: preinit step wasn't called but changed pod phase to running")
				}
				if j.jobInit != nil && !j.jobInit.done {
					return fmt.Errorf("job: init containers hook wasn't called but changed pod phase to running")
				}
				if !j.isReadyAllContainers(pod.Status) {
					continue
				}
				once.Do(func() {
					eg.Go(func() error {
						defer func() {
							finishedRunningPhaseMu.Lock()
							finishedRunningPhase = true
							finishedRunningPhaseMu.Unlock()
						}()
						return j.doRunningPhase(ctx, pod)
					})
				})
			case corev1.PodSucceeded, corev1.PodFailed:
				once.Do(func() {
					eg.Go(func() error {
						if err := j.logStreamInitContainers(ctx, pod); err != nil {
							return err
						}
						if j.podRunningCallback == nil {
							if err := j.logStreamPod(ctx, pod); err != nil {
								return err
							}
						}
						return nil
					})
				})
				if pod.Status.Phase == corev1.PodFailed {
					finishedRunningPhaseMu.Lock()
					finished := finishedRunningPhase
					finishedRunningPhaseMu.Unlock()

					if finished {
						return &FailedJob{Pod: pod}
					}
					return &JobUnexpectedError{Pod: pod}
				}
				return nil
			}
			phase = pod.Status.Phase
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func (j *Job) doRunningPhase(ctx context.Context, pod *corev1.Pod) error {
	if err := j.logStreamInitContainers(ctx, pod); err != nil {
		return err
	}
	if j.podRunningCallback != nil {
		if err := j.podRunningCallback(pod); err != nil {
			return err
		}
	} else {
		if err := j.logStreamPod(ctx, pod); err != nil {
			return err
		}
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
			return errLogStreamInitContainer(err)
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
				return err
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
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
		return errLogStream(j.Name, pod, container, err)
	}
	defer stream.Close()

	if enabledCommandLog {
		j.containerLogs <- j.commandLog(pod, container)
	}

	errchan := make(chan error)

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
			return errLogStream(j.Name, pod, container, err)
		}
		return nil
	}
	return nil
}
