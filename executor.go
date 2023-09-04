package kubejob

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/lestrrat-go/backoff"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
)

type JobExecutor struct {
	Container    corev1.Container
	ContainerIdx int
	Pod          *corev1.Pod
	agentCfg     *AgentConfig
	agentPort    uint16
	agentClient  *AgentClient
	command      []string
	args         []string
	job          *Job
	isRunning    bool
	stopped      bool
	isRunningMu  sync.Mutex
	err          error
}

func (e *JobExecutor) EnabledAgent() bool {
	return e.agentCfg != nil && e.agentCfg.Enabled(e.Container.Name)
}

func (e *JobExecutor) setPod(pod *corev1.Pod) error {
	e.Pod = pod
	if e.EnabledAgent() {
		signedToken, err := e.agentCfg.IssueJWT()
		if err != nil {
			return err
		}
		client, err := NewAgentClient(
			pod,
			e.agentPort,
			e.Container.WorkingDir,
			string(signedToken),
		)
		if err != nil {
			return fmt.Errorf("failed to create agent client: %w", err)
		}
		e.agentClient = client
	}
	return nil
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
			vars = append(vars, fmt.Sprintf("VAR%d=$(cat <<-'EOS'\n%s\nEOS\n)", idx, c))
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

func (e *JobExecutor) exec(ctx context.Context, cmd []string) ([]byte, error) {
	if e.EnabledAgent() {
		result, err := e.agentClient.Exec(ctx, cmd, nil)
		if err != nil {
			return nil, err
		}
		if result.Success {
			return []byte(result.Output), nil
		}
		return []byte(result.Output), errCommandFromAgent(result.ErrorMessage)
	}
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
		return nil, fmt.Errorf("job: failed to create spdy executor: %w", err)
	}
	r, w := io.Pipe()
	var writerErr error
	go func() {
		writerErr = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
			Stdin:  nil,
			Stdout: w,
			Stderr: w,
			Tty:    false,
		})
		w.Close()
	}()
	buf := new(bytes.Buffer)
	_, readerErr := buf.ReadFrom(r)
	if writerErr != nil || readerErr != nil {
		return buf.Bytes(), errCommand(readerErr, writerErr)
	}
	return buf.Bytes(), nil
}

func (e *JobExecutor) execWithRetry(ctx context.Context, cmd []string) ([]byte, error) {
	var (
		out []byte
		err error
	)
	policy := backoff.NewExponential(
		backoff.WithInterval(1*time.Second),
		backoff.WithMaxRetries(ExecRetryCount),
	)
	b, cancel := policy.Start(ctx)
	defer cancel()

	retryCount := 0
	for backoff.Continue(b) {
		out, err = e.exec(ctx, cmd)
		if err != nil {
			if cmdErr, ok := err.(*CommandError); ok {
				if cmdErr.IsExitError() {
					break
				}
			}
			// cannot connect to Pod. retry....
			e.job.logDebug(
				"%s at %s. retry: %d/%d",
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

func (e *JobExecutor) Exec(ctx context.Context) ([]byte, error) {
	defer func() {
		if err := e.Stop(); err != nil {
			e.job.logWarn("%s", err)
		}
	}()
	return e.ExecOnly(ctx)
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

func (e *JobExecutor) ExecPrepareCommand(ctx context.Context, cmd []string) ([]byte, error) {
	if e.IsRunning() {
		return nil, fmt.Errorf("job: failed to run prepare command. main command is already executed")
	}
	if !e.job.disabledCommandLog {
		fmt.Println(strings.Join(cmd, " "))
	}

	out, err := e.execWithRetry(ctx, cmd)
	if err != nil {
		return out, err
	}
	return out, nil
}

func (e *JobExecutor) ExecOnly(ctx context.Context) ([]byte, error) {
	if e.IsRunning() {
		return nil, fmt.Errorf("job: duplicate command error. command is already executed")
	}
	if !e.job.disabledCommandLog {
		fmt.Println(strings.Join(append(e.command, e.args...), " "))
	}
	e.setIsRunning(true)
	out, err := e.execWithRetry(ctx, append(e.command, e.args...))
	e.err = err
	if err != nil {
		return out, &FailedJob{Pod: e.Pod, Reason: err}
	}
	return out, nil
}

func (e *JobExecutor) ExecAsync(ctx context.Context) error {
	if e.IsRunning() {
		return fmt.Errorf("job: duplicate command error. command is already executed")
	}
	if !e.job.disabledCommandLog {
		fmt.Println(strings.Join(append(e.command, e.args...), " "))
	}
	e.setIsRunning(true)
	go func() {
		_, err := e.execWithRetry(ctx, append(e.command, e.args...))
		e.err = err
		if err := e.Stop(); err != nil {
			e.job.logWarn("failed to stop async executor: %s", err)
		}
	}()
	return nil
}

func (e *JobExecutor) TerminationLog(log string) error {
	if !e.IsRunning() {
		return fmt.Errorf("job: must be executed command before sending termination log")
	}
	if e.stopped {
		return fmt.Errorf("job: failed to send termination log because container has already been stopped")
	}
	if e.EnabledAgent() {
		return nil
	}
	termMessagePath := e.Container.TerminationMessagePath
	if termMessagePath == "" {
		termMessagePath = corev1.TerminationMessagePathDefault
	}
	if _, err := e.execWithRetry(context.Background(), []string{"echo", log, ">", termMessagePath}); err != nil {
		return err
	}
	return nil
}

func (e *JobExecutor) Stop() error {
	if e.stopped {
		return nil
	}
	defer func() {
		e.setIsRunning(false)
	}()

	// Re-create the context to ensure that the shutdown process is executed.
	ctx := context.Background()
	if e.EnabledAgent() {
		if err := e.agentClient.Stop(ctx); err != nil {
			return errStopContainer(err)
		}
	} else {
		var status int
		if e.err != nil {
			status = 1
		}
		if _, err := e.execWithRetry(ctx, []string{"echo", fmt.Sprint(status), ">", "/tmp/kubejob-status"}); err != nil {
			return errStopContainer(err)
		}
	}
	e.stopped = true
	return nil
}

type JobInitContainerExecutionHandler func(context.Context, *JobExecutor) error

type jobInit struct {
	done                     bool
	containers               []corev1.Container
	executors                []*JobExecutor
	executedContainerNameMap map[string]struct{}
	stepNum                  int
	handler                  JobInitContainerExecutionHandler
	agentCfg                 *AgentConfig
}

func (j *jobInit) needsToRun(status corev1.PodStatus) bool {
	if j == nil {
		return false
	}
	if j.done {
		return false
	}
	if status.Phase != corev1.PodPending {
		return false
	}
	return true
}

func (j *jobInit) isReplacedCommand(c corev1.Container) bool {
	if len(c.Command) == 0 {
		return false
	}
	if j.agentCfg != nil && j.agentCfg.Enabled(c.Name) {
		return c.Command[0] == j.agentCfg.InstalledPath(c.Name)
	}
	if len(c.Args) == 0 {
		return false
	}
	return c.Args[0] == jobCommandTemplate
}

func (j *jobInit) run(ctx context.Context, pod *corev1.Pod) error {
	if j.done {
		return nil
	}
	for _, status := range pod.Status.InitContainerStatuses {
		if status.State.Running != nil {
			if _, exists := j.executedContainerNameMap[status.Name]; exists {
				continue
			}
			for _, c := range pod.Spec.InitContainers {
				if c.Name == status.Name {
					if j.isReplacedCommand(c) {
						exec := j.executors[j.stepNum]
						if err := exec.setPod(pod); err != nil {
							return fmt.Errorf("job: failed to set corev1.Pod to executor instance: %w", err)
						}
						if err := j.handler(ctx, exec); err != nil {
							return err
						}
						if err := exec.Stop(); err != nil {
							return err
						}
						j.stepNum++
						j.executedContainerNameMap[status.Name] = struct{}{}
					}
				}
			}
		}
	}
	if j.stepNum >= len(j.executors) {
		j.done = true
	}
	return nil
}

func (j *Job) SetInitContainerExecutionHandler(handler JobInitContainerExecutionHandler) error {
	if handler == nil {
		return fmt.Errorf("job: failed to set JobInitContainerExecutionHandler. handler is nil")
	}
	j.jobInit = &jobInit{
		handler:                  handler,
		executedContainerNameMap: map[string]struct{}{},
	}
	return nil
}

func (j *Job) setupInitContainers() error {
	if j.jobInit == nil {
		return nil
	}
	for idx, c := range j.Job.Spec.Template.Spec.InitContainers {
		c := c
		var agentPort uint16
		if j.agentCfg != nil && j.agentCfg.Enabled(c.Name) {
			port, err := j.agentCfg.NewAllocatedPort()
			if err != nil {
				return err
			}
			agentPort = port
			c.Env = append(c.Env, j.agentCfg.PublicKeyEnv())
		}
		j.jobInit.executors = append(j.jobInit.executors, &JobExecutor{
			Container:    c,
			ContainerIdx: idx,
			command:      c.Command,
			args:         c.Args,
			job:          j,
			agentCfg:     j.agentCfg,
			agentPort:    agentPort,
		})
		j.jobInit.containers = append(j.jobInit.containers, jobTemplateCommandContainer(c, j.agentCfg, agentPort))
	}
	return nil
}

// UseAgent when you use RunWithExecutionHandler, kubejob replace specified commands with a wait loop command
// to control when the command is executed.
// If the kubejob-agent is present in the container,
// you can specify its path and the port to listen to so that it will wait using the kubejob-agent.
func (j *Job) UseAgent(agentCfg *AgentConfig) {
	j.agentCfg = agentCfg
}

type JobExecutionHandler func(context.Context, []*JobExecutor) error

type JobFinalizerHandler func(context.Context, *JobExecutor) error

type JobFinalizer struct {
	Container corev1.Container
	Handler   JobFinalizerHandler
}

// RunWithExecutionHandler allows control timing the container commands are executed.
//
// finalizer: specify the container to call after executing JobExecutionHandler.
// If a sidecar is explicitly specified, it can be stopped after the execution handler has finished, but this will not work if an injected container is present.
// If SetDeletePropagationPolicy is set, kubejob itself deletes the Pod by the forced termination process, so the Job can be terminated even if an injected container exists.
// However, if kubejob itself does not delete Pods, the forced termination process cannot be executed either.
// Therefore, by specifying the finalizer container, you can explicitly terminate the injected container.
func (j *Job) RunWithExecutionHandler(ctx context.Context, handler JobExecutionHandler, finalizer *JobFinalizer) error {
	executorMap := map[string]*JobExecutor{}
	for idx := range j.Job.Spec.Template.Spec.Containers {
		executor, err := j.containerToJobExecutor(idx, &j.Job.Spec.Template.Spec.Containers[idx])
		if err != nil {
			return err
		}
		executorMap[j.Job.Spec.Template.Spec.Containers[idx].Name] = executor
	}
	var finalizerExecutor *JobExecutor
	if finalizer != nil {
		executor, err := j.containerToJobExecutor(len(j.Job.Spec.Template.Spec.Containers), &finalizer.Container)
		if err != nil {
			return err
		}
		finalizerExecutor = executor
		j.Job.Spec.Template.Spec.Containers = append(j.Job.Spec.Template.Spec.Containers, finalizer.Container)
	}
	j.DisableCommandLog()
	existsErrContainer := false
	var callbackPod *corev1.Pod
	j.podRunningCallback = func(pod *corev1.Pod) error {
		callbackPod = pod
		executors := []*JobExecutor{}
		for _, container := range pod.Spec.Containers {
			if executor, exists := executorMap[container.Name]; exists {
				if err := executor.setPod(pod); err != nil {
					return fmt.Errorf("failed to set corev1.Pod to executor instance: %w", err)
				}
				executors = append(executors, executor)
			} else {
				// found injected container.
				// Since kubejob cannot handle termination of this container, needs to use finalizer.
			}
		}
		if finalizerExecutor != nil {
			if err := finalizerExecutor.setPod(pod); err != nil {
				return fmt.Errorf("failed to set corev1.Pod to finalizer executor instance: %w", err)
			}
		}
		defer func() {
			for _, executor := range executors {
				if executor.err != nil {
					existsErrContainer = true
				}
				if err := executor.Stop(); err != nil {
					j.logWarn("failed to stop %s", err)
				}
			}
			if finalizerExecutor != nil {
				// Re-create the context to ensure that the shutdown process is executed.
				ctx := context.Background()
				if finalizer.Handler != nil {
					if err := finalizer.Handler(ctx, finalizerExecutor); err != nil {
						j.logWarn("failed to finalize: %s", err)
					}
					if err := finalizerExecutor.Stop(); err != nil {
						j.logWarn("failed to stop finalizer: %w", err)
					}
				} else {
					if _, err := finalizerExecutor.Exec(ctx); err != nil {
						j.logWarn("failed to finalize: %s", err)
					}
				}
			}
		}()
		if err := handler(ctx, executors); err != nil {
			return err
		}
		return nil
	}

	// Create a new context.
	// If cancel is set, it is passed to the ExecutionHandler after the pod is Running.
	// Create a new context so that it does not exit before that time.
	if err := j.Run(context.Background()); err != nil {
		return err
	}

	// if call cancel() to stop all containers, return `nil` error from Run() loop.
	// So, existsErrContainer check whether exists stopped container with failed status.
	if existsErrContainer {
		return &FailedJob{Pod: callbackPod}
	}
	return nil
}

func (j *Job) containerToJobExecutor(idx int, container *corev1.Container) (*JobExecutor, error) {
	orgContainer := *container
	command := container.Command
	args := container.Args

	var agentPort uint16
	if j.agentCfg != nil && j.agentCfg.Enabled(container.Name) {
		port, err := j.agentCfg.NewAllocatedPort()
		if err != nil {
			return nil, fmt.Errorf("failed to allocate a new port for agent: %w", err)
		}
		agentPort = port
		replaceCommandByAgentCommand(container, j.agentCfg.InstalledPath(container.Name), port)
		container.Env = append(container.Env, j.agentCfg.PublicKeyEnv())
	} else {
		replaceCommandByJobTemplate(container)
	}
	container.Env = append(container.Env, corev1.EnvVar{
		Name:  jobOriginalCommandEnvName,
		Value: strings.Join(command, " "),
	})
	container.Env = append(container.Env, corev1.EnvVar{
		Name:  jobOriginalCommandArgsEnvName,
		Value: strings.Join(args, " "),
	})
	return &JobExecutor{
		Container:    orgContainer,
		ContainerIdx: idx,
		command:      command,
		args:         args,
		job:          j,
		agentCfg:     j.agentCfg,
		agentPort:    agentPort,
	}, nil
}
