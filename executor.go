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
		return nil, fmt.Errorf("job: failed to create spdy executor: %w", err)
	}
	r, w := io.Pipe()
	var writerErr error
	go func() {
		writerErr = exec.Stream(remotecommand.StreamOptions{
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

func (e *JobExecutor) Exec() ([]byte, error) {
	defer func() {
		if err := e.Stop(); err != nil {
			e.job.logWarn("%s", err)
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
		return nil, fmt.Errorf("job: failed to run prepare command. main command is already executed")
	}
	if !e.job.disabledCommandLog {
		fmt.Println(strings.Join(cmd, " "))
	}

	out, err := e.execWithRetry(cmd)
	if err != nil {
		return out, &FailedJob{Pod: e.Pod, Reason: err}
	}
	return out, nil
}

func (e *JobExecutor) ExecOnly() ([]byte, error) {
	if e.IsRunning() {
		return nil, fmt.Errorf("job: duplicate command error. command is already executed")
	}
	if !e.job.disabledCommandLog {
		fmt.Println(strings.Join(append(e.command, e.args...), " "))
	}
	e.setIsRunning(true)
	out, err := e.execWithRetry(append(e.command, e.args...))
	e.err = err
	if err != nil {
		return out, &FailedJob{Pod: e.Pod, Reason: err}
	}
	return out, nil
}

func (e *JobExecutor) ExecAsync() error {
	if e.IsRunning() {
		return fmt.Errorf("job: duplicate command error. command is already executed")
	}
	if !e.job.disabledCommandLog {
		fmt.Println(strings.Join(append(e.command, e.args...), " "))
	}
	e.setIsRunning(true)
	go func() {
		_, err := e.execWithRetry(append(e.command, e.args...))
		e.err = err
		if err := e.Stop(); err != nil {
			e.job.logWarn("failed to stop async executor: %s", err)
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
		return errStopContainer(err)
	}
	e.stopped = true
	return nil
}

type JobExecutionHandler func([]*JobExecutor) error

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
						j.logWarn("failed to stop %s", err)
						forceStop = true
					}
				}
			}
			if forceStop {
				cancelFn()
			}
		}()
		if err := handler(executors); err != nil {
			return err
		}
		return nil
	}
	if err := j.Run(ctx); err != nil {
		return err
	}

	// if call cancel() to stop all containers, return `nil` error from Run() loop.
	// So, existsErrContainer check whether exists stopped container with failed status.
	if existsErrContainer {
		return &FailedJob{Pod: callbackPod}
	}
	return nil
}
