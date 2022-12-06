package kubejob

import (
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	executil "k8s.io/client-go/util/exec"
)

type FailedJob struct {
	Pod    *corev1.Pod
	Reason error
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
	if j.Reason != nil {
		return j.Reason.Error()
	}
	return "job: failed to job"
}

type CleanupError struct {
	JobName string
	Errs    []error
}

func (e *CleanupError) Error() string {
	msgs := make([]string, 0, len(e.Errs))
	for _, err := range e.Errs {
		msgs = append(msgs, err.Error())
	}
	msg := strings.Join(msgs, ":")
	return fmt.Sprintf("job: failed to cleanup %s: %s", e.JobName, msg)
}

type LogStreamError struct {
	JobName       string
	Pod           *corev1.Pod
	Container     corev1.Container
	InitContainer bool
	Err           error
}

func (e *LogStreamError) Error() string {
	var containerKey string
	if e.InitContainer {
		containerKey = "init container"
	} else {
		containerKey = "container"
	}
	return fmt.Sprintf(
		"job: failed to log stream %s pod:%s %s:%s: %s",
		e.JobName,
		e.Pod.Name,
		containerKey,
		e.Container.Name,
		e.Err,
	)
}

type JobCreationError struct {
	JobName         string
	JobGenerateName string
	Err             error
}

func (e *JobCreationError) jobName() string {
	if e.JobName != "" {
		return e.JobName
	}
	if e.JobGenerateName != "" {
		return e.JobGenerateName
	}
	return ""
}

func (e *JobCreationError) Error() string {
	jobName := e.jobName()
	if jobName != "" {
		return fmt.Sprintf("job: failed to create job %s: %s", jobName, e.Err)
	}
	return fmt.Sprintf("job: failed to create job: %s", e.Err)
}

type JobWatchError struct {
	JobName string
	Err     error
}

func (e *JobWatchError) Error() string {
	return fmt.Sprintf("job: failed to watch job %s: %s", e.JobName, e.Err)
}

type ValidationError struct {
	Required string
	Err      error
}

func (e *ValidationError) Error() string {
	if e.Required != "" {
		return fmt.Sprintf("job: validation error. %s must be specified", e.Required)
	}
	if e.Err != nil {
		return fmt.Sprintf("job: failed to decode job spec: %s", e.Err)
	}
	return ""
}

type PreInitError struct {
	Err error
}

func (e *PreInitError) Error() string {
	return fmt.Sprintf("job: failed to run preinit step: %s", e.Err)
}

type CommandError struct {
	Message   string
	ReaderErr error
	WriterErr error
}

func (e *CommandError) IsExitError() bool {
	if e.Message != "" {
		return true
	}
	if _, ok := e.ReaderErr.(executil.ExitError); ok {
		return true
	}
	if _, ok := e.WriterErr.(executil.ExitError); ok {
		return true
	}
	return false
}

func (e *CommandError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	msgs := []string{}
	if e.ReaderErr != nil {
		msgs = append(msgs, fmt.Sprintf("read error: %s", e.ReaderErr))
	}
	if e.WriterErr != nil {
		msgs = append(msgs, fmt.Sprintf("write error: %s", e.WriterErr))
	}
	return fmt.Sprintf("job: failed to run command: %s", strings.Join(msgs, ". "))
}

type JobStopContainerError struct {
	Reason error
}

func (e *JobStopContainerError) Error() string {
	return fmt.Sprintf("job: failed to stop container. %s", e.Reason)
}

type PendingPhaseTimeoutError struct {
	Timeout   time.Duration
	StartedAt time.Time
}

func (e *PendingPhaseTimeoutError) Error() string {
	return fmt.Sprintf(
		"job: failed to move running phase. %s has passed while in the PodInitializing state",
		e.Timeout,
	)
}

type CopyError struct {
	SrcPath string
	DstPath string
	Err     error
}

func (e *CopyError) Error() string {
	return fmt.Sprintf("job: failed to copy from %s to %s. %s", e.SrcPath, e.DstPath, e.Err)
}

type JobUnexpectedError struct {
	Pod *corev1.Pod
}

func (e *JobUnexpectedError) Error() string {
	return fmt.Sprintf("job: pod status became failed without being in running phase: %s", e.Pod.Status.Message)
}

type JobMultiError struct {
	Errs []error
}

type ErrorType int

const (
	FailedJobType ErrorType = iota
	CleanupErrorType
	LogStreamErrorType
	JobCreationErrorType
	JobWatchErrorType
	ValidationErrorType
	PreInitErrorType
	CommandErrorType
	JobStopContainerErrorType
	PendingPhaseTimeoutErrorType
	CopyErrorType
	JobUnexpectedErrorType
)

func (e *JobMultiError) Has(typ ErrorType) bool {
	switch typ {
	case FailedJobType:
		for _, err := range e.Errs {
			if _, ok := err.(*FailedJob); ok {
				return true
			}
		}
	case CleanupErrorType:
		for _, err := range e.Errs {
			if _, ok := err.(*CleanupError); ok {
				return true
			}
		}
	case LogStreamErrorType:
		for _, err := range e.Errs {
			if _, ok := err.(*LogStreamError); ok {
				return true
			}
		}
	case JobCreationErrorType:
		for _, err := range e.Errs {
			if _, ok := err.(*JobCreationError); ok {
				return true
			}
		}
	case JobWatchErrorType:
		for _, err := range e.Errs {
			if _, ok := err.(*JobWatchError); ok {
				return true
			}
		}
	case ValidationErrorType:
		for _, err := range e.Errs {
			if _, ok := err.(*ValidationError); ok {
				return true
			}
		}
	case PreInitErrorType:
		for _, err := range e.Errs {
			if _, ok := err.(*PreInitError); ok {
				return true
			}
		}
	case CommandErrorType:
		for _, err := range e.Errs {
			if _, ok := err.(*CommandError); ok {
				return true
			}
		}
	case JobStopContainerErrorType:
		for _, err := range e.Errs {
			if _, ok := err.(*JobStopContainerError); ok {
				return true
			}
		}
	case PendingPhaseTimeoutErrorType:
		for _, err := range e.Errs {
			if _, ok := err.(*PendingPhaseTimeoutError); ok {
				return true
			}
		}
	case CopyErrorType:
		for _, err := range e.Errs {
			if _, ok := err.(*CopyError); ok {
				return true
			}
		}
	case JobUnexpectedErrorType:
		for _, err := range e.Errs {
			if _, ok := err.(*JobUnexpectedError); ok {
				return true
			}
		}
	}
	return false
}

func (e *JobMultiError) Error() string {
	errs := make([]string, 0, len(e.Errs))
	for _, err := range e.Errs {
		errs = append(errs, err.Error())
	}
	return strings.Join(errs, ":")
}

func errPendingPhase(startedAt time.Time, timeout time.Duration) error {
	return &PendingPhaseTimeoutError{
		StartedAt: startedAt,
		Timeout:   timeout,
	}
}

func errCopy(srcPath, dstPath string, err error) error {
	return &CopyError{
		SrcPath: srcPath,
		DstPath: dstPath,
		Err:     err,
	}
}

func errCopyWithReaderWriter(srcPath, dstPath string, readerErr error, writerErr error, out string) error {
	msgs := []string{}
	if readerErr != nil {
		msgs = append(msgs, fmt.Sprintf("read error: %s", readerErr))
	}
	if writerErr != nil {
		msgs = append(msgs, fmt.Sprintf("write error: %s", writerErr))
	}
	if len(out) > 0 {
		msgs = append(msgs, fmt.Sprintf("logs: %s", out))
	}
	return &CopyError{
		SrcPath: srcPath,
		DstPath: dstPath,
		Err:     fmt.Errorf("failed to run command: %s", strings.Join(msgs, ". ")),
	}
}

func errCopyWithEmptyPath(srcPath, dstPath string) error {
	return &CopyError{
		SrcPath: srcPath,
		DstPath: dstPath,
		Err:     fmt.Errorf("cannot specify empty path"),
	}
}

func errStopContainer(reason error) error {
	return &JobStopContainerError{Reason: reason}
}

func errCommand(reader, writer error) error {
	return &CommandError{
		ReaderErr: reader,
		WriterErr: writer,
	}
}

func errCommandFromAgent(msg string) error {
	return &CommandError{
		Message: msg,
	}
}

func errPreInit(err error) error {
	return &PreInitError{Err: err}
}

func errInvalidYAML(err error) error {
	return &ValidationError{Err: err}
}

func errRequiredParam(required string) error {
	return &ValidationError{Required: required}
}

func errJobCreation(jobName, generateName string, err error) error {
	return &JobCreationError{
		JobName:         jobName,
		JobGenerateName: generateName,
		Err:             err,
	}
}

func errJobWatch(jobName string, err error) error {
	return &JobWatchError{
		JobName: jobName,
		Err:     err,
	}
}

func errCleanup(jobName string, errs []error) error {
	return &CleanupError{
		JobName: jobName,
		Errs:    errs,
	}
}

func errLogStream(jobName string, pod *corev1.Pod, container corev1.Container, err error) error {
	return &LogStreamError{
		JobName:   jobName,
		Pod:       pod,
		Container: container,
		Err:       err,
	}
}

func errLogStreamInitContainer(err error) error {
	logStreamErr, ok := err.(*LogStreamError)
	if ok {
		logStreamErr.InitContainer = true
	}
	return err
}

func errMulti(errs ...error) *JobMultiError {
	return &JobMultiError{
		Errs: errs,
	}
}
