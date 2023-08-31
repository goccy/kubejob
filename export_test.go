package kubejob

import "context"

func (e *JobExecutor) ExecWithPodNotFoundError() ([]byte, error) {
	name := e.Pod.Name
	e.Pod.Name = "invalid-pod-name"
	out, err := e.Exec(context.Background())
	e.Pod.Name = name
	e.Stop()
	return out, err
}

func SetExecRetryCount(retry int) func() {
	defaultCount := ExecRetryCount
	ExecRetryCount = retry
	return func() { ExecRetryCount = defaultCount }
}

var AgentAuthUnaryInterceptor = agentAuthUnaryInterceptor
var AgentAuthStreamInterceptor = agentAuthStreamInterceptor
