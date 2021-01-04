package kubejob

func (e *JobExecutor) ExecWithPodNotFoundError() ([]byte, error) {
	name := e.Pod.Name
	e.Pod.Name = "invalid-pod-name"
	out, err := e.Exec()
	e.Pod.Name = name
	e.Stop()
	return out, err
}
