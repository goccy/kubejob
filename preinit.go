package kubejob

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// PreInit can define the process you want to execute before the process of init container specified at the time of starting Job.
// It is mainly intended to be used when you use `(*JobExecutor).CopyToFile` to copy an arbitrary file to pod before init processing.
func (j *Job) PreInit(c corev1.Container, cb func(exec *JobExecutor) error) {
	j.preInit = &preInit{
		container: c,
		callback:  cb,
	}
}

func (j *Job) setupPreInitContainer() error {
	if j.preInit == nil {
		return nil
	}
	c := j.preInit.container
	var agentPort uint16
	if j.agentCfg != nil && j.agentCfg.Enabled(c.Name) {
		port, err := j.agentCfg.NewAllocatedPort()
		if err != nil {
			return err
		}
		agentPort = port
		c.Env = append(c.Env, j.agentCfg.PublicKeyEnv())
	}
	j.preInit.container = jobTemplateCommandContainer(c, j.agentCfg, agentPort)
	j.preInit.exec = &JobExecutor{
		Container: c,
		command:   c.Command,
		args:      c.Args,
		job:       j,
		agentCfg:  j.agentCfg,
		agentPort: agentPort,
	}
	return nil
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
	if err := i.exec.setPod(pod); err != nil {
		return fmt.Errorf("job: failed to set corev1.Pod to preinit executor instance: %w", err)
	}
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
			return errPreInit(err)
		}
	}
	i.done = true
	if err := i.exec.Stop(); err != nil {
		return err
	}
	return nil
}
