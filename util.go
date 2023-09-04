package kubejob

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const jobCommandTemplate = `
while [ ! -f /tmp/kubejob-status ]
do
    sleep 1;
done

exit $(cat /tmp/kubejob-status)
`

const (
	jobOriginalCommandEnvName     = "KUBEJOB_ORIGINAL_COMMAND"
	jobOriginalCommandArgsEnvName = "KUBEJOB_ORIGINAL_COMMAND_ARGS"
)

func jobTemplateCommandContainer(c corev1.Container, agentCfg *AgentConfig, agentPort uint16) corev1.Container {
	copied := c.DeepCopy()
	if agentCfg != nil && agentCfg.Enabled(c.Name) {
		replaceCommandByAgentCommand(copied, agentCfg.InstalledPath(c.Name), agentPort, agentCfg.Timeout())
	} else {
		replaceCommandByJobTemplate(copied)
	}
	copied.Env = append(copied.Env, corev1.EnvVar{
		Name:  jobOriginalCommandEnvName,
		Value: strings.Join(c.Command, " "),
	})
	copied.Env = append(copied.Env, corev1.EnvVar{
		Name:  jobOriginalCommandArgsEnvName,
		Value: strings.Join(c.Args, " "),
	})
	return *copied
}

func replaceCommandByAgentCommand(c *corev1.Container, path string, port uint16, timeout string) {
	c.Command = []string{path}
	c.Args = []string{
		"--port", fmt.Sprint(port),
	}
	if timeout != "" {
		c.Args = append(c.Args, "--timeout", timeout)
	}
}

func replaceCommandByJobTemplate(c *corev1.Container) {
	c.Command = []string{"sh", "-c"}
	c.Args = []string{jobCommandTemplate}
}
