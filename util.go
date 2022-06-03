package kubejob

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

const jobCommandTemplate = `
while [ ! -f /tmp/kubejob-status ]
do
    sleep 1;
done

exit $(cat /tmp/kubejob-status)
`

func jobTemplateCommandContainer(c corev1.Container, agentCfg *AgentConfig, agentPort uint16) corev1.Container {
	copied := c.DeepCopy()
	if agentCfg != nil {
		replaceCommandByAgentCommand(copied, agentCfg.path, agentPort)
	} else {
		replaceCommandByJobTemplate(copied)
	}
	return *copied
}

func replaceCommandByAgentCommand(c *corev1.Container, path string, port uint16) {
	c.Command = []string{path}
	c.Args = []string{
		"--port", fmt.Sprint(port),
	}
}

func replaceCommandByJobTemplate(c *corev1.Container) {
	c.Command = []string{"sh", "-c"}
	c.Args = []string{jobCommandTemplate}
}
