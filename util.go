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

func jobTemplateCommandContainer(c corev1.Container, agentCfg *AgentConfig) corev1.Container {
	copied := c.DeepCopy()
	if agentCfg != nil {
		replaceCommandByAgentConfig(copied, agentCfg)
	} else {
		replaceCommandByJobTemplate(copied)
	}
	return *copied
}

func replaceCommandByAgentConfig(c *corev1.Container, cfg *AgentConfig) {
	c.Command = []string{cfg.path}
	c.Args = []string{
		"--grpc-port", fmt.Sprint(cfg.grpcPort),
		"--health-check-port", fmt.Sprint(cfg.healthCheckPort),
	}
}

func replaceCommandByJobTemplate(c *corev1.Container) {
	c.Command = []string{"sh", "-c"}
	c.Args = []string{jobCommandTemplate}
}
