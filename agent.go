package kubejob

const (
	defaultAgentGRPCPort        = 5000
	defaultAgentHealthCheckPort = 6000
)

type AgentConfig struct {
	path            string
	grpcPort        uint16
	healthCheckPort uint16
}

func NewAgentConfig(path string) *AgentConfig {
	return &AgentConfig{
		path:            path,
		grpcPort:        defaultAgentGRPCPort,
		healthCheckPort: defaultAgentHealthCheckPort,
	}
}

func (c *AgentConfig) SetGRPCPort(port uint16) {
	c.grpcPort = port
}

func (c *AgentConfig) SetHealthCheckPort(port uint16) {
	c.healthCheckPort = port
}
