package kubejob

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
	corev1 "k8s.io/api/core/v1"
)

const (
	defaultAgentGRPCPort        = 5000
	defaultAgentHealthCheckPort = 6000
	rsaBitSize                  = 2048
	agentJWTIssuer              = "kubejob"
	agentPublicKeyPEMName       = "AGENT_PUBLIC_KEY_PEM"
)

type AgentConfig struct {
	path            string
	grpcPort        uint16
	healthCheckPort uint16
	privateKey      *rsa.PrivateKey
	publicKeyPEM    string
}

func NewAgentConfig(path string) (*AgentConfig, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, rsaBitSize)
	if err != nil {
		return nil, fmt.Errorf("job: failed to generate rsa key: %w", err)
	}
	publicKeyPEM, err := jwk.EncodePEM(&privateKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("job: failed to encode public key: %w", err)
	}
	return &AgentConfig{
		path:            path,
		grpcPort:        defaultAgentGRPCPort,
		healthCheckPort: defaultAgentHealthCheckPort,
		privateKey:      privateKey,
		publicKeyPEM:    string(publicKeyPEM),
	}, nil
}

func (c *AgentConfig) IssueJWT() ([]byte, error) {
	token, err := jwt.NewBuilder().
		Issuer(agentJWTIssuer).
		IssuedAt(time.Now()).
		Build()
	if err != nil {
		return nil, fmt.Errorf("job: failed to create jwt token: %w", err)
	}
	signed, err := jwt.Sign(token, jwt.WithKey(jwa.RS256, c.privateKey))
	if err != nil {
		return nil, fmt.Errorf("job: failed to sign token: %w", err)
	}
	return signed, nil
}

func (c *AgentConfig) PublicKeyEnv() corev1.EnvVar {
	return corev1.EnvVar{
		Name:  agentPublicKeyPEMName,
		Value: c.publicKeyPEM,
	}
}

func (c *AgentConfig) SetGRPCPort(port uint16) {
	c.grpcPort = port
}

func (c *AgentConfig) SetHealthCheckPort(port uint16) {
	c.healthCheckPort = port
}

func authorizeAgentJWT(signed []byte) (jwt.Token, error) {
	publicKey := os.Getenv(agentPublicKeyPEMName)
	if publicKey == "" {
		return nil, fmt.Errorf("failed to get public key to authorize token")
	}
	pubkey, _, err := jwk.DecodePEM([]byte(publicKey))
	if err != nil {
		return nil, fmt.Errorf("failed to decode public key: %w", err)
	}
	token, err := jwt.Parse(signed, jwt.WithKey(jwa.RS256, pubkey))
	if err != nil {
		return nil, fmt.Errorf("failed to verify token: %w", err)
	}
	if token.Issuer() != agentJWTIssuer {
		return nil, fmt.Errorf("unknown issuer %s", token.Issuer())
	}
	return token, nil
}

func agentAuthUnaryInterceptor(signedToken string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		md := metadata.Pairs("Authorization", fmt.Sprintf("Bearer %s", signedToken))
		ctx = metadata.NewOutgoingContext(ctx, md)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func agentAuthStreamInterceptor(signedToken string) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		md := metadata.Pairs("Authorization", fmt.Sprintf("Bearer %s", signedToken))
		ctx = metadata.NewOutgoingContext(ctx, md)
		return streamer(ctx, desc, cc, method, opts...)
	}
}
