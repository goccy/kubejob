package kubejob

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/goccy/kubejob/agent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	corev1 "k8s.io/api/core/v1"
)

type AgentClient struct {
	serverPod  *corev1.Pod
	workingDir string
	client     agent.AgentClient
}

const (
	GRPCClientKeepaliveTime    = 30 * time.Second
	GRPCClientKeepaliveTimeout = 5 * 60 * time.Second
)

func NewAgentClient(agentServerPod *corev1.Pod, listenPort uint16, workingDir, signedToken string) (*AgentClient, error) {
	ipAddr := agentServerPod.Status.PodIP
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", ipAddr, listenPort),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                GRPCClientKeepaliveTime,
			Timeout:             GRPCClientKeepaliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithUnaryInterceptor(agentAuthUnaryInterceptor(signedToken)),
		grpc.WithStreamInterceptor(agentAuthStreamInterceptor(signedToken)),
	)
	if err != nil {
		return nil, fmt.Errorf("job: failed to dial grpc: %w", err)
	}
	fmt.Println("setup keepalive")
	client := agent.NewAgentClient(conn)
	fmt.Println("agent-client", fmt.Sprintf("%s:%d", ipAddr, listenPort))
	return &AgentClient{
		serverPod:  agentServerPod,
		workingDir: workingDir,
		client:     client,
	}, nil
}

type AgentExecResult struct {
	Output         string
	Success        bool
	ExitCode       int32
	ErrorMessage   string
	ElapsedTimeSec int64
}

func (c *AgentClient) Exec(ctx context.Context, command []string, env []corev1.EnvVar) (*AgentExecResult, error) {
	agentEnv := make([]*agent.Env, 0, len(env))
	for _, e := range env {
		agentEnv = append(agentEnv, &agent.Env{
			Name:  e.Name,
			Value: e.Value,
		})
	}
	res, err := c.client.Exec(ctx, &agent.ExecRequest{
		Command:    command,
		Env:        agentEnv,
		WorkingDir: c.workingDir,
	})
	if err != nil {
		return nil, err
	}
	return &AgentExecResult{
		Output:         res.Output,
		Success:        res.Success,
		ExitCode:       res.ExitCode,
		ErrorMessage:   res.ErrorMessage,
		ElapsedTimeSec: res.ElapsedTimeSec,
	}, nil
}

func (c *AgentClient) CopyFrom(ctx context.Context, srcPath, dstPath string) error {
	finfo, err := os.Stat(dstPath)
	archivedFilePath := fmt.Sprintf("%s.tar", dstPath)
	if err == nil && finfo.IsDir() {
		// if dstPath is directory, copy specified src into it.
		archivedFilePath = filepath.Join(dstPath, fmt.Sprintf("%s.tar", filepath.Base(dstPath)))
	}
	defer os.Remove(archivedFilePath) // ignore error
	if err := c.copyFrom(ctx, srcPath, archivedFilePath); err != nil {
		return err
	}
	if err := extractArchivedFile(archivedFilePath, dstPath); err != nil {
		return fmt.Errorf("failed to extract file %s: %w", archivedFilePath, err)
	}
	return nil
}

func (c *AgentClient) copyFrom(ctx context.Context, srcPath, dstPath string) error {
	stream, err := c.client.CopyFrom(ctx, &agent.CopyFromRequest{
		Path: srcPath,
	})
	if err != nil {
		fmt.Println("err", fmt.Errorf("job: failed to create grpc stream to copy from pod: %w", err))
		return fmt.Errorf("job: failed to create grpc stream to copy from pod: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		fmt.Println(fmt.Errorf("job: failed to create directory %s: %w", filepath.Dir(dstPath), err))
		return fmt.Errorf("job: failed to create directory %s: %w", filepath.Dir(dstPath), err)
	}
	f, err := os.Create(dstPath)
	if err != nil {
		fmt.Println(fmt.Errorf("job: failed to create file %s to copy: %w", dstPath, err))
		return fmt.Errorf("job: failed to create file %s to copy: %w", dstPath, err)
	}
	defer f.Close()

	for {
		copyFromResponse, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("job: failed to grpc stream copy: %w", err)
		}
		if _, err := f.Write(copyFromResponse.Data); err != nil {
			return fmt.Errorf("job: failed to write data: %w", err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("job: failed to close send request: %w", err)
	}
	return nil
}

func (c *AgentClient) CopyTo(ctx context.Context, srcPath, dstPath string) error {
	stream, err := c.client.CopyTo(ctx)
	if err != nil {
		return fmt.Errorf("job: failed to create grpc stream to copy to pod: %w", err)
	}

	buf := make([]byte, defaultStreamFileChunkSize)
	if _, err := os.Stat(srcPath); err != nil {
		return fmt.Errorf("job: failed to get status of file %s: %w", srcPath, err)
	}

	archivedFilePath, err := archivePath(srcPath)
	if err != nil {
		return err
	}
	f, err := os.Open(archivedFilePath)
	if err != nil {
		return fmt.Errorf("job: failed to open archived file %s: %w", archivedFilePath, err)
	}
	defer f.Close()
	finfo, err := f.Stat()
	if err != nil {
		return fmt.Errorf("job: failed to get file info of %s: %w", archivedFilePath, err)
	}
	if err := stream.Send(&agent.CopyToRequest{
		Path: dstPath,
	}); err != nil {
		return fmt.Errorf("job: failed to send dst path with grpc stream: %w", err)
	}
	retryCount := 0
	sendSize := 0
	for {
		n, err := f.Read(buf)
		if err == io.EOF {
			if int64(sendSize) != finfo.Size() && retryCount < 3 {
				retryCount++
				fmt.Printf("SEND SIZE is not finfo.Size(). retry %d\n", retryCount)
				fmt.Println("SEND SIZE", sendSize, "finfo.Size() = ", finfo.Size())
				continue
			}
			break
		}
		if err != nil {
			return fmt.Errorf("job: failed to read file %s: %w", archivedFilePath, err)
		}
		sendSize += n
		if err := stream.Send(&agent.CopyToRequest{
			Data: buf[:n],
		}); err != nil {
			return fmt.Errorf("job: failed to send file data with grpc stream: %w", err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("job: failed to close grpc stream: %w", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("job: failed to recv data with grpc stream: %w", err)
	}
	if resp.CopiedLength != finfo.Size() {
		fmt.Println("SEND SIZE", sendSize, "finfo.Size = ", finfo.Size())
		return fmt.Errorf("job: mismatch copied length. expected size %d but got copied size %d", finfo.Size(), resp.CopiedLength)
	}
	return nil
}

func (c *AgentClient) Stop(ctx context.Context) error {
	_, err := c.client.Finish(ctx, &agent.FinishRequest{})
	if err != nil {
		return fmt.Errorf("job: failed to finish agent: %w", err)
	}
	return nil
}
