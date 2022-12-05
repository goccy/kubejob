package kubejob

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/goccy/kubejob/agent"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

var _ agent.AgentServer = &AgentServer{}

const defaultStreamFileChunkSize = 1024 // 1KB

type AgentServer struct {
	port   uint16
	stopCh chan struct{}
}

func NewAgentServer(port uint16) *AgentServer {
	return &AgentServer{
		port:   port,
		stopCh: make(chan struct{}),
	}
}

func (s *AgentServer) Exec(ctx context.Context, req *agent.ExecRequest) (*agent.ExecResponse, error) {
	log.Println("received exec request")
	env := os.Environ()
	for _, e := range req.Env {
		env = append(env, fmt.Sprintf("%s=%s", e.Name, e.Value))
	}
	log.Printf("exec command: %s", strings.Join(req.Command, " "))
	var buf bytes.Buffer
	w := io.MultiWriter(&buf, os.Stdout)
	cmd := exec.Command(req.Command[0], req.Command[1:]...)
	cmd.Stdout = w
	cmd.Stderr = w
	cmd.Env = env
	cmd.Dir = req.GetWorkingDir()
	var errMessage string
	start := time.Now()
	if err := cmd.Run(); err != nil {
		errMessage = err.Error()
	}
	elapsedTime := time.Since(start)
	log.Printf("elapsed time: %s", elapsedTime)
	return &agent.ExecResponse{
		Output:         buf.String(),
		Success:        cmd.ProcessState.Success(),
		ExitCode:       int32(cmd.ProcessState.ExitCode()),
		ErrorMessage:   errMessage,
		ElapsedTimeSec: int64(elapsedTime.Seconds()),
	}, nil
}

func (s *AgentServer) CopyFrom(req *agent.CopyFromRequest, stream agent.Agent_CopyFromServer) error {
	log.Println("received copyFrom request")
	if err := s.copyFrom(req, stream); err != nil {
		log.Println(err)
	}
	return nil
}

func (s *AgentServer) copyFrom(req *agent.CopyFromRequest, stream agent.Agent_CopyFromServer) error {
	archivedFilePath, err := archivePath(req.Path)
	if err != nil {
		return err
	}
	f, err := os.Open(archivedFilePath)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", archivedFilePath, err)
	}
	defer f.Close()
	buf := make([]byte, defaultStreamFileChunkSize)
	for {
		n, err := f.Read(buf)
		if err == io.EOF {
			fmt.Println("server: EOF", n)
			if n > 0 {
				return fmt.Errorf("failed to send buffer length %d", n)
			}
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", archivedFilePath, err)
		}
		fmt.Println("send buffer")
		if err := stream.Send(&agent.CopyFromResponse{
			Data: buf[:n],
		}); err != nil {
			fmt.Println(fmt.Errorf("failed to send file data with grpc stream: %w", err))
			return fmt.Errorf("failed to send file data with grpc stream: %w", err)
		}
	}
	return nil
}

func (s *AgentServer) CopyTo(stream agent.Agent_CopyToServer) error {
	log.Println("received copyTo request")
	copyToResponse, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to recv data: %w", err)
	}
	path := copyToResponse.GetPath()
	finfo, err := os.Stat(path)
	archivedFilePath := fmt.Sprintf("%s.tar", path)
	if err == nil && finfo.IsDir() {
		// if path is directory, copy specified src into it.
		archivedFilePath = filepath.Join(path, fmt.Sprintf("%s.tar", filepath.Base(path)))
	}
	defer os.Remove(archivedFilePath)
	copiedLength, err := s.copyTo(archivedFilePath, stream)
	if err != nil {
		log.Printf("copied length %d", copiedLength)
		log.Println(err)
		return err
	}
	if err := extractArchivedFile(archivedFilePath, path); err != nil {
		return fmt.Errorf("failed to extract archived file %s: %w", archivedFilePath, err)
	}
	return nil
}

func (s *AgentServer) copyTo(path string, stream agent.Agent_CopyToServer) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, fmt.Errorf("failed to create directory %s: %w", filepath.Dir(path), err)
	}
	f, err := os.Create(path)
	if err != nil {
		return 0, fmt.Errorf("failed to create copy target file %s: %w", path, err)
	}
	defer f.Close()

	copiedLength := int64(0)
	for {
		copyToResponse, err := stream.Recv()
		if err == io.EOF {
			fmt.Println("recv io.EOF", copyToResponse)
			break
		}
		if err != nil {
			return copiedLength, fmt.Errorf("failed to copy file. current copied buffer size is %d: %w", copiedLength, err)
		}
		n, err := f.Write(copyToResponse.Data)
		if err != nil {
			return copiedLength, fmt.Errorf("failed to write data to file: %w", err)
		}
		copiedLength += int64(n)
	}
	if err := stream.Send(&agent.CopyToResponse{CopiedLength: copiedLength}); err != nil {
		return copiedLength, fmt.Errorf("failed to send CopyToResponse: %w", err)
	}
	return copiedLength, nil
}

func (s *AgentServer) Finish(ctx context.Context, req *agent.FinishRequest) (*agent.FinishResponse, error) {
	log.Println("received finish request")
	defer func() {
		s.stopCh <- struct{}{}
	}()
	return &agent.FinishResponse{}, nil
}

func (s *AgentServer) Run(ctx context.Context) error {
	listenPort, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen grpc port %d: %w", s.port, err)
	}

	server := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(
			keepalive.EnforcementPolicy{
				MinTime:             5 * time.Second,
				PermitWithoutStream: true,
			},
		),
		grpc.StreamInterceptor(grpc_auth.StreamServerInterceptor(authFunc)),
		grpc.UnaryInterceptor(grpc_auth.UnaryServerInterceptor(authFunc)),
	)
	agent.RegisterAgentServer(server, s)

	reflection.Register(server)

	log.Println("start agent")
	done := make(chan struct{})
	go func() {
		server.Serve(listenPort)
		done <- struct{}{}
	}()

	select {
	case <-s.stopCh:
		log.Println("stop agent gracefully")
		server.GracefulStop()
		select {
		case <-done:
			log.Println("stopped agent successfully")
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

func authFunc(ctx context.Context) (context.Context, error) {
	signed, err := grpc_auth.AuthFromMD(ctx, "Bearer")
	if err != nil {
		return ctx, fmt.Errorf("failed to get authorization token: %w", err)
	}
	if _, err := authorizeAgentJWT([]byte(signed)); err != nil {
		return ctx, err
	}
	return ctx, nil
}
