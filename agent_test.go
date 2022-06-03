package kubejob_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/goccy/kubejob"
	"github.com/goccy/kubejob/agent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	startAllocationPort = 5000
)

func createGRPCConn(t *testing.T, signedToken string) grpc.ClientConnInterface {
	t.Helper()
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", startAllocationPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithUnaryInterceptor(kubejob.AgentAuthUnaryInterceptor(signedToken)),
		grpc.WithStreamInterceptor(kubejob.AgentAuthStreamInterceptor(signedToken)),
	)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestAgentServer(t *testing.T) {
	agentConfig, err := kubejob.NewAgentConfig("")
	if err != nil {
		t.Fatal(err)
	}
	token, err := agentConfig.IssueJWT()
	if err != nil {
		t.Fatal(err)
	}
	publicKeyEnv := agentConfig.PublicKeyEnv()
	if err := os.Setenv(publicKeyEnv.Name, publicKeyEnv.Value); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Unsetenv(publicKeyEnv.Name); err != nil {
			t.Fatal(err)
		}
	}()
	signedToken := string(token)
	t.Run("finish", func(t *testing.T) {
		agentServer := kubejob.NewAgentServer(startAllocationPort)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		done := make(chan struct{})

		go func() {
			if err := agentServer.Run(ctx); err != nil {
				t.Fatal(err)
			}
			done <- struct{}{}
		}()

		agentClient := agent.NewAgentClient(createGRPCConn(t, signedToken))
		if _, err := agentClient.Finish(context.Background(), &agent.FinishRequest{}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("exec", func(t *testing.T) {
		agentServer := kubejob.NewAgentServer(startAllocationPort)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		done := make(chan struct{})

		go func() {
			if err := agentServer.Run(ctx); err != nil {
				t.Fatal(err)
			}
			done <- struct{}{}
		}()

		agentClient := agent.NewAgentClient(createGRPCConn(t, signedToken))

		for _, test := range []struct {
			name             string
			command          []string
			env              []*agent.Env
			expectedSuccess  bool
			expectedOutput   string
			expectedError    string
			expectedExitCode int32
		}{
			{
				name:    "success",
				command: []string{"sh", "-c", "echo $FOO; echo $BAR;"},
				env: []*agent.Env{
					{Name: "FOO", Value: "foo"},
					{Name: "BAR", Value: "bar"},
				},
				expectedSuccess: true,
				expectedOutput:  "foo\nbar\n",
			},
			{
				name:            "error",
				command:         []string{"sh", "-c", "ls invalid-file"},
				expectedSuccess: false,
				expectedOutput:  "No such file or directory\n",
				expectedError:   "exit status 1",
			},
		} {
			test := test
			t.Run(test.name, func(t *testing.T) {
				execResponse, err := agentClient.Exec(context.Background(), &agent.ExecRequest{
					Command: test.command,
					Env:     test.env,
				})
				if err != nil {
					t.Fatal(err)
				}
				if test.expectedSuccess {
					if !execResponse.Success {
						t.Fatalf("expected success but got error. %+v", execResponse)
					}
					if execResponse.ExitCode != 0 {
						t.Fatalf("failed to get exitCode. expected 0 but got %d", execResponse.ExitCode)
					}
				} else {
					if execResponse.Success {
						t.Fatalf("expected failure but got success. %+v", execResponse)
					}
					if execResponse.ExitCode == 0 {
						t.Fatal("failed to get exitCode")
					}
				}
				if !strings.Contains(execResponse.Output, test.expectedOutput) {
					t.Fatalf("failed to captured output. expected %q but got %q", test.expectedOutput, execResponse.Output)
				}
				if len(test.expectedError) > 0 {
					if execResponse.ErrorMessage == "" {
						t.Fatalf("failed to captured error. expected %q but got empty", test.expectedError)
					}
				}
			})
		}
		if _, err := agentClient.Finish(context.Background(), &agent.FinishRequest{}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("copyFrom", func(t *testing.T) {
		agentServer := kubejob.NewAgentServer(startAllocationPort)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		done := make(chan struct{})

		go func() {
			if err := agentServer.Run(ctx); err != nil {
				t.Fatal(err)
			}
			done <- struct{}{}
		}()

		// create temporary file to copy
		srcFile, err := os.CreateTemp("", "repo")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(srcFile.Name())
		t.Logf("src file path: %s", srcFile.Name())

		contentSize := 1024*1024 + 10 // 1MB + ext
		srcContent := bytes.Repeat([]byte{'a'}, contentSize)
		if _, err := srcFile.Write(srcContent); err != nil {
			t.Fatal(err)
		}
		if err := srcFile.Close(); err != nil {
			t.Fatal(err)
		}

		agentClient := agent.NewAgentClient(createGRPCConn(t, signedToken))
		stream, err := agentClient.CopyFrom(context.Background(), &agent.CopyFromRequest{
			Path: srcFile.Name(),
		})
		if err != nil {
			t.Fatal(err)
		}

		dstFile, err := os.CreateTemp("", "repo")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(dstFile.Name())
		t.Logf("dst file path: %s", dstFile.Name())

		for {
			copyFromResponse, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := dstFile.Write(copyFromResponse.Data); err != nil {
				t.Fatal(err)
			}
		}
		if err := dstFile.Close(); err != nil {
			t.Fatal(err)
		}

		finfo, err := os.Stat(dstFile.Name())
		if err != nil {
			t.Fatal(err)
		}
		if finfo.Size() != int64(contentSize) {
			t.Fatalf("failed to copy from: expected size %d but got %d", contentSize, finfo.Size())
		}
		dstContent, err := os.ReadFile(dstFile.Name())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(srcContent, dstContent) {
			t.Fatalf("failed to copy content")
		}
		if _, err := agentClient.Finish(context.Background(), &agent.FinishRequest{}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("copyTo", func(t *testing.T) {
		agentServer := kubejob.NewAgentServer(startAllocationPort)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		done := make(chan struct{})

		go func() {
			if err := agentServer.Run(ctx); err != nil {
				t.Fatal(err)
			}
			done <- struct{}{}
		}()

		// create temporary file to copy
		srcFile, err := os.CreateTemp("", "repo")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(srcFile.Name())
		t.Logf("src file path: %s", srcFile.Name())

		contentSize := 1024*1024 + 10 // 1MB + ext
		srcContent := bytes.Repeat([]byte{'a'}, contentSize)
		if _, err := srcFile.Write(srcContent); err != nil {
			t.Fatal(err)
		}
		if err := srcFile.Close(); err != nil {
			t.Fatal(err)
		}

		dstFilePath := fmt.Sprintf("%s-copied", srcFile.Name())
		agentClient := agent.NewAgentClient(createGRPCConn(t, signedToken))
		stream, err := agentClient.CopyTo(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		// send file content
		buf := make([]byte, 1024)
		f, err := os.Open(srcFile.Name())
		if err != nil {
			t.Fatal(err)
		}
		stream.Send(&agent.CopyToRequest{
			Path: dstFilePath,
		})
		for {
			n, err := f.Read(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			stream.Send(&agent.CopyToRequest{
				Data: buf[:n],
			})
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if resp.CopiedLength != int64(contentSize) {
			t.Fatalf("failed to copy. expected %d but got %d", contentSize, resp.CopiedLength)
		}

		finfo, err := os.Stat(dstFilePath)
		if err != nil {
			t.Fatal(err)
		}
		if finfo.Size() != int64(contentSize) {
			t.Fatalf("failed to copy from: expected size %d but got %d", contentSize, finfo.Size())
		}
		dstContent, err := os.ReadFile(dstFilePath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(srcContent, dstContent) {
			t.Fatalf("failed to copy content")
		}
		if _, err := agentClient.Finish(context.Background(), &agent.FinishRequest{}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				t.Fatal(err)
			}
		}
	})
}
