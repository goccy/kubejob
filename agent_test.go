package kubejob_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/goccy/kubejob"
	"github.com/goccy/kubejob/agent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
)

const (
	startAllocationPort = uint16(5000)
)

func createGRPCConn(t *testing.T, signedToken string, port uint16) grpc.ClientConnInterface {
	t.Helper()
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", port),
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
	agentConfig, err := kubejob.NewAgentConfig(map[string]string{})
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
	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		agentServer := kubejob.NewAgentServer(startAllocationPort)
		if err := agentServer.Run(ctx); err == nil {
			t.Fatal("expected cancel error")
		}
	})
	t.Run("finish", func(t *testing.T) {
		port := startAllocationPort + 1
		agentServer := kubejob.NewAgentServer(port)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		done := make(chan error)

		go func() {
			done <- agentServer.Run(ctx)
		}()

		agentClient := agent.NewAgentClient(createGRPCConn(t, signedToken, port))
		if _, err := agentClient.Finish(context.Background(), &agent.FinishRequest{}); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("timeout", func(t *testing.T) {
		port := startAllocationPort + 2
		agentServer := kubejob.NewAgentServer(port)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		done := make(chan struct{})

		go func() {
			if err := agentServer.Run(ctx); err == nil {
				t.Fatal("expected timeout error")
			}
			done <- struct{}{}
		}()

		agentClient := agent.NewAgentClient(createGRPCConn(t, signedToken, port))
		go func() {
			if _, err := agentClient.Exec(context.Background(), &agent.ExecRequest{
				Command: []string{"sleep", "60"},
			}); err != nil {
				t.Fatal(err)
			}
		}()
		if _, err := agentClient.Finish(context.Background(), &agent.FinishRequest{}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-ctx.Done():
			if err := ctx.Err(); err == nil {
				t.Fatal("expected timeout error")
			}
		}
	})

	t.Run("exec", func(t *testing.T) {
		port := startAllocationPort + 3
		agentServer := kubejob.NewAgentServer(port)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		done := make(chan struct{})

		go func() {
			if err := agentServer.Run(ctx); err != nil {
				t.Fatal(err)
			}
			done <- struct{}{}
		}()

		agentClient := agent.NewAgentClient(createGRPCConn(t, signedToken, port))

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
		t.Run("file to file", func(t *testing.T) {
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

			srcFile, err := os.CreateTemp("", "repo")
			if err != nil {
				t.Fatal(err)
			}
			defer srcFile.Close()
			writeContent(t, srcFile)
			t.Logf("src file path: %s", srcFile.Name())

			dstDir, err := os.MkdirTemp("", "repo2")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dstDir)
			dstFilePath := filepath.Join(dstDir, filepath.Base(srcFile.Name()))
			t.Logf("dst file path: %s", dstFilePath)

			agentClient, err := kubejob.NewAgentClient(
				&corev1.Pod{Status: corev1.PodStatus{PodIP: "127.0.0.1"}},
				startAllocationPort,
				"",
				signedToken,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := agentClient.CopyFrom(ctx, srcFile.Name(), dstFilePath); err != nil {
				t.Fatal(err)
			}
			if err := agentClient.Stop(ctx); err != nil {
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
		t.Run("file to directory", func(t *testing.T) {
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

			srcFile, err := os.CreateTemp("", "repo")
			if err != nil {
				t.Fatal(err)
			}
			defer srcFile.Close()
			writeContent(t, srcFile)
			t.Logf("src file path: %s", srcFile.Name())

			dstDir, err := os.MkdirTemp("", "repo2")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dstDir)
			t.Logf("dst directory: %s", dstDir)

			agentClient, err := kubejob.NewAgentClient(
				&corev1.Pod{Status: corev1.PodStatus{PodIP: "127.0.0.1"}},
				startAllocationPort,
				"",
				signedToken,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := agentClient.CopyFrom(ctx, srcFile.Name(), dstDir); err != nil {
				t.Fatal(err)
			}
			if err := agentClient.Stop(ctx); err != nil {
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
		t.Run("directory to directory", func(t *testing.T) {
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

			srcDir := createTemporaryDirectory(t)
			defer os.RemoveAll(srcDir)
			t.Logf("src directory: %s", srcDir)

			dstDir, err := os.MkdirTemp("", "repo2")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dstDir)
			t.Logf("dst directory: %s", dstDir)

			agentClient, err := kubejob.NewAgentClient(
				&corev1.Pod{Status: corev1.PodStatus{PodIP: "127.0.0.1"}},
				startAllocationPort,
				"",
				signedToken,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := agentClient.CopyFrom(ctx, srcDir, dstDir); err != nil {
				t.Fatal(err)
			}
			validateTemporaryDirectory(t, filepath.Join(dstDir, filepath.Base(srcDir)))
			if err := agentClient.Stop(ctx); err != nil {
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
	})

	t.Run("copyTo", func(t *testing.T) {
		t.Run("file to file", func(t *testing.T) {
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

			srcFile, err := os.CreateTemp("", "repo")
			if err != nil {
				t.Fatal(err)
			}
			defer srcFile.Close()
			writeContent(t, srcFile)
			t.Logf("src file path: %s", srcFile.Name())

			dstDir, err := os.MkdirTemp("", "repo2")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dstDir)
			dstFilePath := filepath.Join(dstDir, filepath.Base(srcFile.Name()))
			t.Logf("dst file path: %s", dstFilePath)

			agentClient, err := kubejob.NewAgentClient(
				&corev1.Pod{Status: corev1.PodStatus{PodIP: "127.0.0.1"}},
				startAllocationPort,
				"",
				signedToken,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := agentClient.CopyTo(ctx, srcFile.Name(), dstFilePath); err != nil {
				t.Fatal(err)
			}
			if err := agentClient.Stop(ctx); err != nil {
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
		t.Run("file to directory", func(t *testing.T) {
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

			srcFile, err := os.CreateTemp("", "repo")
			if err != nil {
				t.Fatal(err)
			}
			defer srcFile.Close()
			writeContent(t, srcFile)
			t.Logf("src file path: %s", srcFile.Name())

			dstDir, err := os.MkdirTemp("", "repo2")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dstDir)
			t.Logf("dst directory: %s", dstDir)

			agentClient, err := kubejob.NewAgentClient(
				&corev1.Pod{Status: corev1.PodStatus{PodIP: "127.0.0.1"}},
				startAllocationPort,
				"",
				signedToken,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := agentClient.CopyTo(ctx, srcFile.Name(), dstDir); err != nil {
				t.Fatal(err)
			}
			if err := agentClient.Stop(ctx); err != nil {
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
		t.Run("directory to directory", func(t *testing.T) {
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

			srcDir := createTemporaryDirectory(t)
			defer os.RemoveAll(srcDir)
			t.Logf("src directory: %s", srcDir)

			dstDir, err := os.MkdirTemp("", "repo2")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dstDir)
			t.Logf("dst directory: %s", dstDir)

			agentClient, err := kubejob.NewAgentClient(
				&corev1.Pod{Status: corev1.PodStatus{PodIP: "127.0.0.1"}},
				startAllocationPort,
				"",
				signedToken,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := agentClient.CopyTo(ctx, srcDir, dstDir); err != nil {
				t.Fatal(err)
			}
			validateTemporaryDirectory(t, filepath.Join(dstDir, filepath.Base(srcDir)))
			if err := agentClient.Stop(ctx); err != nil {
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
	})
}

func writeContent(t *testing.T, f *os.File) {
	contentSize := 1024*1024 + 10 // 1MB + ext
	content := bytes.Repeat([]byte{'a'}, contentSize)
	if _, err := f.Write(content); err != nil {
		t.Fatal(err)
	}
}

func createTemporaryDirectory(t *testing.T) string {
	t.Helper()
	repoDir, err := os.MkdirTemp("", "repo")
	if err != nil {
		t.Fatal(err)
	}
	contentDir := filepath.Join(repoDir, "content")
	if err := os.Mkdir(contentDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		fileName := fmt.Sprintf("%d.txt", i)
		path := filepath.Join(contentDir, fileName)
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		writeContent(t, f)
		f.Close()
		if err := os.Symlink(path, filepath.Join(contentDir, fmt.Sprintf("%s.sym", fileName))); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(path, filepath.Join(repoDir, fmt.Sprintf("%s.sym", fileName))); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(contentDir, filepath.Join(repoDir, "content_sym")); err != nil {
		t.Fatal(err)
	}
	return repoDir
}

func validateTemporaryDirectory(t *testing.T, extractedDir string) {
	t.Helper()

	contentInfo, err := os.Stat(filepath.Join(extractedDir, "content"))
	if err != nil {
		t.Fatal(err)
	}
	if !contentInfo.IsDir() {
		t.Fatalf("failed to copy directory")
	}

	contentDirPath, err := os.Readlink(filepath.Join(extractedDir, "content_sym"))
	if err != nil {
		t.Fatal(err)
	}
	if contentDirPath != filepath.Join(extractedDir, "content") {
		t.Fatalf("failed to read symlink path: %s", contentDirPath)
	}

	for i := 0; i < 3; i++ {
		fileName := fmt.Sprintf("%d.txt", i)
		symlinkFileName := fmt.Sprintf("%s.sym", fileName)

		filePath := filepath.Join(extractedDir, "content", fileName)
		if _, err := os.Stat(filePath); err != nil {
			t.Fatal(err)
		}
		p, err := os.Readlink(filepath.Join(extractedDir, "content", symlinkFileName))
		if err != nil {
			t.Fatalf("failed to read symlink: %v", err)
		}
		if p != filePath {
			t.Fatalf("failed to read symlink path: %s", p)
		}
		p2, err := os.Readlink(filepath.Join(extractedDir, symlinkFileName))
		if err != nil {
			t.Fatalf("failed to read symlink: %v", err)
		}
		if p2 != filePath {
			t.Fatalf("failed to read symlink path: %s", p)
		}
	}
}
