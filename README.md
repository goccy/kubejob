# kubejob

[![PkgGoDev](https://pkg.go.dev/badge/github.com/goccy/kubejob)](https://pkg.go.dev/github.com/goccy/kubejob)
![Go](https://github.com/goccy/kubejob/workflows/test/badge.svg)
[![codecov](https://codecov.io/gh/goccy/kubejob/branch/master/graph/badge.svg)](https://codecov.io/gh/goccy/kubejob)

A library for managing Kubernetes Job in Go

# Features

- Creates a Kubernetes Job
- Run and wait for Kubernetes Job
- Capture logs of Kubernetes Job
- Can use `context.Context` to run Kubernetes Job
- Delayed execution of Kubernetes Job ( also support Sidecar pattern )
- Can control execution order ( and timing ) of command for multiple containers at Job
- Copy any files or directory between local file system and container in Job
- Can insert any process before the process of the init container
- Automatically clean up Kubernetes Job

# Installation

```bash
$ go get github.com/goccy/kubejob
```

# Synopsis

## Simply create and run and wait Kubernetes Job

```go

import (
  "github.com/goccy/kubejob"
  batchv1 "k8s.io/api/batch/v1"
  corev1 "k8s.io/api/core/v1"
  metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
  "k8s.io/client-go/rest"
)

var cfg *rest.Config // = rest.InClusterConfig() assign *rest.Config value to cfg

job, err := kubejob.NewJobBuilder(cfg, "default").BuildWithJob(&batchv1.Job{
  ObjectMeta: metav1.ObjectMeta{
    GenerateName: "kubejob-",
  },
  Spec: batchv1.JobSpec{
    Template: corev1.PodTemplateSpec{
      Spec: corev1.PodSpec{
        Containers: []corev1.Container{
          {
            Name:    "test",
            Image:   "golang:1.21.0-bookworm",
            Command: []string{"echo", "hello"},
          },
        },
      },
    },
  },
})
if err != nil {
  panic(err)
}

// Run KubernetesJob and output log to stdout.
// If Job doesn't exit status by `0` , `Run()` returns error.
if err := job.Run(context.Background()); err != nil {
	panic(err)
}
```

## Manage execution timing

If you don't want to execute the Job immediately after the Pod is `Running` state, you can delay the execution timing.
Also, this feature can be used if the Job has multiple containers and you want to control their execution order.

```go
ctx := context.Background()
if err := job.RunWithExecutionHandler(ctx, func(executors []*kubejob.JobExecutor) error {
  // callback when the Pod is in Running status.
  // `executors` corresponds to the list of `Containers` specified in `JobSpec`
  for _, exec := range executors { 
    // `Exec()` executes the command
    out, err := exec.Exec()
    if err != nil {
      panic(err)
    }
    fmt.Println(string(out), err)
  }
  return nil
})
```

## Manage execution with Sidecar

```go
job, err := kubejob.NewJobBuilder(cfg, "default").BuildWithJob(&batchv1.Job{
  ObjectMeta: metav1.ObjectMeta{
    GenerateName: "kubejob-",
  },
  Spec: batchv1.JobSpec{
    Template: corev1.PodTemplateSpec{
      Spec: corev1.PodSpec{
        Containers: []corev1.Container{
          {
            Name:    "main",
            Image:   "golang:1.21.0-bookworm",
            Command: []string{"echo", "hello"},
          },
          {
            Name:    "sidecar",
            Image:   "nginx:latest",
            Command: []string{"nginx"},
          },
        },
      },
    },
  },
})
if err != nil {
  panic(err)
}
if err := job.RunWithExecutionHandler(context.Background(), func(executors []*kubejob.JobExecutor) error {
  for _, exec := range executors {
    if exec.Container.Name == "sidecar" {
      // `ExecAsync()` executes the command and doesn't wait finished.
      exec.ExecAsync()
    } else {
      out, err := exec.Exec()
      if err != nil {
        panic(err)
      }
      fmt.Pritln(string(out))
    }
  }
  return nil
})
```

## Execution with kubejob-agent

Normally, copying and executing commands using JobExecutor is performed using the shell or tar command in the container image and using the Kubernetes API ( `pods/exec` ).
However, if you need to copy large files or run a large number of commands and don't want to overload the Kubernetes API, or if you don't have a shell or tar command in your container image, you can use the `kubejob-agent` method.

kubejob-agent is a CLI tool that can be installed by the following methods.

```console
$ go install github.com/goccy/kubejob/cmd/kubejob-agent
```

Include this tool in your container image, create `kubejob.AgentConfig` with the path where `kubejob-agent` is located, and give it to `kubejob.Job` as follows:

```go
agentConfig, err := kubejob.NewAgentConfig(map[string]string{
  "container-name": filepath.Join("/", "bin", "kubejob-agent"),
})
if err != nil {
  return err
}
job.UseAgent(agentConfig)
```

This will switch from communication using the Kubernetes API to gRPC-based communication with kubejob-agent.
Communication with `kubejob-agent` is performed using JWT issued using the RSA Key issued each time Kubernetes Job is started, so requests cannot be sent directly to the container from other processes.

<img width="700px" src="https://user-images.githubusercontent.com/209884/172190425-26f17151-20e9-4e6b-857f-8b9ae230927a.png"/>

# Requirements

## Role

```yaml
kind: Role
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: kubejob
rules:
  - apiGroups:
      - batch
    resources:
      - jobs
    verbs:
      - create
      - delete
  - apiGroups:
      - ""
    resources:
      - pods
    verbs:
      - get
      - list
      - watch
      - delete
  - apiGroups:
      - ""
    resources:
      - pods/log
    verbs:
      - get
      - watch
  - apiGroups:
      - ""
    resources:
      - pods/exec
    verbs:
      - create # required when using kubejob.JobExecutor without kubejob-agent
```

# Tools

## cmd/kubejob

### Installation

```bash
go install github.com/goccy/kubejob/cmd/kubejob
```

```console
Usage:
  kubejob [OPTIONS]

Application Options:
  -n, --namespace= specify namespace (default: default)
  -f, --file=      specify yaml or json file for written job definition
  -i, --image=     specify container image

Help Options:
  -h, --help       Show this help message
```

### Example

```bash
$ kubejob --image golang:1.21.0-bookworm -- go version
go version
go version go1.21.0 linux/amd64
```
