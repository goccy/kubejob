# kubejob

[![PkgGoDev](https://pkg.go.dev/badge/github.com/goccy/kubejob)](https://pkg.go.dev/github.com/goccy/kubejob)
![Go](https://github.com/goccy/kubejob/workflows/test/badge.svg)
[![codecov](https://codecov.io/gh/goccy/kubejob/branch/master/graph/badge.svg)](https://codecov.io/gh/goccy/kubejob)

A library for managing Kubernetes Job in Go

# Features

- Creating a Kubernetes Job
- Run and wait for Kubernetes Job
- Can capture logs of Kubernetes Job
- Can use `context.Context` to run Kubernetes Job
- Delayed execution of Kubernetes Job ( also support Sidecar pattern )
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
  apiv1 "k8s.io/api/core/v1"
  "k8s.io/client-go/rest"
)

var cfg *rest.Config // = rest.InClusterConfig() assign *rest.Config value to cfg

job, err := kubejob.NewJobBuilder(cfg, "default").BuildWithJob(&batchv1.Job{
  Spec: batchv1.JobSpec{
    Template: apiv1.PodTemplateSpec{
      Spec: apiv1.PodSpec{
        Containers: []apiv1.Container{
          {
            Name:    "test",
            Image:   "golang:1.15",
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
  Spec: batchv1.JobSpec{
    Template: apiv1.PodTemplateSpec{
      Spec: apiv1.PodSpec{
        Containers: []apiv1.Container{
          {
            Name:    "main",
            Image:   "golang:1.15",
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


# Tools

## cmd/kubejob

### Installation

```bash
go get github.com/goccy/kubejob/cmd/kubejob
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
$ kubejob --image golang:1.14 -- go version
go version
go version go1.14.4 linux/amd64
```
