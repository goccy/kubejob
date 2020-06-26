# kubejob

CLI and Go library for Kubernetes Job

# Installation

```bash
$ go get github.com/goccy/kubejob/cmd/kubejob
```

# How to use CLI

```
Usage:
  kubejob [OPTIONS]

Application Options:
  -n, --namespace=  specify namespace (default: default)
  -i, --image=      specify container image
      --in-cluster  specify whether in cluster
  -c, --config=     specify local kubeconfig path. ( default: $HOME/.kube/config )
  -j, --job=        specify yaml or json file for written job definition

Help Options:
  -h, --help        Show this help message
```

## Example

```bash
$ kubejob --image golang:1.14 -- go version
go version go1.14.4 linux/amd64
```

# How to use as a library

```go
  clientset, _ := kubernetes.NewForConfig(config)
  job, _ := kubejob.NewJobBuilder(clientset, "default").
      SetImage("golang:1.14").
      SetCommand([]string{"go", "version"}).
      Build()
  job.Run(context.Background()) // start job and waiting for
```
