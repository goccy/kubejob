# kubejob

CLI and Go library for Kubernetes Job

# Installation

```bash
$ go get github.com/goccy/kubejob/cmd/kubejob
```

# How to use kubejob CLI

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

2020/06/27 00:23:05 Usage:
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
$ kubejob --image golang:1.14.4 -- go version
go version go1.14.4 linux/amd64
```

# How to use as a library

```go
package main

import (
 "github.com/goccy/kubejob"
)

func main() {
  clientset, _ := kubernetes.NewForConfig(config)
  job, _ := kubejob.NewJobBuilder(clientset, "default").
      SetImage("golang:1.14.4").
      SetCommand([]string{"go", "version"}).
      Build()
  job.Run(context.Background()) // start job and waiting for
}
```
