package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/goccy/kubejob"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/tools/clientcmd"
)

func runJob(namespace string) error {

	home := os.Getenv("HOME")
	kubeconfig := filepath.Join(home, ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return err
	}
	/*
		// creates the in-cluster config
		config, err := rest.InClusterConfig()
		if err != nil {
			return err
		}
	*/
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	yml := `
apiVersion: batch/v1
kind: Job
metadata:
  name: goversion-job
spec:
  backoffLimit: 0
  template:
    metadata:
      name: goversion-pod
      labels:
        test: hoge
    spec:
      restartPolicy: Never
      containers:
      - name: go-version
        image: golang:1.14.4
        command: ["go", "version"]
`
	job := kubejob.NewJobBuilder(clientset, namespace).BuildWithReader(strings.NewReader(yml))

	if err := job.Run(context.Background()); err != nil {
		log.Fatalf("%+v", err)
	}
	return nil
}

func main() {
	if err := runJob("default"); err != nil {
		log.Fatalf("%+v", err)
	}
}
