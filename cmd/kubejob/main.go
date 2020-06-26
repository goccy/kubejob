package main

import (
	"context"
	"log"
	"os"
	"path/filepath"

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
	job, err := kubejob.NewJobBuilder(clientset, namespace).
		SetImage("golang:1.14.4").
		SetCommand([]string{"go", "version"}).
		Build()
	if err != nil {
		log.Fatalf("%+v", err)
	}
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
