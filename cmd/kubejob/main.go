package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/goccy/kubejob"
	"github.com/jessevdk/go-flags"
	"golang.org/x/xerrors"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type option struct {
	Namespace string `description:"specify namespace" short:"n" long:"namespace" default:"default"`
	Image     string `description:"specify container image" short:"i" long:"image"`
	InCluster bool   `description:"specify whether in cluster" long:"in-cluster"`
	Config    string `description:"specify local kubeconfig path. ( default: $HOME/.kube/config )" short:"c" long:"config"`
	Job       string `description:"specify yaml or json file for written job definition" short:"j" long:"job"`
}

func loadConfig(opt option) (*rest.Config, error) {
	if opt.InCluster {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return nil, xerrors.Errorf("failed to load config in cluster: %w", err)
		}
		return cfg, nil
	}
	p := opt.Config
	if p == "" {
		p = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", p)
	if err != nil {
		return nil, xerrors.Errorf("failed to load config from %s: %w", p, err)
	}
	return cfg, nil
}

func _main(args []string, opt option) error {
	if opt.Image == "" && opt.Job == "" {
		return xerrors.Errorf("image or job must be specified")
	}
	cfg, err := loadConfig(opt)
	if err != nil {
		return xerrors.Errorf("failed to load config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return xerrors.Errorf("failed to create clientset: %w", err)
	}
	var job *kubejob.Job
	if opt.Job != "" {
		file, err := os.Open(opt.Job)
		if err != nil {
			return xerrors.Errorf("failed to open file %s: %w", opt.Job, err)
		}
		j, err := kubejob.NewJobBuilder(clientset, opt.Namespace).BuildWithReader(file)
		if err != nil {
			return xerrors.Errorf("failed to build job: %w", err)
		}
		job = j
	} else {
		if len(args) == 0 {
			return xerrors.Errorf("command is required. please speficy after '--' section")
		}
		j, err := kubejob.NewJobBuilder(clientset, opt.Namespace).
			SetImage(opt.Image).
			SetCommand(args).
			Build()
		if err != nil {
			return xerrors.Errorf("failed to build job: %w", err)
		}
		job = j
	}
	if err := job.Run(context.Background()); err != nil {
		return xerrors.Errorf("failed to run job: %w", err)
	}
	return nil
}

func main() {
	var opt option
	parser := flags.NewParser(&opt, flags.Default)
	args, err := parser.Parse()
	if err != nil {
		return
	}
	if err := _main(args, opt); err != nil {
		fmt.Println(err)
	}
}
