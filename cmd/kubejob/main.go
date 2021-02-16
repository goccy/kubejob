package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/goccy/kubejob"
	"github.com/jessevdk/go-flags"
	"golang.org/x/xerrors"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

type option struct {
	Namespace string `description:"specify namespace" short:"n" long:"namespace" default:"default"`
	File      string `description:"specify yaml or json file for written job definition" short:"f" long:"file"`
	Image     string `description:"specify container image" short:"i" long:"image"`
}

func getKubeConfig() string {
	if v := os.Getenv("KUBECONFIG"); v != "" {
		return v
	}
	home := homedir.HomeDir()
	config := filepath.Join(home, ".kube", "config")
	if _, err := os.Stat(config); err == nil {
		return config
	}
	return ""
}

func loadConfig() (*rest.Config, error) {
	cfg, err := clientcmd.BuildConfigFromFlags("", getKubeConfig())
	if err != nil {
		return nil, xerrors.Errorf("failed to create config: %w", err)
	}
	return cfg, nil
}

func namespace(opt option) (string, error) {
	if opt.Namespace != "" {
		return opt.Namespace, nil
	}
	rules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: getKubeConfig()}
	c, err := rules.Load()
	if err != nil {
		return "", xerrors.Errorf("failed to load default namespace: %w", err)
	}
	return c.Contexts[c.CurrentContext].Namespace, nil
}

func _main(args []string, opt option) error {
	if opt.Image == "" && opt.File == "" {
		return xerrors.Errorf("image or file option must be specified")
	}
	cfg, err := loadConfig()
	if err != nil {
		return xerrors.Errorf("failed to load config: %w", err)
	}
	ns, err := namespace(opt)
	if err != nil {
		return xerrors.Errorf("failed to get namespace: %w", err)
	}
	var job *kubejob.Job
	if opt.File != "" {
		file, err := os.Open(opt.File)
		if err != nil {
			return xerrors.Errorf("failed to open file %s: %w", opt.File, err)
		}
		j, err := kubejob.NewJobBuilder(cfg, ns).BuildWithReader(file)
		if err != nil {
			return xerrors.Errorf("failed to build job: %w", err)
		}
		job = j
	} else {
		if len(args) == 0 {
			return xerrors.Errorf("command is required. please speficy after '--' section")
		}
		j, err := kubejob.NewJobBuilder(cfg, ns).
			SetImage(opt.Image).
			SetCommand(args).
			Build()
		if err != nil {
			return xerrors.Errorf("failed to build job: %w", err)
		}
		job = j
	}

	ctx, cancel := context.WithCancel(context.Background())
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case s := <-interrupt:
			fmt.Printf("receive %s. try to graceful stop\n", s)
			cancel()
		}
	}()

	if err := job.Run(ctx); err != nil {
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
		fmt.Printf("%+v", err)
	}
}
