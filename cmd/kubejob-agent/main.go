package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/goccy/kubejob"
	"github.com/jessevdk/go-flags"
)

type option struct {
	Port    uint16 `description:"specify listening port" long:"port" required:"true"`
	Timeout string `description:"specify timeout by Go's time.Duration format. see details: https://pkg.go.dev/time#ParseDuration" long:"timeout"`
}

func (o option) getTimeout() (time.Duration, error) {
	if o.Timeout == "" {
		return 0, nil
	}
	return time.ParseDuration(o.Timeout)
}

func _main(args []string, opt option) error {
	timeout, err := opt.getTimeout()
	if err != nil {
		return err
	}
	ctx := context.Background()
	if timeout != 0 {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer func() {
			fmt.Fprintf(os.Stderr, "kubejob-agent: timeout (%s): %s\n", opt.Timeout, err.Error())
			cancel()
		}()
		return runAgentServer(ctx, opt.Port)
	}
	return runAgentServer(ctx, opt.Port)
}

func runAgentServer(ctx context.Context, port uint16) error {
	agentServer := kubejob.NewAgentServer(port)
	return agentServer.Run(ctx)
}

func parseOpt() ([]string, option, error) {
	var opt option
	parser := flags.NewParser(&opt, flags.Default)
	args, err := parser.Parse()
	return args, opt, err
}

const (
	exitSuccess = 0
	exitFailure = 1
)

func main() {
	args, opt, err := parseOpt()
	if err != nil {
		flagsErr, ok := err.(*flags.Error)
		if !ok {
			fmt.Fprintf(os.Stderr, "kubejob-agent: unknown parsed option error: %T %v\n", err, err)
			os.Exit(exitFailure)
		}
		if flagsErr.Type == flags.ErrHelp {
			return
		}
		os.Exit(exitFailure)
	}
	if err := _main(args, opt); err != nil {
		fmt.Fprintf(os.Stderr, "kubejob-agent: %+v\n", err)
		os.Exit(exitFailure)
	}
	os.Exit(exitSuccess)
}
