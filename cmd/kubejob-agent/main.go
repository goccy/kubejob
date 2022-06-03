package main

import (
	"context"
	"fmt"
	"os"

	"github.com/goccy/kubejob"
	"github.com/jessevdk/go-flags"
)

type option struct {
	Port uint16 `description:"specify listening port" long:"port" required:"true"`
}

func _main(args []string, opt option) error {
	agentServer := kubejob.NewAgentServer(opt.Port)
	return agentServer.Run(context.Background())
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
		fmt.Fprintf(os.Stderr, "kubejob-agent: %+v", err)
		os.Exit(exitFailure)
	}
	os.Exit(exitSuccess)
}
