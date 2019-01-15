package executor

import (
	"encoding/json"
	"fmt"
	"os/exec"

	"github.com/golang/protobuf/ptypes"
	hclog "github.com/hashicorp/go-hclog"
	plugin "github.com/hashicorp/go-plugin"
	"github.com/hashicorp/nomad/drivers/shared/executor/proto"
	"github.com/hashicorp/nomad/helper/discover"
	"github.com/hashicorp/nomad/plugins/base"
)

const (
	// ExecutorDefaultMaxPort is the default max port used by the executor for
	// searching for an available port
	ExecutorDefaultMaxPort = 14512

	// ExecutorDefaultMinPort is the default min port used by the executor for
	// searching for an available port
	ExecutorDefaultMinPort = 14000
)

// CreateExecutor launches an executor plugin and returns an instance of the
// Executor interface
func CreateExecutor(logger hclog.Logger, driverConfig *base.ClientDriverConfig,
	executorConfig *ExecutorConfig) (Executor, *plugin.Client, error) {

	c, err := json.Marshal(executorConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create executor config: %v", err)
	}
	bin, err := discover.NomadExecutable()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to find the nomad binary: %v", err)
	}

	config := &plugin.ClientConfig{
		HandshakeConfig:  base.Handshake,
		Plugins:          map[string]plugin.Plugin{"executor": &ExecutorPlugin{}},
		Cmd:              exec.Command(bin, "executor", string(c)),
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		Logger:           logger.Named("executor"),
	}

	if driverConfig != nil {
		config.MaxPort = driverConfig.ClientMaxPort
		config.MinPort = driverConfig.ClientMinPort
	} else {
		config.MaxPort = ExecutorDefaultMaxPort
		config.MinPort = ExecutorDefaultMinPort
	}

	// setting the setsid of the plugin process so that it doesn't get signals sent to
	// the nomad client.
	if config.Cmd != nil {
		isolateCommand(config.Cmd)
	}

	executorClient := plugin.NewClient(config)
	rpcClient, err := executorClient.Client()
	if err != nil {
		return nil, nil, fmt.Errorf("error creating rpc client for executor plugin: %v", err)
	}

	raw, err := rpcClient.Dispense("executor")
	if err != nil {
		return nil, nil, fmt.Errorf("unable to dispense the executor plugin: %v", err)
	}
	executorPlugin := raw.(Executor)
	return executorPlugin, executorClient, nil
}

// CreateExecutorWithConfig launches a plugin with a given plugin config
func CreateExecutorWithConfig(reattachConfig *plugin.ReattachConfig, logger hclog.Logger) (Executor, *plugin.Client, error) {
	config := &plugin.ClientConfig{
		HandshakeConfig: base.Handshake,
		Reattach:        reattachConfig,
		Plugins:         map[string]plugin.Plugin{"executor": &ExecutorPlugin{}},

		// TODO: Use versioned plugin map to support backwards compatibility with
		// existing pre-0.9 executors
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		Logger:           logger.Named("executor"),
	}

	executorClient := plugin.NewClient(config)
	rpcClient, err := executorClient.Client()
	if err != nil {
		return nil, nil, fmt.Errorf("error creating rpc client for executor plugin: %v", err)
	}

	raw, err := rpcClient.Dispense("executor")
	if err != nil {
		return nil, nil, fmt.Errorf("unable to dispense the executor plugin: %v", err)
	}
	executorPlugin, ok := raw.(Executor)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected executor rpc type: %T", raw)
	}
	return executorPlugin, executorClient, nil
}

func processStateToProto(ps *ProcessState) (*proto.ProcessState, error) {
	timestamp, err := ptypes.TimestampProto(ps.Time)
	if err != nil {
		return nil, err
	}
	pb := &proto.ProcessState{
		Pid:      int32(ps.Pid),
		ExitCode: int32(ps.ExitCode),
		Signal:   int32(ps.Signal),
		Time:     timestamp,
	}

	return pb, nil
}

func processStateFromProto(pb *proto.ProcessState) (*ProcessState, error) {
	timestamp, err := ptypes.Timestamp(pb.Time)
	if err != nil {
		return nil, err
	}

	return &ProcessState{
		Pid:      int(pb.Pid),
		ExitCode: int(pb.ExitCode),
		Signal:   int(pb.Signal),
		Time:     timestamp,
	}, nil
}
