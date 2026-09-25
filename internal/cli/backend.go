package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/backend"
	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/health"
	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/protocol"
)

const (
	backendModeManaged     = "managed"
	backendModeDevelopment = "development"
	// 关闭预算的取值范围与默认值按增补 1 C9 冻结；默认值待 AUTO-MAS 侧
	// 实测「MaaFW 任务运行中收到 close」的耗时后再议，本任务只加开关。
	backendShutdownTimeoutDefault = "5"
	backendShutdownTimeoutMin     = 1
	backendShutdownTimeoutMax     = 120
	// 受监督端口按增补 1 C12：缺省随模式而定（managed 36163 / development 36164），
	// 因此 flag 的默认值留空，由 parseBackendPort 在解析 --mode 之后给出。
	backendPortManagedDefault     = health.DefaultPort
	backendPortDevelopmentDefault = 36164
)

func backendSuperviseCommand(deps *deps) *cobra.Command {
	var mode string
	var repo string
	var shutdownTimeout string
	var port string
	command := &cobra.Command{
		Use:   "supervise",
		Short: "启动并监督后端进程",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			deps.exitCode = runBackendSuperviseSession(
				deps.ctx,
				deps,
				commandPath(cmd),
				protocol.StageBackendSpawn,
				func(ctx context.Context, emitter *protocol.Emitter, mailbox *backend.ControlMailbox, control *backendControl) (sessionSuccess, error) {
					if mode == "" {
						return sessionSuccess{}, &commandError{
							code:    protocol.CodeInvalidArgument,
							stage:   protocol.StageBackendSpawn,
							message: "必须显式指定后端运行模式",
							details: map[string]any{"field": "mode"},
							cause:   errors.New("backend mode is required"),
						}
					}
					if mode != backendModeManaged && mode != backendModeDevelopment {
						return sessionSuccess{}, &commandError{
							code:    protocol.CodeUnsupportedMode,
							stage:   protocol.StageBackendSpawn,
							message: "当前后端运行模式尚不受支持",
							details: map[string]any{"mode": mode},
							cause:   errors.New("backend mode is unsupported"),
						}
					}
					if mode == backendModeDevelopment {
						if strings.TrimSpace(repo) == "" {
							return sessionSuccess{}, &commandError{
								code:    protocol.CodeInvalidArgument,
								stage:   protocol.StageBackendSpawn,
								message: "开发模式必须指定源码目录",
								details: map[string]any{"field": "repo"},
								cause:   errors.New("development repository is required"),
							}
						}
						if !filepath.IsAbs(repo) {
							repo = filepath.Join(deps.options.cwd, repo)
						}
						repo = filepath.Clean(repo)
					} else if strings.TrimSpace(repo) != "" {
						return sessionSuccess{}, &commandError{
							code:    protocol.CodeInvalidArgument,
							stage:   protocol.StageBackendSpawn,
							message: "managed 模式不接受 --repo",
							details: map[string]any{"field": "repo", "mode": mode},
							cause:   errors.New("managed mode does not accept development repository"),
						}
					}
					shutdownBudget, err := parseBackendShutdownTimeout(shutdownTimeout)
					if err != nil {
						return sessionSuccess{}, err
					}
					supervisedPort, err := parseBackendPort(port, cmd.Flags().Changed("port"), mode)
					if err != nil {
						return sessionSuccess{}, err
					}
					service, err := deps.options.backendFactory(
						ctx,
						deps.global.layout,
						deps.io.Err,
						deps.options.clock,
						deps.global.mirrorPolicy,
					)
					if err != nil {
						return sessionSuccess{}, err
					}
					if service == nil {
						return sessionSuccess{}, &commandError{
							code:    protocol.CodeInternalError,
							stage:   protocol.StageBackendSpawn,
							message: "后端监督器初始化失败",
							details: map[string]any{},
							cause:   errors.New("backend service is nil"),
						}
					}
					pid := os.Getpid()
					if pid <= 0 {
						return sessionSuccess{}, &commandError{
							code:    protocol.CodeInternalError,
							stage:   protocol.StageBackendSpawn,
							message: "Runtime 进程身份不可用",
							details: map[string]any{},
							cause:   errors.New("runtime pid is invalid"),
						}
					}
					if err := service.Supervise(ctx, backend.Request{
						OperationID:        emitter.OperationID(),
						RuntimePID:         uint32(pid),
						Mode:               backend.Mode(mode),
						DevelopmentRepo:    repo,
						ShutdownTimeout:    shutdownBudget,
						Port:               supervisedPort,
						PortExplicit:       cmd.Flags().Changed("port"),
						Emitter:            &backendEventEmitter{emitter: emitter, control: control},
						Control:            mailbox,
						BeforeShutdown:     mailbox.BeforeShutdown,
						BeforeControlClose: control.BeforeControlClose,
					}); err != nil {
						return sessionSuccess{}, err
					}
					return sessionSuccess{
						message: "后端监督已停止",
						details: map[string]any{},
						status:  string(protocol.StateStopped),
					}, nil
				},
			)
			return nil
		},
	}
	command.Flags().StringVar(&mode, "mode", "", "后端运行模式：managed 或 development")
	command.Flags().StringVar(&repo, "repo", "", "development 模式源码目录")
	command.Flags().StringVar(
		&shutdownTimeout,
		"shutdown-timeout",
		backendShutdownTimeoutDefault,
		"关闭后端的等待上限（秒），取值 1~120",
	)
	command.Flags().StringVar(
		&port,
		"port",
		"",
		"受监督后端监听端口，取值 1024~65535；缺省 managed 36163、development 36164",
	)
	return command
}

// parseBackendPort 校验 --port（增补 1 C12）：整数、合法范围 1024~65535，未显式给出时
// 按模式取缺省；显式给出的空串与非整数一样拒绝。与 --shutdown-timeout 同理收 string
// 自行解析，让非整数与越界共用 INVALID_ARGUMENT 的 result 语义，而不是落到 Cobra 的
// stderr 诊断通道。
func parseBackendPort(raw string, explicit bool, mode string) (int, error) {
	if !explicit {
		if mode == backendModeDevelopment {
			return backendPortDevelopmentDefault, nil
		}
		return backendPortManagedDefault, nil
	}
	reject := func(cause error) error {
		return &commandError{
			code:    protocol.CodeInvalidArgument,
			stage:   protocol.StageBackendSpawn,
			message: "受监督端口必须是 1024 到 65535 之间的整数",
			details: map[string]any{"field": "port", "value": raw},
			cause:   cause,
		}
	}
	port, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, reject(errors.New("backend port is not an integer"))
	}
	if !health.ValidPort(port) {
		return 0, reject(errors.New("backend port is out of range"))
	}
	return port, nil
}

// parseBackendShutdownTimeout 校验 --shutdown-timeout（增补 1 C9）：正整数秒、
// 合法范围 1~120。这里刻意收 string 而不是让 pflag 收 int——pflag 的整数解析失败
// 发生在 Cobra 解析阶段，只会走 stderr 诊断通道，产不出 INVALID_ARGUMENT 的
// result 事件；自行解析才能让越界与非整数共用同一条失败语义。
func parseBackendShutdownTimeout(raw string) (time.Duration, error) {
	reject := func(cause error) error {
		return &commandError{
			code:    protocol.CodeInvalidArgument,
			stage:   protocol.StageBackendSpawn,
			message: "关闭超时必须是 1 到 120 之间的整数秒",
			details: map[string]any{"field": "shutdown-timeout", "value": raw},
			cause:   cause,
		}
	}
	seconds, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, reject(errors.New("backend shutdown timeout is not an integer"))
	}
	if seconds < backendShutdownTimeoutMin || seconds > backendShutdownTimeoutMax {
		return 0, reject(errors.New("backend shutdown timeout is out of range"))
	}
	return time.Duration(seconds) * time.Second, nil
}

func runBackendSuperviseSession(
	ctx context.Context,
	deps *deps,
	command string,
	stage protocol.Stage,
	run func(context.Context, *protocol.Emitter, *backend.ControlMailbox, *backendControl) (sessionSuccess, error),
) (exitCode int) {
	runtimeVersion, versionPanicked, versionPanicFrames := helloRuntimeVersionSafely(ctx, deps.options.versionSource)
	telemetryState := newSessionTelemetryState(deps, command, stage, runtimeVersion, versionPanicked, versionPanicFrames)
	var emitter *protocol.Emitter
	defer func() {
		if recover() != nil {
			failure := unexpectedPanicError(stage)
			telemetryState.addPanic(failure, capturePanicFrames())
			if emitter == nil {
				writeDiagnostic(deps.io, errors.New("unexpected panic"))
				exitCode = protocol.ExitCodePreconditionFailed
			} else {
				exitCode, telemetryState.terminalWritten = safeEmitPanicFailure(deps, emitter, stage, failure)
			}
		}
		telemetryState.finish()
	}()

	output, err := newProcessOutput(deps)
	if err != nil {
		return sessionSetupFailure(deps, err)
	}
	emitter, err = output.NewEmitter(
		runtimeVersion,
		command,
		// backend supervise 的 ControlReader 注册了 cancel/shutdown/status 三条命令，
		// 公告必须与之一致：调用方按契约只信 hello.capabilities。
		[]string{
			string(protocol.CapabilityStdinCancel),
			string(protocol.CapabilityStateV1),
			string(protocol.CapabilityLogStream),
			string(protocol.CapabilityStdinShutdown),
			string(protocol.CapabilityStdinStatus),
		},
		protocol.WithClock(deps.options.clock),
	)
	if err != nil {
		return sessionSetupFailure(deps, err)
	}
	telemetryState.sessionStarted = true
	if err := ctx.Err(); err != nil {
		telemetryState.operationErr = err
		exitCode, telemetryState.terminalWritten = emitFailure(deps, emitter, stage, err)
		return exitCode
	}
	operationContext, cancel := context.WithCancel(ctx)
	defer cancel()
	readerContext, cancelReader := context.WithCancel(operationContext)
	defer cancelReader()
	mailbox := backend.NewControlMailbox(64)
	control := newBackendControl(mailbox, stage, func(command protocol.ControlCommand) error {
		return mailbox.Submit(readerContext, command)
	})
	mailbox.SetStageCallback(control.SetStage)
	reader, err := protocol.NewControlReader(
		deps.io.In,
		emitter,
		control,
		protocol.ControlCancel,
		protocol.ControlShutdown,
		protocol.ControlStatus,
	)
	if err != nil {
		failure := backendControlInfrastructureError(stage, err)
		telemetryState.operationErr = failure
		exitCode, telemetryState.terminalWritten = emitFailure(deps, emitter, stage, failure)
		return exitCode
	}
	mailbox.SetBeforeShutdown(func(string) {
		cancelReader()
		mailbox.StopAccepting()
		reader.StopAccepting()
	})
	control.SetBeforeControlClose(func() {
		cancelReader()
		mailbox.StopAccepting()
		reader.StopAccepting()
	})
	controlDone := make(chan error, 1)
	go func() {
		readErr := runControlReaderSafely(readerContext, reader)
		switch {
		case readErr == nil:
			// 增补 1 C13：hello 之后 stdin 到达 EOF 即宿主断开，视为隐式 shutdown；
			// 被 StopAccepting 或 ctx 停止的 reader 不满足 InputClosed，不会误触发。
			if reader.InputClosed() {
				control.SubmitImplicitShutdown()
			}
		case isWorkspaceControlContextCancellation(readerContext, readErr):
		default:
			if _, panicked := recoveredControlReaderPanic(readErr); panicked {
				// reader 自身崩溃是 Runtime 缺陷，仍走基础设施故障路径并上报。
				control.SetReaderError(readErr)
				break
			}
			// C13：读取出错与 EOF 同样视为宿主断开——走优雅关闭对用户只会更好，
			// 硬失败路径反而会把后端 Job 硬杀；错误本身保留在 stderr 诊断里。
			writeDiagnostic(deps.io, fmt.Errorf("stdin control read failed, treating as host disconnect: %w", readErr))
			control.SubmitImplicitShutdown()
			readErr = nil
		}
		controlDone <- readErr
	}()
	success, runErr, panicked, panicFrames := invokeSessionRun(stage, func() (sessionSuccess, error) {
		return run(operationContext, emitter, mailbox, control)
	})
	cancelReader()
	reader.StopAccepting()
	mailbox.StopAccepting()
	cancel()
	stopErr := stopWorkspaceControl(reader, deps.io.In, controlDone)
	mailbox.Close()
	if stopErr != nil {
		runErr = joinWorkspaceControlError(runErr, backendControlInfrastructureError(control.CurrentControlStage(), stopErr))
	}
	if readerErr := control.ReaderError(); readerErr != nil {
		runErr = joinWorkspaceControlError(runErr, backendControlInfrastructureError(control.CurrentControlStage(), readerErr))
	}
	if commandID := control.CommandID(); commandID != "" {
		if runErr != nil {
			runErr = addControlCommandID(runErr, commandID)
		} else {
			success.details = protocol.WithControlCommandID(success.details, commandID)
		}
	}
	if runErr != nil {
		telemetryState.operationErr = runErr
		if panicked {
			telemetryState.addPanic(runErr, panicFrames)
		}
		exitCode, telemetryState.terminalWritten = emitFailure(deps, emitter, stage, runErr)
		return exitCode
	}
	exitCode, telemetryState.terminalWritten = emitSuccess(deps, emitter, stage, success)
	if exitCode != protocol.ExitCodeSuccess {
		telemetryState.operationErr = telemetryOutputFailure(stage)
	}
	return exitCode
}

type backendControl struct {
	mu                 sync.RWMutex
	mailbox            *backend.ControlMailbox
	submit             func(protocol.ControlCommand) error
	stage              protocol.Stage
	commandID          string
	readerErr          error
	beforeControlClose func()
}

func (c *backendControl) SetBeforeControlClose(callback func()) {
	c.mu.Lock()
	c.beforeControlClose = callback
	c.mu.Unlock()
}

func (c *backendControl) BeforeControlClose() {
	c.mu.RLock()
	callback := c.beforeControlClose
	c.mu.RUnlock()
	if callback != nil {
		callback()
	}
}

func newBackendControl(mailbox *backend.ControlMailbox, stage protocol.Stage, submit func(protocol.ControlCommand) error) *backendControl {
	return &backendControl{mailbox: mailbox, submit: submit, stage: stage}
}

func (c *backendControl) PrepareControl(command protocol.ControlCommand) (protocol.ControlDisposition, protocol.ControlAction, error) {
	return protocol.ControlAccepted, func() error {
		if err := c.submit(command); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return expectedControlCancellation{cause: err}
			}
			return err
		}
		if command.Command == protocol.ControlCancel || command.Command == protocol.ControlShutdown {
			c.mu.Lock()
			if c.commandID == "" {
				c.commandID = command.CommandID
			}
			c.mu.Unlock()
		}
		return nil
	}, nil
}

// SubmitImplicitShutdown 把宿主断开（stdin EOF 或读取出错）翻译成一条没有 commandId 的
// shutdown 投进同一个 mailbox（增补 1 C13）。已有终止命令、mailbox 已停止或操作已收口时
// Submit 返回错误，这正是隐式关闭的幂等语义，因此安全忽略；result 因 commandId 为空
// 而不回显 controlCommandId。
func (c *backendControl) SubmitImplicitShutdown() {
	// 忽略 ErrControlStopped / ErrControlMailboxClosed / ctx 错误：它们都表示关闭已在路上。
	_ = c.submit(protocol.ControlCommand{Protocol: protocol.Version, Command: protocol.ControlShutdown})
}

// StopAfterShutdown 保留 cancel-first 后续命令进入 mailbox 的机会；shutdown-first
// 仍沿用协议默认的 reader 停止语义。
func (c *backendControl) StopAfterShutdown(protocol.ControlCommand) bool {
	if c == nil || c.mailbox == nil {
		return true
	}
	if terminal, ok := c.mailbox.TerminalCommand(); ok && terminal.Command == protocol.ControlCancel {
		return false
	}
	return true
}

type expectedControlCancellation struct{ cause error }

func (e expectedControlCancellation) Error() string { return e.cause.Error() }

func (e expectedControlCancellation) Unwrap() error { return e.cause }

func (c *backendControl) CurrentControlStage() protocol.Stage {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stage
}

func (c *backendControl) SetStage(stage protocol.Stage) {
	c.mu.Lock()
	c.stage = stage
	c.mu.Unlock()
}

func (c *backendControl) CommandID() string {
	c.mu.RLock()
	commandID := c.commandID
	c.mu.RUnlock()
	if commandID != "" {
		return commandID
	}
	if command, ok := c.mailbox.TerminalCommand(); ok {
		return command.CommandID
	}
	return ""
}

func (c *backendControl) SetReaderError(err error) {
	c.mailbox.SetReaderError(err)
	c.mu.Lock()
	c.readerErr = err
	c.mu.Unlock()
}

func (c *backendControl) ReaderError() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.readerErr
}

func backendControlInfrastructureError(stage protocol.Stage, cause error) error {
	return &commandError{
		code:                  protocol.CodeInternalError,
		stage:                 stage,
		message:               "stdin 控制通道读取失败",
		details:               map[string]any{},
		cause:                 cause,
		controlInfrastructure: true,
	}
}

type backendEventEmitter struct {
	emitter *protocol.Emitter
	control *backendControl
}

func (e *backendEventEmitter) EmitState(event protocol.StateEvent) error {
	e.control.SetStage(event.Stage)
	return e.emitter.EmitState(event)
}

func (e *backendEventEmitter) EmitLog(event protocol.LogEvent) error {
	return e.emitter.EmitLog(event)
}

func (e *backendEventEmitter) EmitWarning(event protocol.WarningEvent) error {
	return e.emitter.EmitWarning(event)
}
