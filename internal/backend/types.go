package backend

import (
	"context"
	"time"

	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/health"
	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/mirror"
	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/process"
	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/protocol"
	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/state"
	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/uv"
)

// EventEmitter 是后端监督使用的窄事件出口；顶层 error/result 由 CLI 统一发出。
type EventEmitter interface {
	EmitState(protocol.StateEvent) error
	EmitLog(protocol.LogEvent) error
	EmitWarning(protocol.WarningEvent) error
}

// Request 描述一次受管后端监督请求。
type Request struct {
	OperationID     string
	RuntimePID      uint32
	Mode            Mode
	DevelopmentRepo string
	// SourceRepository 表示源码根以 managed 入口托管，仅允许 Runtime 根与源码根相同。
	SourceRepository bool
	// ShutdownTimeout 是从发出 POST /api/core/close 到进程退出的等待上限，
	// 超时才收 Job（增补 1 C9）。CLI 由 --shutdown-timeout 提供，取值 1~120 秒；
	// 为零或负数时回退 Dependencies.ShutdownTimeout。
	ShutdownTimeout time.Duration
	// Port 是受监督后端的监听端口（增补 1 C12）。CLI 由 --port 提供；为零时按模式
	// 取缺省（managed 36163 / development 36164），越界映射 INVALID_ARGUMENT。
	// 注入 uv 的 AUTO_MAS_SUPERVISED_PORT、健康检查地址、关闭地址与 baseUrl 全部由它派生。
	Port int
	// PortExplicit 区分用户显式指定的端口与模式默认端口；默认端口被占用时仅后者允许回退。
	PortExplicit       bool
	Emitter            EventEmitter
	Control            ControlReceiver
	BeforeShutdown     func(string)
	BeforeControlClose func()
}

// Mode 选择后端源码与环境的监督策略。
type Mode string

const (
	// ModeManaged 使用 Runtime 管理的仓库、环境和版本身份。
	ModeManaged Mode = "managed"
	// ModeDevelopment 只监督调用方显式指定的源码目录与既有 .venv。
	ModeDevelopment Mode = "development"
)

// ControlReceiver 为监督循环提供已由 ControlReader 校验并按 FIFO 排队的命令。
type ControlReceiver interface {
	Receive(context.Context) (protocol.ControlCommand, error)
}

// Dependencies 是 ManagedSupervisor 的消费侧依赖集合。
type Dependencies struct {
	Lock            LockSet
	State           StateStore
	Repository      RepositoryChecker
	Entry           EntryChecker
	UV              UVRunner
	Health          HealthChecker
	Logger          LoggerFactory
	Clock           func() time.Time
	UVPath          string
	PythonPath      string
	PythonPaths     []string
	PID             PIDProbe
	HTTP            HTTPCloser
	ShutdownTimeout time.Duration
	RestartDelay    time.Duration
	Timer           func(time.Duration) <-chan time.Time
	NewTimer        func(time.Duration) Timer
	// MirrorPolicy 是已解析的全局镜像策略，用于按增补 1 C11 生成下发给后端的
	// 有序源列表。零值表示调用方未配置，按目录默认顺序处理。
	MirrorPolicy mirror.Policy
	// Ranker 是本进程的测速器（增补 2 C16）；nil 表示按目录顺序下发。
	Ranker *mirror.Ranker
	// Relay 启动受监督期间常驻的回环中继（增补 2 C17 第 8 条）；nil 表示不起中继。
	Relay RelayStarter
}

// Timer 是可停止的重启等待计时器，避免 timer channel 在收口后泄漏。
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// HTTPCloser 请求受管后端执行优雅关闭。
type HTTPCloser interface {
	Close(context.Context) error
}

// LockSet 提供后端 Mutex 的零等待租约。
type LockSet interface {
	Acquire(context.Context) (Lease, error)
	Close() error
}

// Lease 是一次后端 Mutex 租约。
type Lease interface {
	Close() error
}

// StateStore 是后端消费的稳定环境与事务存储窄接口。
type StateStore interface {
	ReadEnvironment(context.Context) (state.EnvironmentState, error)
	ReadBackendTransaction(context.Context) (Transaction, error)
	BeginBackendTransaction(context.Context, TransactionInput) (TransactionHandle, error)
	UpdateBackendTransaction(context.Context, TransactionHandle, protocol.Stage) error
	RemoveBackendTransaction(context.Context, TransactionHandle) error
	Close() error
}

// Transaction 描述已有 backend 事务的最小事实。
type Transaction struct {
	PID     uint32
	Version string
	Stage   protocol.Stage
	Handle  TransactionHandle
}

// TransactionInput 描述新建 backend 事务的业务身份。
type TransactionInput struct {
	OperationID string
	PID         uint32
	Version     string
	Stage       protocol.Stage
}

// TransactionHandle 是条件删除 backend 事务所需的不可伪造 token。
type TransactionHandle interface{}

// PIDProbe 判断事务记录对应的旧监督进程是否仍存活。
type PIDProbe interface {
	Alive(context.Context, uint32) (bool, error)
}

// ErrTransactionNotFound 表示当前没有 backend 事务。
var ErrTransactionNotFound = errTransactionNotFound{}

type errTransactionNotFound struct{}

func (errTransactionNotFound) Error() string { return "backend transaction not found" }

// RepositoryChecker 返回经验证的活动仓库 revision。
type RepositoryChecker interface {
	Check(context.Context) (RepositoryResult, error)
}

// RepositoryResult 是 backend 需要的仓库检查结果。
type RepositoryResult struct {
	Healthy bool
	Version string
	Commit  string
	Reason  string
}

// EntryChecker 验证受管 backend 入口文件的普通文件与 reparse 身份。
type EntryChecker interface {
	Check(context.Context, string) error
}

// UVRunner 是长驻受管 uv 的唯一启动入口。
type UVRunner interface {
	Check(context.Context, uv.RunOptions) error
	Executable() string
	StartManaged(context.Context, []string, uv.ManagedOptions, process.StreamSink) (ManagedProcess, error)
}

// ManagedProcess 是 backend 需要的进程生命周期能力。
type ManagedProcess interface {
	PID() uint32
	Exited() <-chan struct{}
	Wait(context.Context) (process.ExitResult, error)
	Snapshot() ([]process.Info, error)
	Terminate(uint32) error
	WaitEmpty(context.Context) error
	Close() error
}

// HealthChecker 执行已注入的后端健康与身份检查。
type HealthChecker interface {
	Check(context.Context, health.Expectation, health.Probe) error
}

// Logger 记录受管流并提供日志路径与收口操作。
type Logger interface {
	Record(context.Context, process.StreamRecord) error
	LogPath() string
	Close() error
}

// LoggerFactory 延迟创建本次监督操作的日志 sink。
type LoggerFactory func(context.Context, Request) (Logger, error)
