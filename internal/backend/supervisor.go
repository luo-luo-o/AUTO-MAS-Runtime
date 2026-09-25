package backend

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/config"
	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/health"
	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/mirror"
	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/process"
	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/protocol"
	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/state"
	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/uv"
)

const cleanupTimeout = 30 * time.Second

// ManagedSupervisor 监督单个受管后端 Job 的完整生命周期。
type ManagedSupervisor struct {
	layout *config.Layout
	deps   Dependencies
	// relayDiagnostics 是本次监督的中继诊断 sink；Logger 建立后 attach，见 relay.go。
	relayDiagnostics *relayDiagnostics
	// infrastructure 是按增补 1 C11 下发给后端的受管基础设施。layout 与
	// MirrorPolicy 在 supervisor 生命周期内不变，因此只在构造期解析一次，
	// 首次启动与单次自动重启、managed 与 development 都读同一份值。
	infrastructure uv.SupervisionInfrastructure
}

// NewManagedSupervisor 创建可按请求选择 managed 或 development 的后端监督器。
func NewManagedSupervisor(layout *config.Layout, deps Dependencies) (*ManagedSupervisor, error) {
	if layout == nil {
		return nil, errors.New("backend layout is nil")
	}
	if deps.Lock == nil || deps.State == nil || deps.Repository == nil ||
		deps.Entry == nil || deps.UV == nil || deps.Health == nil || deps.Logger == nil {
		return nil, errors.New("backend dependencies are incomplete")
	}
	if deps.Clock == nil {
		deps.Clock = time.Now
	}
	if deps.ShutdownTimeout <= 0 {
		deps.ShutdownTimeout = defaultShutdownTimeout
	}
	if deps.RestartDelay <= 0 {
		deps.RestartDelay = defaultRestartDelay
	}
	if deps.UVPath == "" {
		deps.UVPath = deps.UV.Executable()
	}
	if deps.PythonPath == "" {
		deps.PythonPath = layout.VenvPythonExecutable()
	}
	if len(deps.PythonPaths) == 0 && deps.PythonPath != "" {
		deps.PythonPaths = []string{deps.PythonPath}
	}
	if deps.UVPath == "" || deps.PythonPath == "" {
		return nil, errors.New("backend process identity paths are incomplete")
	}
	// 构造期只按目录顺序解析一次（不联网），用于尽早拒绝非法的显式源选择；
	// Supervise 开始时会按实测顺序与中继首项重新解析。
	infrastructure, err := supervisionInfrastructure(layout, deps.MirrorPolicy)
	if err != nil {
		return nil, err
	}
	return &ManagedSupervisor{layout: layout, deps: deps, infrastructure: infrastructure}, nil
}

// supervisionInfrastructure 解析增补 1 C11 下发给后端的受管基础设施。
//
// 这里是 Runtime 里唯一同时持有 layout 与 mirror.Policy 的位置：目录取自 layout，
// 两个有序源列表由 mirror.BuildPlan 给出——plan 的顺序**就是**尝试顺序
// （显式首选最前、目录顺序其次、官方源末位），`--mirror-only` 不含官方源，
// `--offline` 得到空列表。本任务因此不需要任何新的 mirror API。
func supervisionInfrastructure(
	layout *config.Layout,
	policy mirror.Policy,
) (uv.SupervisionInfrastructure, error) {
	return supervisionInfrastructureWithPlan(context.Background(), layout, policy, nil)
}

// supervisionInfrastructureWithPlan 按给定的尝试顺序来源解析下发列表；plan 为 nil 时按目录顺序。
func supervisionInfrastructureWithPlan(
	ctx context.Context,
	layout *config.Layout,
	policy mirror.Policy,
	plan mirror.PlanFunc,
) (uv.SupervisionInfrastructure, error) {
	if plan == nil {
		catalog, err := mirror.DefaultCatalog()
		if err != nil {
			return uv.SupervisionInfrastructure{}, fmt.Errorf("build backend mirror catalog: %w", err)
		}
		plan = mirror.CatalogPlanFunc(catalog)
	}
	packageIndex, err := mirrorSources(ctx, plan, policy, mirror.KindPackageIndex)
	if err != nil {
		return uv.SupervisionInfrastructure{}, err
	}
	python, err := mirrorSources(ctx, plan, policy, mirror.KindPython)
	if err != nil {
		return uv.SupervisionInfrastructure{}, err
	}
	return uv.SupervisionInfrastructure{
		UVCacheDir:          layout.UVCacheDir(),
		PythonInstallDir:    layout.PythonDir(),
		PackageIndexSources: packageIndex,
		PythonSources:       python,
	}, nil
}

// mirrorSources 返回单个 Kind 的有序源地址。
//
// 失败语义与 internal/uv 的网络路径一致：ErrPolicyRejected 表示用户显式指定了一个
// 选不出来的源，必须失败关闭（静默换源等于无视用户意图）；其他错误只说明 Policy
// 本身没被配置（例如零值 Policy），退回目录默认顺序。
func mirrorSources(ctx context.Context, plan mirror.PlanFunc, policy mirror.Policy, kind mirror.Kind) ([]string, error) {
	built, err := buildPlanOrDefault(ctx, plan, policy, kind)
	if err != nil {
		return nil, err
	}
	sources := built.Sources()
	addresses := make([]string, 0, len(sources))
	for _, source := range sources {
		addresses = append(addresses, source.BaseURL())
	}
	return addresses, nil
}

// buildPlanOrDefault 按策略取顺序；显式源选不出来失败关闭，零值策略退回目录默认顺序，取消原样上抛。
func buildPlanOrDefault(ctx context.Context, plan mirror.PlanFunc, policy mirror.Policy, kind mirror.Kind) (mirror.Plan, error) {
	built, err := plan(ctx, policy, kind)
	if err == nil {
		return built, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return mirror.Plan{}, err
	}
	if errors.Is(err, mirror.ErrPolicyRejected) {
		return mirror.Plan{}, newError(protocol.CodeInvalidArgument, protocol.StageBackendSpawn, "镜像源选择无效", map[string]any{
			"sourceKind": kind.String(),
		}, err)
	}
	defaultPolicy, defaultErr := mirror.NewPolicy(mirror.PolicySpec{Preferred: map[mirror.Kind]string{}})
	if defaultErr != nil {
		return mirror.Plan{}, fmt.Errorf("build default backend mirror policy: %w", defaultErr)
	}
	built, defaultErr = plan(ctx, defaultPolicy, kind)
	if defaultErr != nil {
		return mirror.Plan{}, fmt.Errorf("build backend mirror plan: %w", errors.Join(err, defaultErr))
	}
	return built, nil
}

// Supervise 启动并长驻监督指定模式的后端，直到调用方取消或 Job 根进程退出。
func (s *ManagedSupervisor) Supervise(ctx context.Context, request Request) (returnErr error) {
	if ctx == nil {
		return newError(protocol.CodeInvalidArgument, protocol.StageBackendSpawn, "后端监督上下文不可用", nil, errors.New("backend context is nil"))
	}
	if s == nil || s.layout == nil {
		return newError(protocol.CodeInternalError, protocol.StageBackendSpawn, "后端监督器不可用", nil, errors.New("backend supervisor is nil"))
	}
	if request.OperationID == "" || request.RuntimePID == 0 || request.Emitter == nil {
		return newError(protocol.CodeInvalidArgument, protocol.StageBackendSpawn, "后端监督请求无效", nil, errors.New("backend request is invalid"))
	}
	mode := modeForRequest(request)
	if mode != ModeManaged && mode != ModeDevelopment {
		return newError(protocol.CodeUnsupportedMode, protocol.StageBackendSpawn, "后端运行模式不受支持", map[string]any{"mode": string(mode)}, errors.New("backend mode is unsupported"))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// 源码检出可以显式使用 managed 启动 Runtime；其 app-root 同时就是源码根，
	// 复用 development 执行器，发布包的 <app-root>\repo 布局保持不变。
	if mode == ModeManaged && isSourceRepositoryRoot(s.layout.AppRoot()) {
		mode = ModeDevelopment
		request.Mode = mode
		request.DevelopmentRepo = s.layout.AppRoot()
		request.SourceRepository = true
	}
	// 隐式端口在进程间串行分配，直到前一个实例的后端完成绑定。
	if !request.PortExplicit {
		lease, err := acquirePortAllocation(ctx)
		if err != nil {
			return newError(protocol.CodeBackendSpawnFailed, protocol.StageBackendSpawn, "无法协调受监督端口分配", nil, err)
		}
		if lease != nil {
			emitter := &portLeaseEmitter{EventEmitter: request.Emitter, lease: lease}
			request.Emitter = emitter
			defer func() { returnErr = errors.Join(returnErr, emitter.release()) }()
		}
	}
	port, err := resolveSupervisedPort(request, mode)
	if err != nil {
		return err
	}
	request.Port = port
	// 回环中继与实测顺序在任何 spawn 之前就位，并随本次监督整体存活（增补 2 C17 第 8 条）。
	// 后端 Logger 此时还没建立，中继诊断先进缓冲，Logger 建立后 attach 冲刷。
	s.relayDiagnostics = &relayDiagnostics{}
	supervisedRelay, infrastructure, err := s.startSupervisedRelay(ctx, s.relayDiagnostics)
	if err != nil {
		return preferCancellation(ctx, err)
	}
	s.infrastructure = infrastructure
	defer func() {
		if closeErr := supervisedRelay.close(); closeErr != nil {
			returnErr = errors.Join(returnErr, closeErr)
		}
	}()
	if mode == ModeDevelopment {
		var err error
		request, err = s.normalizeDevelopmentRequest(ctx, request)
		if err != nil {
			return err
		}
		if request.Control == nil {
			mailbox := NewControlMailbox(defaultControlMailboxCapacity)
			request.Control = mailbox
			defer mailbox.Close()
		}
	}
	if request.Control != nil {
		return s.superviseControlled(ctx, request)
	}

	locks, err := s.deps.Lock.Acquire(ctx)
	if err != nil || locks == nil {
		if err == nil {
			err = errors.New("backend lock lease is nil")
		}
		primary := preferCancellation(ctx, mapDependencyError(protocol.StageBackendSpawn, protocol.CodeMutexOperationFailed, "后端 Mutex 获取失败", err))
		var cleanupErr error
		if locks != nil {
			cleanupErr = errors.Join(cleanupErr, mapMutexCleanupError(locks.Close()))
		}
		cleanupErr = errors.Join(cleanupErr, mapStateCleanupError(s.deps.State.Close()))
		cleanupErr = errors.Join(cleanupErr, mapMutexCleanupError(s.deps.Lock.Close()))
		return errors.Join(primary, cleanupErr)
	}
	lockOwned := true
	defer func() {
		if lockOwned {
			returnErr = errors.Join(returnErr, mapMutexCleanupError(locks.Close()))
			returnErr = errors.Join(returnErr, mapMutexCleanupError(s.deps.Lock.Close()))
		}
	}()
	stateOwned := true
	defer func() {
		if stateOwned {
			returnErr = errors.Join(returnErr, mapStateCleanupError(s.deps.State.Close()))
		}
	}()

	if err := s.recoverStaleTransaction(ctx); err != nil {
		return preferCancellation(ctx, err)
	}
	if err := s.checkEntry(ctx); err != nil {
		return preferCancellation(ctx, err)
	}
	environment, err := s.deps.State.ReadEnvironment(ctx)
	if err != nil {
		readErr := newError(protocol.CodeEnvironmentBroken, protocol.StageBackendSpawn, "受管环境状态不可读", map[string]any{
			"field":  "environment",
			"reason": "read_failed",
		}, err)
		return preferCancellation(ctx, readErr)
	}
	if err := validateEnvironmentReady(environment); err != nil {
		return preferCancellation(ctx, err)
	}
	revision, err := s.checkRepository(ctx, environment)
	if err != nil {
		return preferCancellation(ctx, err)
	}
	if err := s.deps.UV.Check(ctx, uv.RunOptions{
		Stage:         protocol.StageBackendSpawn,
		ProjectDir:    s.layout.RepoDir(),
		ProjectEnvDir: s.layout.VenvDir(),
	}); err != nil {
		return preferCancellation(ctx, mapDependencyError(protocol.StageBackendSpawn, protocol.CodeUVExecFailed, "受管 uv 校验失败", err))
	}

	logger, err := s.deps.Logger(ctx, request)
	if err != nil || logger == nil {
		if logger != nil {
			err = errors.Join(err, mapLoggerCleanupError(logger.Close()))
		}
		if err == nil {
			err = errors.New("backend logger is nil")
		}
		failure := withFailureDetails(newError(protocol.CodeInternalError, protocol.StageBackendSpawn, "后端日志初始化失败", map[string]any{"sink": "runtime_log"}, err), logger, nil)
		return preferCancellation(ctx, failure)
	}
	s.relayDiagnostics.attach(ctx, logger)
	loggerOwned := true
	defer func() {
		s.relayDiagnostics.attach(ctx, nil)
		if loggerOwned {
			returnErr = errors.Join(returnErr, mapLoggerCleanupError(logger.Close()))
		}
	}()

	tx, err := s.deps.State.BeginBackendTransaction(ctx, TransactionInput{
		OperationID: request.OperationID,
		PID:         request.RuntimePID,
		Version:     revision.Version,
		Stage:       protocol.StageBackendSpawn,
	})
	txOwned := tx != nil
	defer func() {
		if txOwned {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
			removeErr := s.deps.State.RemoveBackendTransaction(cleanupCtx, tx)
			cancel()
			returnErr = errors.Join(returnErr, mapStateCleanupError(removeErr))
		}
	}()
	if err != nil || tx == nil {
		if err == nil {
			err = errors.New("backend transaction handle is nil")
		}
		failure := withFailureDetails(newError(protocol.CodeStateWriteFailed, protocol.StageBackendSpawn, "后端事务写入失败", nil, err), logger, nil)
		return preferCancellation(ctx, failure)
	}

	// 事务建立后再次读取环境与 revision，避免检查与 spawn 之间使用过期事实。
	if err := s.recheckRevision(ctx, environment, revision); err != nil {
		return preferCancellation(ctx, withFailureDetails(err, logger, nil))
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	gate := &streamGate{stage: protocol.StageBackendSpawn}
	sink := s.streamSink(request, logger, gate)
	processOwned := false
	proc, err := s.deps.UV.StartManaged(ctx, []string{
		"run", "--project", s.layout.RepoDir(), "--no-sync", s.layout.BackendEntryFile(),
	}, uv.ManagedOptions{
		RunOptions: uv.RunOptions{
			Stage: protocol.StageBackendSpawn,
			// cwd 是 app-root 而不是 repo：后端相对 cwd 创建的用户数据必须留在
			// workspace sync 整体替换范围之外（增补 1 C6）。入口随之改传绝对路径。
			WorkingDir: s.layout.AppRoot(),
			ProjectDir: s.layout.RepoDir(),
			Line:       nil,
		},
		Identity:       &uv.SupervisionIdentity{Version: revision.Version, Commit: revision.Commit},
		Infrastructure: s.infrastructure,
		Port:           request.Port,
	}, sink)
	if err != nil || proc == nil {
		if fault := gate.Fault(); fault != nil {
			if proc != nil {
				cleanup := s.cleanupProcess(context.WithoutCancel(ctx), proc, tx, logger)
				processOwned, txOwned, loggerOwned = false, false, false
				return errors.Join(cleanup.err, withFailureDetailsExtra(fault, logger, proc, cleanup.details))
			}
			return withFailureDetails(fault, logger, nil)
		}
		if err == nil {
			err = errors.New("managed process is nil")
		}
		spawnFailure := preferCancellation(ctx, newError(protocol.CodeBackendSpawnFailed, protocol.StageBackendSpawn, "后端进程启动失败", nil, err))
		if proc != nil {
			cleanup := s.cleanupProcess(context.WithoutCancel(ctx), proc, tx, logger)
			processOwned, txOwned, loggerOwned = false, false, false
			primary := withFailureDetailsExtra(spawnFailure, logger, proc, cleanup.details)
			return errors.Join(cleanup.err, primary)
		}
		return withFailureDetails(spawnFailure, logger, proc)
	}
	processOwned = true
	defer func() {
		if processOwned {
			cleanup := s.cleanupProcess(context.WithoutCancel(ctx), proc, nil, nil)
			returnErr = errors.Join(returnErr, cleanup.err)
		}
	}()
	if fault := gate.Fault(); fault != nil {
		cleanup := s.cleanupProcess(context.WithoutCancel(ctx), proc, tx, logger)
		processOwned, txOwned, loggerOwned = false, false, false
		return errors.Join(cleanup.err, withFailureDetailsExtra(fault, logger, proc, cleanup.details))
	}

	if err := s.emitState(request.Emitter, protocol.StageBackendSpawn, protocol.StateStartingBackend, "正在启动后端", map[string]any{}); err != nil {
		return err
	}
	if err := gate.Open(request.Emitter); err != nil {
		return s.failAfterStarting(request, proc, tx, logger, gate, err, &processOwned, &txOwned, &loggerOwned)
	}
	gate.SetStage(protocol.StageBackendHealth)
	if err := s.deps.State.UpdateBackendTransaction(ctx, tx, protocol.StageBackendHealth); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			cleanup := s.cleanupProcess(context.WithoutCancel(ctx), proc, tx, logger)
			processOwned, txOwned, loggerOwned = false, false, false
			return errors.Join(cleanup.err, ctxErr)
		}
		return s.failAfterStarting(request, proc, tx, logger, gate, newError(protocol.CodeStateWriteFailed, protocol.StageBackendHealth, "后端事务写入失败", nil, err), &processOwned, &txOwned, &loggerOwned)
	}
	if fault := gate.Fault(); fault != nil {
		return s.failAfterStarting(request, proc, tx, logger, gate, fault, &processOwned, &txOwned, &loggerOwned)
	}

	probe := processProbe{process: proc, uvPath: s.deps.UVPath, pythonPaths: append([]string(nil), s.deps.PythonPaths...)}
	if err := s.deps.Health.Check(ctx, health.Expectation{
		Mode:     health.ModeManaged,
		Protocol: protocol.Version,
		Version:  revision.Version,
		Commit:   revision.Commit,
		Port:     request.Port,
	}, probe); err != nil {
		if fault := gate.Fault(); fault != nil {
			cleanup := s.cleanupProcess(context.WithoutCancel(ctx), proc, tx, logger)
			processOwned, txOwned, loggerOwned = false, false, false
			return errors.Join(cleanup.err, withFailureDetailsExtra(fault, logger, proc, cleanup.details))
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			cleanup := s.cleanupProcess(context.WithoutCancel(ctx), proc, tx, logger)
			processOwned, txOwned, loggerOwned = false, false, false
			return errors.Join(cleanup.err, ctxErr)
		}
		mapped := withFailureDetails(mapDependencyError(protocol.StageBackendHealth, protocol.CodeBackendHealthInvalid, "后端健康检查失败", err), logger, proc)
		return s.failAfterStarting(request, proc, tx, logger, gate, mapped, &processOwned, &txOwned, &loggerOwned)
	}

	if err := s.deps.State.UpdateBackendTransaction(ctx, tx, protocol.StageBackendRun); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			cleanup := s.cleanupProcess(context.WithoutCancel(ctx), proc, tx, logger)
			processOwned, txOwned, loggerOwned = false, false, false
			return errors.Join(cleanup.err, ctxErr)
		}
		return s.failAfterStarting(request, proc, tx, logger, gate, newError(protocol.CodeStateWriteFailed, protocol.StageBackendRun, "后端事务写入失败", nil, err), &processOwned, &txOwned, &loggerOwned)
	}
	if fault := gate.Fault(); fault != nil {
		return s.failAfterStarting(request, proc, tx, logger, gate, fault, &processOwned, &txOwned, &loggerOwned)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		cleanup := s.cleanupProcess(context.WithoutCancel(ctx), proc, tx, logger)
		processOwned, txOwned, loggerOwned = false, false, false
		return errors.Join(cleanup.err, ctxErr)
	}
	gate.SetStage(protocol.StageBackendRun)
	if err := s.emitState(request.Emitter, protocol.StageBackendRun, protocol.StateRunning, "后端已就绪", map[string]any{
		"pid":     proc.PID(),
		"baseUrl": health.BaseURL(request.Port),
		"logPath": logger.LogPath(),
	}); err != nil {
		return s.failAfterStarting(request, proc, tx, logger, gate, err, &processOwned, &txOwned, &loggerOwned)
	}

	select {
	case <-ctx.Done():
		cleanup := s.cleanupProcess(context.WithoutCancel(ctx), proc, tx, logger)
		processOwned, txOwned, loggerOwned = false, false, false
		primary := ctx.Err()
		if fault := gate.Fault(); fault != nil {
			primary = withFailureDetailsExtra(fault, logger, proc, cleanup.details)
		}
		return errors.Join(cleanup.err, primary)
	case <-proc.Exited():
		cleanup := s.cleanupProcess(context.WithoutCancel(ctx), proc, tx, logger)
		processOwned, txOwned, loggerOwned = false, false, false
		primary := error(newCommittedError(protocol.CodeBackendExitedUnexpectedly, protocol.StageBackendRun, "后端意外退出", nil, nil))
		if fault := gate.Fault(); fault != nil {
			primary = withFailureDetails(fault, logger, proc)
		} else if ctxErr := ctx.Err(); ctxErr != nil {
			primary = ctxErr
		}
		return errors.Join(cleanup.err, withFailureDetailsExtra(primary, logger, proc, cleanup.details))
	}
}

func isSourceRepositoryRoot(root string) bool {
	for _, name := range []string{"main.py", "pyproject.toml", ".venv"} {
		info, err := os.Stat(filepath.Join(root, name))
		if err != nil {
			return false
		}
		if name == ".venv" {
			if !info.IsDir() {
				return false
			}
		} else if !info.Mode().IsRegular() {
			return false
		}
	}
	return true
}

func (s *ManagedSupervisor) recoverStaleTransaction(ctx context.Context) error {
	tx, err := s.deps.State.ReadBackendTransaction(ctx)
	if errors.Is(err, ErrTransactionNotFound) {
		return nil
	}
	if err != nil {
		return preferCancellation(ctx, newError(protocol.CodeStateWriteFailed, protocol.StageBackendSpawn, "后端事务读取失败", map[string]any{
			"field":  "backend_transaction",
			"reason": "read_failed",
		}, err))
	}
	if tx.PID == 0 || tx.Handle == nil {
		return newError(protocol.CodeStateWriteFailed, protocol.StageBackendSpawn, "后端事务无效", nil, errors.New("backend transaction identity is invalid"))
	}
	if s.deps.PID != nil {
		alive, probeErr := s.deps.PID.Alive(ctx, tx.PID)
		if probeErr != nil {
			return newError(protocol.CodeStateWriteFailed, protocol.StageBackendSpawn, "后端事务进程状态不可确认", nil, probeErr)
		}
		if !alive {
			if removeErr := s.deps.State.RemoveBackendTransaction(ctx, tx.Handle); removeErr != nil {
				return newError(protocol.CodeStateWriteFailed, protocol.StageBackendSpawn, "后端陈旧事务清理失败", nil, removeErr)
			}
			return nil
		}
	}
	return newError(protocol.CodeUpdateStateAmbiguous, protocol.StageBackendSpawn, "后端事务与 Mutex 状态不一致", map[string]any{
		"reason": "transaction_pid_alive_without_backend_mutex",
		"pid":    tx.PID,
	}, nil)
}

func (s *ManagedSupervisor) checkEntry(ctx context.Context) error {
	if err := s.deps.Entry.Check(ctx, s.layout.BackendEntryFile()); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if errors.Is(err, ErrEntryUnsafe) {
			return newError(protocol.CodeUnsafeReparsePoint, protocol.StageBackendSpawn, "后端入口文件身份不安全", nil, err)
		}
		return newError(protocol.CodeBackendEntryNotFound, protocol.StageBackendSpawn, "后端入口文件不存在", nil, err)
	}
	return nil
}

func validateEnvironmentReady(environment state.EnvironmentState) error {
	if environment.Status != protocol.StateReadyToStart {
		return newError(protocol.CodeEnvironmentBroken, protocol.StageBackendSpawn, "受管环境尚未就绪", environmentStateDetails(environment), nil)
	}
	if environment.LastSuccessful.Version == "" || environment.LastSuccessful.Commit == "" {
		return newError(protocol.CodeEnvironmentBroken, protocol.StageBackendSpawn, "受管环境 revision 缺失", map[string]any{"field": "lastSuccessful"}, nil)
	}
	return nil
}

func environmentStateDetails(environment state.EnvironmentState) map[string]any {
	details := map[string]any{"state": string(environment.Status)}
	if environment.Broken == nil {
		return details
	}
	details["reason"] = string(environment.Broken.Reason)
	details["brokenStage"] = string(environment.Broken.Stage)
	details["exitCode"] = environment.Broken.ExitCode
	if environment.Broken.LogPath != "" {
		details["logPath"] = environment.Broken.LogPath
	}
	return details
}

func (s *ManagedSupervisor) checkRepository(ctx context.Context, environment state.EnvironmentState) (state.Revision, error) {
	result, err := s.deps.Repository.Check(ctx)
	if err != nil {
		return state.Revision{}, preferCancellation(ctx, newError(protocol.CodeEnvironmentBroken, protocol.StageBackendSpawn, "受管仓库校验失败", map[string]any{
			"field":  "revision",
			"reason": "read_failed",
		}, err))
	}
	if !result.Healthy || result.Version != environment.LastSuccessful.Version || result.Commit != environment.LastSuccessful.Commit {
		details := map[string]any{"field": "revision"}
		if result.Reason != "" {
			details["reason"] = result.Reason
		}
		return state.Revision{}, newError(protocol.CodeEnvironmentBroken, protocol.StageBackendSpawn, "受管仓库 revision 不匹配", details, nil)
	}
	return state.Revision{Version: result.Version, Commit: result.Commit}, nil
}

func (s *ManagedSupervisor) recheckRevision(ctx context.Context, environment state.EnvironmentState, revision state.Revision) error {
	latest, err := s.deps.State.ReadEnvironment(ctx)
	if err != nil {
		return preferCancellation(ctx, newError(protocol.CodeEnvironmentBroken, protocol.StageBackendSpawn, "受管环境状态不可读", map[string]any{
			"field":  "environment",
			"reason": "read_failed",
		}, err))
	}
	if latest.Status != protocol.StateReadyToStart || latest.LastSuccessful != environment.LastSuccessful {
		details := environmentStateDetails(latest)
		details["field"] = "lastSuccessful"
		if _, ok := details["reason"]; !ok {
			details["reason"] = "changed_before_spawn"
		}
		return newError(protocol.CodeEnvironmentBroken, protocol.StageBackendSpawn, "受管环境在启动前发生变化", details, nil)
	}
	result, err := s.deps.Repository.Check(ctx)
	if err != nil || !result.Healthy || result.Version != revision.Version || result.Commit != revision.Commit {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return errors.Join(ctxErr, err)
		}
		details := map[string]any{"field": "revision", "reason": "changed_before_spawn"}
		if err != nil {
			details["reason"] = "read_failed"
		} else if result.Reason != "" {
			details["reason"] = result.Reason
		}
		if err == nil {
			err = errors.New("repository revision changed before spawn")
		}
		return newError(protocol.CodeEnvironmentBroken, protocol.StageBackendSpawn, "受管仓库在启动前发生变化", details, err)
	}
	return nil
}

func preferCancellation(ctx context.Context, err error) error {
	if ctx == nil || err == nil {
		return err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return errors.Join(ctxErr, err)
	}
	return err
}

type streamGate struct {
	mu           sync.Mutex
	open         bool
	fault        error
	faulted      chan struct{}
	stage        protocol.Stage
	pending      []protocol.LogEvent
	pendingBytes int
}

const (
	maxPendingLogEvents = 256
	maxPendingLogBytes  = 4 << 20
)

func (g *streamGate) SetStage(stage protocol.Stage) {
	g.mu.Lock()
	g.stage = stage
	g.mu.Unlock()
}

func (g *streamGate) Fault() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fault
}

func (g *streamGate) Faulted() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.faulted == nil {
		g.faulted = make(chan struct{})
		if g.fault != nil {
			close(g.faulted)
		}
	}
	return g.faulted
}

func (g *streamGate) setFaultLocked(err error) {
	if err == nil || g.fault != nil {
		return
	}
	g.fault = err
	if g.faulted == nil {
		g.faulted = make(chan struct{})
	}
	close(g.faulted)
}

func (g *streamGate) Emit(emitter EventEmitter, event protocol.LogEvent) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fault != nil {
		return g.fault
	}
	if !g.open {
		if len(g.pending) >= maxPendingLogEvents || g.pendingBytes+len(event.Message) > maxPendingLogBytes {
			g.setFaultLocked(newError(protocol.CodeOutputWriteFailed, g.stage, "后端日志协议输出失败", map[string]any{"sink": "protocol_output", "reason": "pending_log_overflow"}, nil))
			return g.fault
		}
		g.pending = append(g.pending, event)
		g.pendingBytes += len(event.Message)
		return nil
	}
	if err := emitter.EmitLog(event); err != nil {
		g.setFaultLocked(newError(protocol.CodeOutputWriteFailed, g.stage, "后端日志协议输出失败", map[string]any{"sink": "protocol_output"}, err))
		return g.fault
	}
	return nil
}

func (g *streamGate) Open(emitter EventEmitter) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fault != nil {
		return g.fault
	}
	for _, event := range g.pending {
		if err := emitter.EmitLog(event); err != nil {
			g.setFaultLocked(newError(protocol.CodeOutputWriteFailed, g.stage, "后端日志协议输出失败", map[string]any{"sink": "protocol_output"}, err))
			return g.fault
		}
	}
	g.pending = nil
	g.pendingBytes = 0
	g.open = true
	return nil
}

func (g *streamGate) setFault(err error) {
	if err == nil {
		return
	}
	g.mu.Lock()
	g.setFaultLocked(err)
	g.mu.Unlock()
}

func (s *ManagedSupervisor) streamSink(request Request, logger Logger, gate *streamGate) process.StreamSink {
	return func(ctx context.Context, record process.StreamRecord) error {
		if err := logger.Record(ctx, record); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				return ctxErr
			}
			gate.mu.Lock()
			stage := gate.stage
			gate.mu.Unlock()
			fault := newError(protocol.CodeInternalError, stage, "后端运行日志写入失败", map[string]any{"sink": "runtime_log"}, err)
			gate.setFault(fault)
			return fault
		}
		// Fragment 仅写入 runtime logger；协议出口只发送最终 Event，或空行。
		if record.Event == "" && !record.EndOfLine {
			return nil
		}
		// 协议出口写失败只登记 gate 故障，**不能**把错误回给 process 层：
		// `ManagedProcess.recordSinkError` 对首个 sink 错误的策略是 `job.Terminate(97)`，
		// 那会在宿主崩溃（stdout 读端已断）时把正在优雅关闭的后端连同进程树一起杀掉，
		// C13「stdout 已断不影响优雅关闭」的容错根本没机会生效。故障已记在 gate 上，
		// 监督循环会经 Faulted() 观察到并走关闭收口；这里继续读管道、继续写文件日志。
		if err := gate.Emit(request.Emitter, protocol.LogEvent{
			Source:  "backend",
			Stream:  record.Stream,
			Message: record.Event,
		}); err != nil {
			return nil
		}
		return nil
	}
}

func (s *ManagedSupervisor) emitState(emitter EventEmitter, stage protocol.Stage, status protocol.StateStatus, message string, details map[string]any) error {
	if details == nil {
		details = map[string]any{}
	}
	if err := emitter.EmitState(protocol.StateEvent{Stage: stage, Status: status, Message: message, Details: details}); err != nil {
		return newError(protocol.CodeOutputWriteFailed, stage, "协议状态输出失败", nil, err)
	}
	return nil
}

func (s *ManagedSupervisor) failAfterStarting(
	request Request,
	proc ManagedProcess,
	tx TransactionHandle,
	logger Logger,
	gate *streamGate,
	primary error,
	processOwned, txOwned, loggerOwned *bool,
) error {
	if fault := gate.Fault(); fault != nil {
		primary = withFailureDetails(fault, logger, proc)
	}
	primary = markCommitted(primary)
	cleanup := s.cleanupProcess(context.Background(), proc, tx, logger)
	primary = withFailureDetailsExtra(primary, logger, proc, cleanup.details)
	*processOwned, *txOwned, *loggerOwned = false, false, false
	stateErr := s.emitState(request.Emitter, protocol.StageBackendCleanup, protocol.StateBackendFailed, "后端启动失败", failureDetails(logger, proc, cleanup.details))
	return errors.Join(cleanup.err, primary, stateErr)
}

func markCommitted(err error) error {
	if err == nil {
		return nil
	}
	var backendErr *Error
	if !errors.As(err, &backendErr) || backendErr == nil || backendErr.committed {
		return err
	}
	return &Error{code: backendErr.code, stage: backendErr.stage, message: backendErr.message, details: backendErr.Details(), cause: err, committed: true}
}

func withFailureDetails(err error, logger Logger, proc ManagedProcess) error {
	return withFailureDetailsExtra(err, logger, proc, nil)
}

func withFailureDetailsExtra(err error, logger Logger, proc ManagedProcess, extra map[string]any) error {
	if err == nil {
		return nil
	}
	var backendErr *Error
	if !errors.As(err, &backendErr) || backendErr == nil {
		return err
	}
	details := backendErr.Details()
	for key, value := range failureDetails(logger, proc, extra) {
		details[key] = value
	}
	return &Error{code: backendErr.code, stage: backendErr.stage, message: backendErr.message, details: details, cause: err, committed: backendErr.committed}
}

func failureDetails(logger Logger, proc ManagedProcess, extra map[string]any) map[string]any {
	details := make(map[string]any, len(extra)+2)
	for key, value := range extra {
		details[key] = value
	}
	if logger != nil {
		if path := logger.LogPath(); path != "" {
			details["logPath"] = path
		}
	}
	if proc != nil {
		if pid := proc.PID(); pid != 0 {
			details["pid"] = pid
		}
	}
	return details
}

// processCleanup 是一次进程树收口的事实：forced 表示动用了 Job 兜底，rootForced 进一步
// 区分「根进程仍存活时被强杀」与「根进程已自行退出、只回收残留孤儿」（增补 1 C14），
// orphans 是后一种情况下终止前快照到的残留成员（不含根进程）；快照失败时 orphansUnknown 为 true。
type processCleanup struct {
	details        map[string]any
	err            error
	forced         bool
	rootForced     bool
	orphans        []process.Info
	orphansUnknown bool
}

func (s *ManagedSupervisor) cleanupProcess(ctx context.Context, proc ManagedProcess, tx TransactionHandle, logger Logger) processCleanup {
	outcome := processCleanup{details: map[string]any{}}
	if ctx == nil {
		ctx = context.Background()
	}
	if proc == nil {
		outcome.err = newCommittedError(protocol.CodeInternalError, protocol.StageBackendCleanup, "后端进程资源不可用", nil, errors.New("managed process is nil"))
		return outcome
	}
	for key, value := range failureDetails(logger, proc, nil) {
		outcome.details[key] = value
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	// processErr 只记录 Job/进程资源收口事实；事务和日志错误不能使可恢复
	// 的 Snapshot 竞态重新升级为进程清理失败。
	var processErr error
	var snapshotErr error
	select {
	case <-proc.Exited():
		// Exited 只证明根进程已结束；Job 仍可能包含持有 stdout/stderr
		// 管道的后代。先读取一次成员快照，确认有树后立即终止，避免
		// proc.Wait 等待读者直到长预算耗尽。
		members, err := proc.Snapshot()
		snapshotErr = err
		if err != nil || hasSurvivingDescendant(members, proc.PID()) {
			outcome.forced = true
			outcome.recordOrphans(members, err, proc.PID())
			processErr = errors.Join(processErr, mapCleanupProcessError("terminate", proc.Terminate(1)))
		}
		exitResult, waitErr := proc.Wait(cleanupCtx)
		if !errors.Is(waitErr, context.DeadlineExceeded) {
			outcome.details["exitCode"] = exitResult.ExitCode
		}
		processErr = errors.Join(processErr, mapCleanupProcessError("wait", withoutExpectedCancellation(waitErr, cleanupCtx.Err() != nil)))
	default:
		// 根进程仍存活：这是真正的强制终止，无论树里还有没有别的成员。
		outcome.rootForced = true
		processErr = errors.Join(processErr, mapCleanupProcessError("terminate", proc.Terminate(1)))
		exitResult, waitErr := proc.Wait(cleanupCtx)
		if !errors.Is(waitErr, context.DeadlineExceeded) {
			outcome.details["exitCode"] = exitResult.ExitCode
		}
		processErr = errors.Join(processErr, mapCleanupProcessError("wait", withoutExpectedCancellation(waitErr, cleanupCtx.Err() != nil)))
	}
	waitEmptyErr := proc.WaitEmpty(cleanupCtx)
	if waitEmptyErr != nil {
		// 根进程可能已退出但后代仍占用 Job；先强制终止，再次 Wait/WaitEmpty，
		// 只有第二次确认空树才允许后续成功收口。
		forceCtx, forceCancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		if !outcome.rootForced {
			// 根进程早已自行退出，此刻残留的成员就是它遗留的孤儿；在终止前留下清单。
			members, err := proc.Snapshot()
			outcome.recordOrphans(members, err, proc.PID())
		}
		terminateErr := proc.Terminate(1)
		exitResult, waitErr := proc.Wait(forceCtx)
		if !errors.Is(waitErr, context.DeadlineExceeded) {
			outcome.details["exitCode"] = exitResult.ExitCode
		}
		forceEmptyErr := proc.WaitEmpty(forceCtx)
		forceCancel()
		outcome.forced = forceEmptyErr == nil
		// 首次收口因上下文预算触发强杀时属于可恢复事实；真实非上下文
		// 错误仍须保留，避免强杀成功掩盖底层资源故障。
		processErr = withoutExpectedCancellation(processErr, false)
		if forceEmptyErr != nil {
			processErr = errors.Join(processErr, mapCleanupProcessError("wait_empty", waitEmptyErr))
		}
		processErr = errors.Join(processErr, mapCleanupProcessError("terminate", terminateErr))
		processErr = errors.Join(processErr, mapCleanupProcessError("wait", withoutExpectedCancellation(waitErr, forceCtx.Err() != nil)))
		processErr = errors.Join(processErr, mapCleanupProcessError("wait_empty", forceEmptyErr))
	} else {
		processErr = errors.Join(processErr, mapCleanupProcessError("wait_empty", waitEmptyErr))
	}
	// Close 也属于进程资源收口，必须先加入 processErr，再决定是否保留
	// Snapshot 诊断。这样 OUTPUT_WRITE_FAILED 等真实主因位于错误链首位。
	processErr = errors.Join(processErr, mapCleanupProcessError("close", proc.Close()))
	if snapshotErr != nil && processErr != nil {
		processErr = errors.Join(processErr, mapCleanupProcessError("snapshot", snapshotErr))
	}
	resultErr := processErr
	if tx != nil {
		resourceCtx, resourceCancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		if err := s.deps.State.RemoveBackendTransaction(resourceCtx, tx); err != nil {
			resultErr = errors.Join(resultErr, mapStateCleanupError(err))
		}
		resourceCancel()
	}
	if logger != nil {
		// 本次尝试的 Logger 即将关闭，中继诊断退回缓冲，等下一次尝试的 Logger 再 attach。
		s.relayDiagnostics.attach(ctx, nil)
		if err := logger.Close(); err != nil {
			resultErr = errors.Join(resultErr, mapLoggerCleanupError(err))
		}
	}
	outcome.err = withFailureDetailsExtra(resultErr, logger, proc, outcome.details)
	return outcome
}

// recordOrphans 把快照里根进程以外的成员记为孤儿；快照失败时只能标记未知。
// 多次调用取并集去重，因为 Exited 分支与 WaitEmpty 兜底分支可能先后各拍一次。
func (c *processCleanup) recordOrphans(members []process.Info, snapshotErr error, rootPID uint32) {
	if snapshotErr != nil {
		c.orphansUnknown = true
		return
	}
	for _, member := range members {
		if member.PID == rootPID {
			continue
		}
		duplicate := false
		for _, known := range c.orphans {
			if known.PID == member.PID {
				duplicate = true
				break
			}
		}
		if !duplicate {
			c.orphans = append(c.orphans, member)
		}
	}
}

// hasSurvivingDescendant 判断根进程退出后 Job 里是否还留着别的成员。
// 根进程自身可能因为进程表尚未收敛而短暂留在快照里，而 Exited 已经证明它退出了；
// 把它算成残留会让优雅关闭误报 BACKEND_FORCE_TERMINATED。真正的后代（例如后端
// 漏掉的 worker）仍然会被识别出来并强制回收。
func hasSurvivingDescendant(members []process.Info, rootPID uint32) bool {
	for _, member := range members {
		if member.PID != rootPID {
			return true
		}
	}
	return false
}

func withoutExpectedCancellation(err error, keepDeadline bool) error {
	if err == nil {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		filtered := make([]error, 0, len(children))
		for _, child := range children {
			if kept := withoutExpectedCancellation(child, keepDeadline); kept != nil {
				filtered = append(filtered, kept)
			}
		}
		return errors.Join(filtered...)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) && !keepDeadline {
		return nil
	}
	return err
}

func mapCleanupProcessError(operation string, err error) error {
	if err == nil {
		return nil
	}
	details := map[string]any{"operation": operation}
	var typed *Error
	if errors.As(err, &typed) {
		for key, value := range typed.Details() {
			details[key] = value
		}
		return newCommittedError(typed.Code(), typed.Stage(), typed.Message(), details, err)
	}
	var coded interface {
		Code() protocol.Code
		Stage() protocol.Stage
		Message() string
		Details() map[string]any
	}
	if errors.As(err, &coded) {
		for key, value := range coded.Details() {
			details[key] = value
		}
		return newCommittedError(coded.Code(), coded.Stage(), coded.Message(), details, err)
	}
	var codeOnly interface{ Code() protocol.Code }
	if errors.As(err, &codeOnly) {
		return newCommittedError(codeOnly.Code(), protocol.StageBackendCleanup, "后端进程资源收口失败", details, err)
	}
	return newCommittedError(protocol.CodeBackendShutdownFailed, protocol.StageBackendCleanup, "后端进程资源收口失败", details, err)
}

func mapStateCleanupError(err error) error {
	if err == nil {
		return nil
	}
	return newCommittedError(protocol.CodeStateWriteFailed, protocol.StageBackendCleanup, "后端状态收口失败", nil, err)
}

func mapMutexCleanupError(err error) error {
	if err == nil {
		return nil
	}
	return markCommitted(mapDependencyError(protocol.StageBackendCleanup, protocol.CodeMutexOperationFailed, "后端 Mutex 收口失败", err))
}

func mapLoggerCleanupError(err error) error {
	if err == nil {
		return nil
	}
	return newCommittedError(protocol.CodeInternalError, protocol.StageBackendCleanup, "后端运行日志收口失败", map[string]any{"sink": "runtime_log"}, err)
}

type processProbe struct {
	process     ManagedProcess
	uvPath      string
	pythonPaths []string
}

func (p processProbe) Exited() <-chan struct{} { return p.process.Exited() }

func (p processProbe) Healthy(ctx context.Context) (bool, error) {
	if ctx == nil {
		return false, errors.New("backend process probe context is nil")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	snapshot, err := p.process.Snapshot()
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	rootPID := p.process.PID()
	rootIndex := -1
	for index, info := range snapshot {
		if info.PID == rootPID {
			rootIndex = index
			if !sameExecutable(info.Executable, p.uvPath) {
				return false, nil
			}
			break
		}
	}
	if rootIndex < 0 {
		return false, nil
	}
	for _, info := range snapshot {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if info.PID == rootPID || !anyExecutable(info.Executable, p.pythonPaths) {
			continue
		}
		if descendantOf(info.PID, rootPID, snapshot) {
			return true, nil
		}
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func descendantOf(pid, root uint32, snapshot []process.Info) bool {
	parents := make(map[uint32]uint32, len(snapshot))
	for _, info := range snapshot {
		parents[info.PID] = info.ParentPID
	}
	seen := make(map[uint32]struct{}, len(snapshot))
	for pid != root {
		if pid == 0 {
			return false
		}
		if _, ok := seen[pid]; ok {
			return false
		}
		seen[pid] = struct{}{}
		parent, ok := parents[pid]
		if !ok {
			return false
		}
		pid = parent
	}
	return true
}

func sameExecutable(left, right string) bool {
	return strings.EqualFold(filepath.Clean(left), filepath.Clean(right))
}

func anyExecutable(path string, candidates []string) bool {
	for _, candidate := range candidates {
		if sameExecutable(path, candidate) {
			return true
		}
	}
	return false
}

var _ health.Probe = processProbe{}
