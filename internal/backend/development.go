package backend

import (
	"context"
	"errors"
	"path/filepath"
	"strings"

	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/filesystem"
	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/protocol"
)

var (
	errDevelopmentPathMissing = errors.New("development path is missing")
	errDevelopmentPathUnsafe  = errors.New("development path is a reparse point")
	errDevelopmentPathType    = errors.New("development path has an unexpected type")
)

// modeForRequest 兼容旧的 managed 消费方；空值只能解释为 managed。
func modeForRequest(request Request) Mode {
	if request.Mode == "" {
		return ModeManaged
	}
	return request.Mode
}

// normalizeDevelopmentRequest 在任何 Runtime 资源获取前固定开发目录身份。
func (s *ManagedSupervisor) normalizeDevelopmentRequest(ctx context.Context, request Request) (Request, error) {
	if modeForRequest(request) != ModeDevelopment {
		return request, nil
	}
	if ctx == nil {
		return request, newError(protocol.CodeInvalidArgument, protocol.StageBackendSpawn, "开发模式上下文不可用", map[string]any{"field": "context"}, errors.New("development context is nil"))
	}
	if err := ctx.Err(); err != nil {
		return request, err
	}
	if strings.TrimSpace(request.DevelopmentRepo) == "" {
		return request, newError(protocol.CodeInvalidArgument, protocol.StageBackendSpawn, "开发模式必须指定源码目录", map[string]any{"field": "repo"}, errors.New("development repository is required"))
	}
	repo, err := filepath.Abs(request.DevelopmentRepo)
	if err != nil {
		return request, newError(protocol.CodeInvalidArgument, protocol.StageBackendSpawn, "开发源码目录无效", map[string]any{"field": "repo", "reason": "absolute_path"}, err)
	}
	repo = filepath.Clean(repo)
	if err := inspectDevelopmentObject(ctx, repo, true); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return request, ctxErr
		}
		if errors.Is(err, errDevelopmentPathUnsafe) {
			return request, newError(protocol.CodeUnsafeReparsePoint, protocol.StageBackendSpawn, "开发源码目录身份不安全", map[string]any{"field": "repo", "reason": "reparse_point"}, err)
		}
		return request, newError(protocol.CodeInvalidArgument, protocol.StageBackendSpawn, "开发源码目录无效", map[string]any{"field": "repo", "reason": developmentPathReason(err)}, err)
	}
	appRoot := filepath.Clean(s.layout.AppRoot())
	inside, containmentErr := filesystem.PathContains(ctx, repo, appRoot)
	if containmentErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return request, ctxErr
		}
		if errors.Is(containmentErr, filesystem.ErrExternalPathUnsafe) {
			return request, newError(protocol.CodeUnsafeReparsePoint, protocol.StageBackendSpawn, "Runtime 或开发源码目录身份不安全", map[string]any{"field": "repo", "reason": "reparse_point"}, containmentErr)
		}
		return request, newError(protocol.CodeInvalidArgument, protocol.StageBackendSpawn, "Runtime 根目录身份不可确认", map[string]any{"field": "app_root", "reason": "identity_unknown"}, containmentErr)
	}
	if inside && !(request.SourceRepository && strings.EqualFold(filepath.Clean(repo), appRoot)) {
		return request, newError(protocol.CodeInvalidArgument, protocol.StageBackendSpawn, "Runtime 根目录不能位于开发源码目录内", map[string]any{"reason": "runtime_root_inside_development_repo"}, nil)
	}
	if err := inspectDevelopmentObject(ctx, filepath.Join(repo, "main.py"), false); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return request, ctxErr
		}
		if errors.Is(err, errDevelopmentPathUnsafe) {
			return request, newError(protocol.CodeUnsafeReparsePoint, protocol.StageBackendSpawn, "开发后端入口身份不安全", map[string]any{"field": "main.py", "reason": "reparse_point"}, err)
		}
		if errors.Is(err, errDevelopmentPathMissing) || errors.Is(err, errDevelopmentPathType) {
			return request, newError(protocol.CodeBackendEntryNotFound, protocol.StageBackendSpawn, "开发后端入口文件不存在", map[string]any{"field": "main.py"}, err)
		}
		return request, newError(protocol.CodeBackendEntryNotFound, protocol.StageBackendSpawn, "开发后端入口不可用", map[string]any{"field": "main.py"}, err)
	}
	if err := inspectDevelopmentObject(ctx, filepath.Join(repo, "pyproject.toml"), false); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return request, ctxErr
		}
		return request, newError(protocol.CodeEnvironmentBroken, protocol.StageBackendSpawn, "开发项目配置不可用", map[string]any{
			"field":  "pyproject.toml",
			"reason": developmentPathReason(err),
		}, err)
	}
	if err := inspectDevelopmentObject(ctx, filepath.Join(repo, ".venv"), true); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return request, ctxErr
		}
		return request, newError(protocol.CodeEnvironmentBroken, protocol.StageBackendSpawn, "开发虚拟环境不可用", map[string]any{
			"field":  ".venv",
			"reason": developmentPathReason(err),
		}, err)
	}
	request.DevelopmentRepo = repo
	return request, nil
}

func inspectDevelopmentObject(ctx context.Context, path string, wantDirectory bool) error {
	path = filepath.Clean(path)
	if path == "" {
		return errDevelopmentPathMissing
	}
	inspection, err := filesystem.InspectExternalPath(ctx, path, wantDirectory)
	if err != nil {
		if errors.Is(err, filesystem.ErrExternalPathUnsafe) {
			return errDevelopmentPathUnsafe
		}
		if errors.Is(err, filesystem.ErrExternalPathNotOrdinary) {
			return errDevelopmentPathType
		}
		return err
	}
	if !inspection.Exists {
		return errDevelopmentPathMissing
	}
	return nil
}

func developmentPathReason(err error) string {
	switch {
	case errors.Is(err, errDevelopmentPathMissing):
		return "missing"
	case errors.Is(err, errDevelopmentPathUnsafe):
		return "reparse_point"
	case errors.Is(err, errDevelopmentPathType):
		return "not_ordinary"
	default:
		return "inspect_failed"
	}
}

func developmentPythonPath(repo string) string {
	return filepath.Join(repo, ".venv", "Scripts", "python.exe")
}

func developmentProjectEnv(repo string) string {
	return filepath.Join(repo, ".venv")
}
