package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/health"
	"github.com/AUTO-MAS-Project/AUTO-MAS-Runtime/internal/protocol"
)

const (
	// developmentDefaultPort 是 development 模式的缺省端口（增补 1 C12）：与 AUTO-MAS dev
	// 已合入的「开发版 36164 / 正式版 36163 并存」约定一致，让受监督的开发版不撞正式版。
	developmentDefaultPort = 36164
	backendClosePath       = "/api/core/close"
)

// defaultPortForMode 返回模式的缺省受监督端口：managed 沿用 health.DefaultPort。
func defaultPortForMode(mode Mode) int {
	if mode == ModeDevelopment {
		return developmentDefaultPort
	}
	return health.DefaultPort
}

// resolveSupervisedPort 把 Request.Port 解析成最终端口。未显式指定时先尝试模式默认端口，
// 被其他实例占用则由内核探测一个空闲端口；显式端口冲突仍保持失败行为。
// 这里是 backend 内唯一的解析点，首启、单次自动重启、managed 与 development 都读写回
// Request 的值，因此同一监督进程生命周期内端口不变。
func resolveSupervisedPort(request Request, mode Mode) (int, error) {
	port := request.Port
	defaultPort := defaultPortForMode(mode)
	if port == 0 {
		port = defaultPort
	}
	if !health.ValidPort(port) {
		return 0, newError(protocol.CodeInvalidArgument, protocol.StageBackendSpawn, "受监督端口超出范围", map[string]any{
			"field": "port",
			"port":  port,
		}, errors.New("supervised port is out of range"))
	}
	// CLI 会把未显式给出的端口解析为模式默认值，因此同时检查 PortExplicit。
	if !request.PortExplicit && port == defaultPort {
		if available, ok := probePort(port); ok {
			return available, nil
		}
		available, ok := probePort(0)
		if !ok {
			return 0, newError(protocol.CodeBackendSpawnFailed, protocol.StageBackendSpawn, "无法分配受监督端口", map[string]any{"port": port}, errors.New("no available supervised port"))
		}
		return available, nil
	}
	return port, nil
}

// probePort 检查后端监听地址是否可用。Windows 允许回环地址与通配地址重复绑定，
// 因此固定端口先检测现存监听者，再按后端的通配地址试绑定。
// 监听器只在探测期间持有，实际后端启动前会释放；监督生命周期内 request.Port 保持不变。
func probePort(port int) (int, bool) {
	if port != 0 {
		connection, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			_ = connection.Close()
			return 0, false
		}
	}
	listener, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		return 0, false
	}
	defer listener.Close()
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok || !health.ValidPort(address.Port) {
		return 0, false
	}
	return address.Port, true
}

// portLeaseEmitter 在后端已经绑定并通过健康检查后释放分配锁。
type portLeaseEmitter struct {
	EventEmitter
	lease *portAllocationLease
}

func (e *portLeaseEmitter) release() error {
	return e.lease.Close()
}

func (e *portLeaseEmitter) EmitState(event protocol.StateEvent) error {
	if event.Stage == protocol.StageBackendRun && event.Status == protocol.StateRunning {
		if err := e.release(); err != nil {
			return err
		}
	}
	return e.EventEmitter.EmitState(event)
}

// backendCloseURL 返回派生端口下的优雅关闭地址。
func backendCloseURL(port int) string {
	return health.BaseURL(port) + backendClosePath
}

// loopbackHTTPCloser 向派生端口的 /api/core/close 发起关闭请求。
type loopbackHTTPCloser struct {
	port int
}

func newLoopbackHTTPCloser(port int) loopbackHTTPCloser {
	return loopbackHTTPCloser{port: port}
}

func (c loopbackHTTPCloser) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("backend shutdown context is nil")
	}
	client := &http.Client{Transport: &http.Transport{Proxy: nil}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, backendCloseURL(c.port), nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	readErr := error(nil)
	if response.Body != nil {
		_, readErr = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	}
	closeErr := error(nil)
	if response.Body != nil {
		closeErr = response.Body.Close()
	}
	client.CloseIdleConnections()
	statusErr := error(nil)
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		statusErr = fmt.Errorf("backend close returned status %d", response.StatusCode)
	}
	return errors.Join(statusErr, readErr, closeErr)
}

var _ HTTPCloser = loopbackHTTPCloser{}
