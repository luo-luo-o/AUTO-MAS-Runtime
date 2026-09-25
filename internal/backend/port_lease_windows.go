//go:build windows

package backend

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"
)

const portAllocationMutexName = `Global\AUTO-MAS-RUNTIME-PORT-ALLOCATION`

type portAllocationLease struct {
	release   chan chan error
	closeOnce sync.Once
	closeErr  error
}

// acquirePortAllocation 跨 Windows 用户会话串行化默认端口探测与后端绑定。
func acquirePortAllocation(ctx context.Context) (*portAllocationLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lease := &portAllocationLease{release: make(chan chan error)}
	ready := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		name, err := windows.UTF16PtrFromString(portAllocationMutexName)
		if err != nil {
			ready <- err
			return
		}
		// 已登录用户共用同一把 Mutex；它仅用于同步，不授权文件访问。
		sd, err := windows.SecurityDescriptorFromString("D:(A;;0x001F0001;;;AU)")
		if err != nil {
			ready <- err
			return
		}
		security := &windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: sd}
		handle, err := windows.CreateMutex(security, false, name)
		if err != nil && !errors.Is(err, windows.ERROR_ALREADY_EXISTS) {
			ready <- err
			return
		}
		defer windows.CloseHandle(handle)
		for {
			if err := ctx.Err(); err != nil {
				ready <- err
				return
			}
			status, waitErr := windows.WaitForSingleObject(handle, 100)
			if waitErr != nil {
				ready <- waitErr
				return
			}
			if status == windows.WAIT_OBJECT_0 || status == windows.WAIT_ABANDONED {
				break
			}
			if status != uint32(windows.WAIT_TIMEOUT) {
				ready <- fmt.Errorf("wait for port allocation mutex: %d", status)
				return
			}
		}
		ready <- nil
		response := <-lease.release
		response <- windows.ReleaseMutex(handle)
	}()
	if err := <-ready; err != nil {
		return nil, err
	}
	return lease, nil
}

func (l *portAllocationLease) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		response := make(chan error, 1)
		l.release <- response
		if err := <-response; err != nil {
			l.closeErr = fmt.Errorf("release port allocation mutex: %w", err)
		}
	})
	return l.closeErr
}
