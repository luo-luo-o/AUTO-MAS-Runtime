//go:build !windows

package backend

import "context"

type portAllocationLease struct{}

func acquirePortAllocation(context.Context) (*portAllocationLease, error) {
	return nil, nil
}

func (*portAllocationLease) Close() error { return nil }
