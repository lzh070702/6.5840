package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname string
	id       string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockname = lockname
	lk.id = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.lockname)
		switch err {
		case rpc.ErrNoKey:
			if lk.ck.Put(lk.lockname, lk.id, 0) == rpc.OK {
				return
			}
		case rpc.OK:
			if val == lk.id {
				return
			}
			if val == "" {
				if lk.ck.Put(lk.lockname, lk.id, ver) == rpc.OK {
					return
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.lockname)
		switch err {
		case rpc.ErrNoKey:
			return
		case rpc.OK:
			if val != lk.id {
				return
			}
			if lk.ck.Put(lk.lockname, "", ver) == rpc.OK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}
