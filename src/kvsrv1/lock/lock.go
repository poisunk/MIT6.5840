package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here

	key      string
	clientID string
	version  rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.key = l
	lk.clientID = kvtest.RandValue(5)
	lk.version = 0
	return lk
}

func (lk *Lock) Acquire() {

	// Your code here
	for {
		value, version, _ := lk.ck.Get(lk.key)
		lk.version = version

		if value == "" {
			err := lk.ck.Put(lk.key, lk.clientID, lk.version)
			if err == rpc.OK {
				lk.version++
				break
			}

			if err == rpc.ErrMaybe {
				value, version, _ := lk.ck.Get(lk.key)
				if value == lk.clientID {
					lk.version = version
					break
				}
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		err := lk.ck.Put(lk.key, "", lk.version)

		if err == rpc.OK {
			lk.version++
			break
		}

		if err == rpc.ErrMaybe {
			value, version, _ := lk.ck.Get(lk.key)
			if value != lk.clientID {
				lk.version = version
				return
			}
		}
		break
	}
}
