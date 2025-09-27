package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const (
	StateFree   = "Free"
	StateLocked = "Locked"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockID   string
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockID = l
	lk.clientID = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here

	// 先get()是否有锁，没有锁的话就put()自己的锁
	for {
		clientID, version, err := lk.ck.Get(lk.lockID)

		// 网络问题直接重试
		if err == rpc.ErrNoKey {
			// 自由锁 直接
			err_put := lk.ck.Put(lk.lockID, lk.clientID, 0)
			if err_put == rpc.OK {
				return
			}
			continue
		}

		// get成功的情况
		if err == rpc.OK {
			if clientID == "" {
				err_put := lk.ck.Put(lk.lockID, lk.clientID, version)

				if err_put == rpc.OK {
					// 成功就退出
					return
				}

				if err_put == rpc.ErrMaybe {
					// 不确定是否成功就再查询一次
					value, _, _ := lk.ck.Get(lk.lockID)

					if value == lk.clientID {
						return
					} else {
						continue
					}
				}
			}
			continue
		}

		// time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here

	// 要释放锁要先查锁是不是自己的
	for {
		clientID, version, err := lk.ck.Get(lk.lockID)
		if err != rpc.OK {
			// 网络问题直接重试
			continue
		}

		if (clientID != "") && (clientID != lk.clientID) {
			// 锁住就等待一会儿
			time.Sleep(10 * time.Microsecond)
			continue
		} else {
			err := lk.ck.Put(lk.lockID, "", version)

			if err == rpc.OK {
				// 成功就退出
				return
			} else if err == rpc.ErrMaybe {
				value, _, _ := lk.ck.Get(lk.lockID)
				if value == "" {
					return
				} else {
					continue
				}
			} else {
				// 重试
				continue
			}
		}
	}
}
