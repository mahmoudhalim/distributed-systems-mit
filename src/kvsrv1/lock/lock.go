package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const Debug = false

func DPrintf(format string, a ...any) {
	if Debug {
		log.Printf(format, a...)
	}
}

func init() {
	log.SetFlags(log.Ltime)
}

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck          kvtest.IKVClerk
	clientID    string
	lockName    string
	lockVersion rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockName: lockname, clientID: kvtest.RandValue(8)}
	DPrintf("Making lock with name %s", lockname)
	return lk
}

func (lk *Lock) Acquire() {
	for {
		DPrintf("Trying to lock %s", lk.lockName)
		state, ver, err := lk.ck.Get(lk.lockName)
		if err == rpc.ErrNoKey { // lock does not exist
			if err := lk.ck.Put(lk.lockName, lk.clientID, 0); err != rpc.OK {
				continue
			}
			lk.lockVersion = 1
			DPrintf("Locked %s", lk.lockName)
			return
		}
		if err == rpc.OK {
			if state == lk.clientID { // I already have the lock
				lk.lockVersion = ver
				return
			}
			if state != "0" { // locked
				time.Sleep(100 * time.Millisecond)
				continue
			}
			// free
			err := lk.ck.Put(lk.lockName, lk.clientID, ver)
			if err != rpc.OK {
				continue
			}
			lk.lockVersion = ver + 1
			return
		}
	}
}

func (lk *Lock) Release() {
	if lk.lockVersion == 0 {
		return
	}
	for {
		err := lk.ck.Put(lk.lockName, "0", lk.lockVersion)
		if err != rpc.OK {
			continue
		}
		DPrintf("Free %s", lk.lockName)
		return
	}
}
