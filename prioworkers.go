package prioworkers

import (
	"log"
	"sync"
	"sync/atomic"
)

const numPriorities = 100

type PrioworkersOptions struct {
	// If number of blocked workers exceeds this limit a worker will be spinned even if priority doesn't allow it
	MaxBlockedWorkers int64
	// Number of low priority workers that will be spinned at the same time when priority allows
	LowPrioSpinCnt int64
	// If set to true LowPrioSpinCnt will be enforced regardless of priority
	// This effectively limits the maximum number of running workers regardless of priority
	// If set to false and there's no worker running with higher priority the worker will spin immediately regardless of LowPrioSpinCnt
	EnforceSpinCntOnHighPrio bool
	// Workers are waiting for signals on channels
	// This option controls the size of the buffer for these channels
	// If set too low workers ending work might block
	SignalChanBuffSize int
	// internal flag to signal that init was called
	initCalled bool
}

type State struct {
	// current state
	RunningWorkersPerPrioCnt [numPriorities]int64
	WaitingWorkersPerPrioCnt [numPriorities]int64
	// all time maximums
	MaxEverRunningSamePrio int64
	MaxEverWaitingSamePrio int64
}

var Options PrioworkersOptions

type prioCounters struct {
	prioCnt [numPriorities]atomic.Int64
	maxEver atomic.Int64
}

var prioRunningCnt prioCounters
var prioWaitingCnt prioCounters
var curRunningPrio atomic.Int64
var prioSignalChans [numPriorities]chan struct{}
var queueTraversalMutex sync.Mutex

func Init(options *PrioworkersOptions) {
	if options != nil {
		Options = *options
	}
	// set defaults
	if Options.SignalChanBuffSize < 1 {
		if Options.LowPrioSpinCnt > 0 {
			Options.SignalChanBuffSize = int(options.LowPrioSpinCnt) + 1
		} else {
			Options.SignalChanBuffSize = 256
		}
	}
	// make prio channels
	for i := 0; i < numPriorities; i++ {
		prioSignalChans[i] = make(chan struct{}, Options.SignalChanBuffSize)
	}
	Options.initCalled = true
}

func WorkStart(prio int64) int64 {
	if !Options.initCalled {
		log.Fatalln("You must call prioworkers init before using it")
	}
	// if work prio is at least current running prio
	// and LowPrioSpinCnt is not enforced on all prio
	// or it is enforced but we're within LowPrioSpinCnt limit
	// then we can start immediately
	if prio >= curRunningPrio.Load() {
		if (!Options.EnforceSpinCntOnHighPrio || Options.LowPrioSpinCnt < 1) ||
			(Options.EnforceSpinCntOnHighPrio && Options.LowPrioSpinCnt > prioRunningCnt.get(prio)) {
			curRunningPrio.Store(prio)
			prioRunningCnt.inc(prio)
			return prio
		}
	}
	// worker with higher priorities are running or LowPrioSpinCnt is enforced on all workers and limit is reached
	// then current worker should wait (unless limit is exceeded)
	// if the number of waiting threads hit the maxBlockedWorkers limit we let it run immediately; otherwise we add it to waiting queue
	if Options.MaxBlockedWorkers > 0 && prioWaitingCnt.get(prio) >= Options.MaxBlockedWorkers {
		prioRunningCnt.inc(prio)
	} else {
		prioWaitingCnt.inc(prio)
		<-prioSignalChans[prio] // block waiting for other workers to finish
		// worker can now start and update state accordingly
		prioRunningCnt.inc(prio)
	}
	return prio
}

func WorkEnd(prio int64) {
	if !Options.initCalled {
		log.Fatalln("You must call prioworkers init before using it")
	}
	prioRunningCnt.dec(prio)
	// if current running priority is higher than this worker priority there's nothing else to do
	// because all lower prios should continue to be blocked waiting for higher prio workers to finish
	if curRunningPrio.Load() > prio {
		return
	}
	// if current running priority is equal to this worker and there are still workers with the same priority waiting we should spin another worker with the same priority
	// for each worker that finished we should spin exactly one worker in its place
	if prio == curRunningPrio.Load() && prioWaitingCnt.get(prio) > 0 {
		prioWaitingCnt.dec(prio)
		prioSignalChans[prio] <- struct{}{}
	} else { // there's no work being done at current or higher prio so we search in our queue for workers at lower priorities that are waiting
		if queueTraversalMutex.TryLock() {
			var i int64
			for i = int64(numPriorities) - 1; i >= 0; i-- { // traverse priority queue downwards
				if prioWaitingCnt.get(i) > 0 { // we find workers waiting at this prio so we unlock all of them
					// set spinCnt to be the number of threads we have to spin; we check that it's within limits
					spinCnt := Options.LowPrioSpinCnt
					if spinCnt < 1 { // LowPrioSpinCnt is set to 0 or less which means that there's no limit - we spin all waiting workers
						spinCnt = prioWaitingCnt.get(i)
					} else {
						spinCnt -= prioRunningCnt.get(i)              // if there are any running workers at same prio we try to stay within LowPrioSpinCnt limits
						spinCnt = min(spinCnt, prioWaitingCnt.get(i)) // we can't spin more workers than how many are actually waiting
					}
					// send signals to workers
					for j := int64(0); j < spinCnt; j++ {
						prioWaitingCnt.dec(i)
						prioSignalChans[i] <- struct{}{}
					}
					// stop searching for lower priorities because we already do work at higher prio
					break
				}
			}
			// update state
			curRunningPrio.Store(max(i, int64(0)))
			queueTraversalMutex.Unlock()
		}
	}
}

func GetState() State {
	if !Options.initCalled {
		log.Fatalln("You must call prioworkers init before using it")
	}
	state := State{
		MaxEverRunningSamePrio: prioRunningCnt.getMaxEver(),
		MaxEverWaitingSamePrio: prioWaitingCnt.getMaxEver(),
	}
	for i := int64(0); i < numPriorities; i++ {
		state.RunningWorkersPerPrioCnt[i] = prioRunningCnt.get(i)
		state.WaitingWorkersPerPrioCnt[i] = prioWaitingCnt.get(i)
	}
	return state
}

func (counters *prioCounters) inc(prio int64) {
	counters.prioCnt[prio].Add(1)
	if counters.prioCnt[prio].Load() > counters.maxEver.Load() {
		counters.maxEver.Store(counters.prioCnt[prio].Load())
	}
}

func (counters *prioCounters) dec(prio int64) {
	counters.prioCnt[prio].Add(-1)
}

func (counters *prioCounters) get(prio int64) int64 {
	return counters.prioCnt[prio].Load()
}

func (counters *prioCounters) getMaxEver() int64 {
	return counters.maxEver.Load()
}
