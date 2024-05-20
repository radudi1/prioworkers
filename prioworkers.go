package prioworkers

import (
	"sync/atomic"
)

const numPriorities = 100

type PrioworkersOptions struct {
	MaxBlockedWorkers        int64 // if number of blocked workers exceeds this limit a worker will be spinned even if priority doesn't allow it
	LowPrioSpinCnt           int64 // number of low priority workers that will be spinned at the same time when priority allows
	EnforceSpinCntOnHighPrio bool
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
var workerChans [numPriorities]chan struct{}

func Init(options *PrioworkersOptions) {
	Options = *options
	for i := 0; i < numPriorities; i++ {
		workerChans[i] = make(chan struct{})
	}
}

func WorkStart(prio int64) int64 {
	// if work prio is at least current running prio we can start immediately - no further processing or storing needs to be done
	if prio >= curRunningPrio.Load() {
		curRunningPrio.Store(prio)
		prioRunningCnt.inc(prio)
	} else { // worker with higher priorities are running and current worker should wait (unless limit is exceeded)
		// if the number of waiting threads hit the maxBlockedWorkers limit we let it run immediately; otherwise we add it to waiting queue
		if Options.MaxBlockedWorkers > 0 && prioWaitingCnt.get(prio) >= Options.MaxBlockedWorkers {
			prioRunningCnt.inc(prio)
		} else {
			prioWaitingCnt.inc(prio)
			<-workerChans[prio] // block waiting for other workers to finish
			// worker can now start and update state accordingly
			prioWaitingCnt.dec(prio)
			prioRunningCnt.inc(prio)
		}
	}
	return prio
}

func WorkEnd(prio int64) {
	prioRunningCnt.dec(prio)
	// if current running priority is higher than this worker priority there's nothing else to do
	if curRunningPrio.Load() > prio {
		return
	}
	// if current running priority is equal to this worker and there are still workers with the same priority waiting we should spin another worker with the same priority
	if prio == curRunningPrio.Load() && prioWaitingCnt.get(prio) > 0 {
		workerChans[prio] <- struct{}{}
	} else { // there's no work being done at current or higher prio so we search in our queue for workers at lower priorities that are waiting
		for i := int64(numPriorities) - 1; i >= 0; i-- { // traverse priority queue downwards
			if prioWaitingCnt.get(i) > 0 { // we find workers waiting at this prio so we unlock all of them
				// set spinCnt to be the number of threads we have to spin; we check that it's within limits
				spinCnt := Options.LowPrioSpinCnt
				if spinCnt < 1 || prioWaitingCnt.get(i) < spinCnt {
					spinCnt = prioWaitingCnt.get(i)
				}
				// send signals to workers
				for j := int64(0); j < spinCnt; j++ {
					workerChans[i] <- struct{}{}
				}
				// stop searching for lower priorities because we already do work at higher prio
				break
			}
			// update state
			curRunningPrio.Store(max(i, int64(0)))
		}
	}
}

func GetState() State {
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
