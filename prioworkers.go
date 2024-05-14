package prioworkers

type PrioworkersOptions struct {
	MaxBlockedWorkers        int // if number of blocked workers exceeds this limit a worker will be spinned even if priority doesn't allow it
	LowPrioSpinCnt           int // number of low priority workers that will be spinned at the same time when priority allows
	EnforceSpinCntOnHighPrio bool
}

type State struct {
	// current state
	WorkStartChanLen         int
	WorkEndChanLen           int
	RunningWorkersPerPrioCnt [100]int
	WaitingWorkersPerPrioCnt [100]int
	// all time maximums
	MaxEverWorkChanLen     int
	MaxEverRunningSamePrio int
	MaxEverWaitingSamePrio int
}

var Options PrioworkersOptions

var workStartChan chan int
var workEndChan chan int
var workerChans [100]chan struct{} // a channel for each priority; workers will block on reading from it
var chanBuffSize int               // size of internal channel buffers;
var getStateChan chan struct{}
var stateChan chan State

func Init(chanBuffSize int, options *PrioworkersOptions) {
	Options = *options
	workStartChan = make(chan int, chanBuffSize)
	workEndChan = make(chan int, chanBuffSize)
	for i := range workerChans {
		workerChans[i] = make(chan struct{})
	}
	getStateChan = make(chan struct{})
	stateChan = make(chan State)
	go workerCoordinator()
}

func WorkStart(prio int) int {
	workStartChan <- prio
	<-workerChans[prio]
	return prio
}

func WorkEnd(prio int) {
	workEndChan <- prio
}

func GetState() State {
	getStateChan <- struct{}{}
	return <-stateChan
}

func workerCoordinator() {

	var prioRunningCnt [100]int
	var prioWaitingCnt [100]int
	var curRunningPrio int

	var maxEverWorkChanLen int
	var maxEverRunningSamePrio int
	var maxEverWaitingSamePrio int

	for {
		select {

		// we receive signal worker needs to start at certain prio
		case prio := <-workStartChan:
			// if work prio is at least current running prio we can start immediately - no further processing or storing needs to be done
			if prio >= curRunningPrio {
				curRunningPrio = prio
				prioRunningCnt[prio]++
				workerChans[prio] <- struct{}{}
			} else { // worker with higher priorities are running and current worker should wait (unless limit is exceeded)
				// if the number of waiting threads hit the maxBlockedWorkers limit we let it run immediately; otherwise we add it to waiting queue
				if Options.MaxBlockedWorkers > 0 && prioWaitingCnt[prio] >= Options.MaxBlockedWorkers {
					prioRunningCnt[prio]++
					workerChans[prio] <- struct{}{}
				} else {
					prioWaitingCnt[prio]++
				}
			}
			// update stats
			if len(workStartChan) > maxEverWorkChanLen {
				maxEverWorkChanLen = len(workStartChan)
			}
			if prioWaitingCnt[prio] > maxEverWaitingSamePrio {
				maxEverWaitingSamePrio = prioWaitingCnt[prio]
			}

		// we receive signal that worker with certain prio ended work
		case prio := <-workEndChan:
			prioRunningCnt[prio]--
			// if current running priority is higher than this worker priority there's nothing else to do
			if curRunningPrio > prio {
				break
			}
			// if current running priority is equal to this worker and there are still workers with the same priority waiting we should spin another worker with the same priority
			if prio == curRunningPrio && prioWaitingCnt[prio] > 0 {
				prioRunningCnt[prio]++
				workerChans[prio] <- struct{}{}
			} else { // there's no work being done at current or higher prio so we search in our queue for workers at lower priorities that are waiting
				for i := prio - 1; i >= 0; i-- { // traverse priority queue downwards
					if prioWaitingCnt[i] > 0 { // we find workers waiting at this prio so we unlock all of them
						// set spinCnt to be the number of threads we have to spin; we check that it's within limits
						spinCnt := Options.LowPrioSpinCnt
						if prioWaitingCnt[i] < spinCnt || spinCnt < 1 {
							spinCnt = prioWaitingCnt[i]
						}
						// update state
						curRunningPrio = i
						prioWaitingCnt[i] -= spinCnt
						prioRunningCnt[i] += spinCnt
						// send signals to workers
						for j := 0; j < spinCnt; j++ {
							workerChans[i] <- struct{}{}
						}
						// stop searching for lower priorities because we already do work at higher prio
						break
					}
				}
			}
			// update stats
			if len(workEndChan) > maxEverWorkChanLen {
				maxEverWorkChanLen = len(workEndChan)
			}
			if prioRunningCnt[prio] > maxEverRunningSamePrio {
				maxEverRunningSamePrio = prioRunningCnt[prio]
			}

		// we receive signal that a client wants current state
		case <-getStateChan:
			state := State{
				WorkStartChanLen:         len(workStartChan),
				WorkEndChanLen:           len(workEndChan),
				RunningWorkersPerPrioCnt: prioRunningCnt,
				WaitingWorkersPerPrioCnt: prioWaitingCnt,
				MaxEverRunningSamePrio:   maxEverRunningSamePrio,
				MaxEverWaitingSamePrio:   maxEverWaitingSamePrio,
			}
			stateChan <- state

		}
	}

}
