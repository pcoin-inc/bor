package blockstm

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

type ExecResult struct {
	err      error
	ver      Version
	txIn     TxnInput
	txOut    TxnOutput
	txAllOut TxnOutput
}

type ExecTask interface {
	Execute(mvh *MVHashMap, incarnation int) error
	MVReadList() []ReadDescriptor
	MVWriteList() []WriteDescriptor
	MVFullWriteList() []WriteDescriptor
	Sender() common.Address
	Settle()
}

type ExecVersionView struct {
	ver    Version
	et     ExecTask
	mvh    *MVHashMap
	sender common.Address
}

func (ev *ExecVersionView) Execute() (er ExecResult) {
	er.ver = ev.ver
	if er.err = ev.et.Execute(ev.mvh, ev.ver.Incarnation); er.err != nil {
		return
	}

	er.txIn = ev.et.MVReadList()
	er.txOut = ev.et.MVWriteList()
	er.txAllOut = ev.et.MVFullWriteList()

	return
}

type ErrExecAbortError struct {
	Dependency int
}

func (e ErrExecAbortError) Error() string {
	if e.Dependency >= 0 {
		return fmt.Sprintf("Execution aborted due to dependency %d", e.Dependency)
	} else {
		return "Execution aborted"
	}
}

type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]

	return x
}

type ParallelExecutionResult struct {
	TxIO  *TxnInputOutput
	Stats *[][]uint64
	Deps  *DAG
}

const numGoProcs = 5
const numSpeculativeProcs = 50

// nolint: gocognit
// A stateless executor that executes transactions in parallel
func ExecuteParallel(tasks []ExecTask, profile bool) (ParallelExecutionResult, error) {
	if len(tasks) == 0 {
		return ParallelExecutionResult{MakeTxnInputOutput(len(tasks)), nil, nil}, nil
	}

	// Stores the execution statistics for each task
	stats := make([][]uint64, 0, len(tasks))
	statsMutex := sync.Mutex{}

	// Channel for tasks that should be prioritized
	chTasks := make(chan ExecVersionView, len(tasks))

	// Channel for speculative tasks
	chSpeculativeTasks := make(chan ExecVersionView, len(tasks))

	// Channel to signal that the result of a transaction could be written to storage
	chSettle := make(chan int, len(tasks))

	// Channel to signal that a transaction has finished executing
	chResults := make(chan struct{}, len(tasks))

	// Map that stores the execution results
	results := map[int]ExecResult{}

	// A priority queue that stores the transaction index of results, so we can validate the results in order
	resultQueue := make(IntHeap, 0, len(tasks))
	heap.Init(&resultQueue)

	// A mutex to protect the results map and priority queue
	resultMutex := sync.Mutex{}

	// A wait group to wait for all settling tasks to finish
	var settleWg sync.WaitGroup

	// An integer that tracks the index of last settled transaction
	lastSettled := -1

	// For a task that runs only after all of its preceding tasks have finished and passed validation,
	// its result will be absolutely valid and therefore its validation could be skipped.
	// This map stores the boolean value indicating whether a task satisfy this condition ( absolutely valid).
	skipCheck := make(map[int]bool)

	for i := 0; i < len(tasks); i++ {
		skipCheck[i] = false
	}

	// Execution tasks stores the state of each execution task
	execTasks := makeStatusManager(len(tasks))

	// Validate tasks stores the state of each validation task
	validateTasks := makeStatusManager(0)

	// Stats for debugging purposes
	var cntExec, cntSuccess, cntAbort, cntTotalValidations, cntValidationFail int

	diagExecSuccess := make([]int, len(tasks))
	diagExecAbort := make([]int, len(tasks))

	// Initialize MVHashMap
	mvh := MakeMVHashMap()

	// Stores the inputs and outputs of the last incardanotion of all transactions
	lastTxIO := MakeTxnInputOutput(len(tasks))

	// Tracks the incarnation number of each transaction
	txIncarnations := make([]int, len(tasks))

	// A map that stores the estimated dependency of a transaction if it is aborted without any known dependency
	estimateDeps := make(map[int][]int, len(tasks))

	for i := 0; i < len(tasks); i++ {
		estimateDeps[i] = make([]int, 0)
	}

	begin := time.Now()

	// Launch workers that execute transactions
	for i := 0; i < numSpeculativeProcs+numGoProcs; i++ {
		go func(procNum int) {
			doWork := func(taskCh chan ExecVersionView) {
				for task := range taskCh {
					start := time.Since(begin)
					res := task.Execute()

					if res.err == nil {
						mvh.FlushMVWriteSet(res.txAllOut)
					}

					resultMutex.Lock()
					heap.Push(&resultQueue, res.ver.TxnIndex)
					results[res.ver.TxnIndex] = res
					chResults <- struct{}{}
					resultMutex.Unlock()

					if profile {
						end := time.Since(begin)

						stat := []uint64{uint64(res.ver.TxnIndex), uint64(res.ver.Incarnation), uint64(start), uint64(end), uint64(procNum)}

						statsMutex.Lock()
						stats = append(stats, stat)
						statsMutex.Unlock()
					}
				}
			}

			if procNum < numSpeculativeProcs {
				doWork(chSpeculativeTasks)
			} else {
				doWork(chTasks)
			}
		}(i)
	}

	// Launch a worker that settles valid transactions
	settleWg.Add(len(tasks))

	go func() {
		for t := range chSettle {
			tasks[t].Settle()
			settleWg.Done()
		}
	}()

	// bootstrap first execution
	tx := execTasks.takeNextPending()
	if tx != -1 {
		cntExec++

		chTasks <- ExecVersionView{ver: Version{tx, 0}, et: tasks[tx], mvh: mvh, sender: tasks[tx].Sender()}
	}

	// Before starting execution, going through each task to check their explicit dependencies (whether they are coming from the same account)
	prevSenderTx := make(map[common.Address]int)

	for i, t := range tasks {
		if tx, ok := prevSenderTx[t.Sender()]; ok {
			execTasks.addDependencies(tx, i)
			execTasks.clearPending(i)
		}

		prevSenderTx[t.Sender()] = i
	}

	var res ExecResult

	var err error

	// Start main validation loop
	for range chResults {
		resultMutex.Lock()
		res = results[heap.Pop(&resultQueue).(int)]
		resultMutex.Unlock()

		if res.err == nil { //nolint:nestif
			lastTxIO.recordRead(res.ver.TxnIndex, res.txIn)

			if res.ver.Incarnation == 0 {
				lastTxIO.recordWrite(res.ver.TxnIndex, res.txOut)
				lastTxIO.recordAllWrite(res.ver.TxnIndex, res.txAllOut)
			} else {
				if res.txAllOut.hasNewWrite(lastTxIO.AllWriteSet(res.ver.TxnIndex)) {
					validateTasks.pushPendingSet(execTasks.getRevalidationRange(res.ver.TxnIndex + 1))
				}

				prevWrite := lastTxIO.AllWriteSet(res.ver.TxnIndex)

				// Remove entries that were previously written but are no longer written

				cmpMap := make(map[Key]bool)

				for _, w := range res.txAllOut {
					cmpMap[w.Path] = true
				}

				for _, v := range prevWrite {
					if _, ok := cmpMap[v.Path]; !ok {
						mvh.Delete(v.Path, res.ver.TxnIndex)
					}
				}

				lastTxIO.recordWrite(res.ver.TxnIndex, res.txOut)
				lastTxIO.recordAllWrite(res.ver.TxnIndex, res.txAllOut)
			}

			validateTasks.pushPending(res.ver.TxnIndex)
			execTasks.markComplete(res.ver.TxnIndex)
			diagExecSuccess[res.ver.TxnIndex]++
			cntSuccess++

			execTasks.removeDependency(res.ver.TxnIndex)
		} else if execErr, ok := res.err.(ErrExecAbortError); ok {

			addedDependencies := false

			if execErr.Dependency >= 0 {
				l := len(estimateDeps[res.ver.TxnIndex])
				for l > 0 && estimateDeps[res.ver.TxnIndex][l-1] > execErr.Dependency {
					execTasks.removeDependency(estimateDeps[res.ver.TxnIndex][l-1])
					estimateDeps[res.ver.TxnIndex] = estimateDeps[res.ver.TxnIndex][:l-1]
					l--
				}
				addedDependencies = execTasks.addDependencies(execErr.Dependency, res.ver.TxnIndex)
			} else {
				estimate := 0

				if len(estimateDeps[res.ver.TxnIndex]) > 0 {
					estimate = estimateDeps[res.ver.TxnIndex][len(estimateDeps[res.ver.TxnIndex])-1]
				}
				addedDependencies = execTasks.addDependencies(estimate, res.ver.TxnIndex)
				newEstimate := estimate + (estimate+res.ver.TxnIndex)/2
				if newEstimate >= res.ver.TxnIndex {
					newEstimate = res.ver.TxnIndex - 1
				}
				estimateDeps[res.ver.TxnIndex] = append(estimateDeps[res.ver.TxnIndex], newEstimate)
			}

			execTasks.clearInProgress(res.ver.TxnIndex)
			if !addedDependencies {
				execTasks.pushPending(res.ver.TxnIndex)
			}
			txIncarnations[res.ver.TxnIndex]++
			diagExecAbort[res.ver.TxnIndex]++
			cntAbort++
		} else {
			err = res.err
			break
		}

		// do validations ...
		maxComplete := execTasks.maxAllComplete()

		var toValidate []int

		for validateTasks.minPending() <= maxComplete && validateTasks.minPending() >= 0 {
			toValidate = append(toValidate, validateTasks.takeNextPending())
		}

		for i := 0; i < len(toValidate); i++ {
			cntTotalValidations++

			tx := toValidate[i]

			if skipCheck[tx] || ValidateVersion(tx, lastTxIO, mvh) {
				validateTasks.markComplete(tx)
			} else {
				cntValidationFail++
				diagExecAbort[tx]++
				for _, v := range lastTxIO.AllWriteSet(tx) {
					mvh.MarkEstimate(v.Path, tx)
				}
				// 'create validation tasks for all transactions > tx ...'
				validateTasks.pushPendingSet(execTasks.getRevalidationRange(tx + 1))
				validateTasks.clearInProgress(tx) // clear in progress - pending will be added again once new incarnation executes
				if execTasks.checkPending(tx) {
					// println() // have to think about this ...
				} else if execTasks.blockCount[tx] == -1 {
					execTasks.pushPending(tx)
					execTasks.clearComplete(tx)
					txIncarnations[tx]++
				}
			}
		}

		// Settle transactions that have been validated to be correct and that won't be re-executed again
		maxValidated := validateTasks.maxAllComplete()
		for lastSettled < maxValidated {
			lastSettled++
			if execTasks.checkInProgress(lastSettled) || execTasks.checkPending(lastSettled) || execTasks.blockCount[lastSettled] >= 0 {
				lastSettled--
				break
			}
			chSettle <- lastSettled
		}

		if validateTasks.countComplete() == len(tasks) && execTasks.countComplete() == len(tasks) {
			log.Debug("blockstm exec summary", "execs", cntExec, "success", cntSuccess, "aborts", cntAbort, "validations", cntTotalValidations, "failures", cntValidationFail, "#tasks/#execs", fmt.Sprintf("%.2f%%", float64(len(tasks))/float64(cntExec)*100))
			break
		}

		// Send the next immediate pending transaction to be executed
		if execTasks.minPending() != -1 && execTasks.minPending() == maxValidated+1 {
			nextTx := execTasks.takeNextPending()
			if nextTx != -1 {
				cntExec++

				skipCheck[nextTx] = true

				chTasks <- ExecVersionView{ver: Version{nextTx, txIncarnations[nextTx]}, et: tasks[nextTx], mvh: mvh, sender: tasks[nextTx].Sender()}
			}
		}

		// Send speculative tasks
		for execTasks.minPending() != -1 && len(chSpeculativeTasks) <= 1 {
			// We skip the next five transactions to avoid the case where they all have conflicts and could not be
			// scheduled for re-execution immediately even when it's their time to run, because they are already in
			// speculative queue.
			nextTx := execTasks.takeNextNPending(5)
			if nextTx != -1 {
				cntExec++

				chSpeculativeTasks <- ExecVersionView{ver: Version{nextTx, txIncarnations[nextTx]}, et: tasks[nextTx], mvh: mvh, sender: tasks[nextTx].Sender()}
			}
		}
	}

	close(chTasks)
	close(chSpeculativeTasks)
	close(chResults)
	settleWg.Wait()
	close(chSettle)

	var dag DAG
	if profile {
		dag = BuildDAG(*lastTxIO)
	}

	return ParallelExecutionResult{lastTxIO, &stats, &dag}, err
}
