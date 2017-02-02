using System;
using System.Threading;
using System.Diagnostics;
using System.Collections.Generic;

/*
Copyright (c) 2016 James Ivie

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

This code may not be incorporated into any source or binary code distributed by Amazon or Microsoft or their respective subsidiaries, subcontractors, parent companies, etc. without a separate license.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

namespace S3BucketSync
{
    /// <summary>
    /// An interface that gives the caller access to the status of the completion of a work unit and allows them wait for it's completion.  This interface is disposable and instance must be disposed.
    /// </summary>
    public interface IWorkerJob : IDisposable
    {
        /// <summary>
        /// Gets whether or not the operation is complete.
        /// </summary>
        bool IsComplete { get; }
        /// <summary>
        /// Waits for the operation to finish.
        /// </summary>
        void WaitOne();
        /// <summary>
        /// Waits for the operation to finish, timing out and returning <b>false</b> if the signal doesn't come in the specified time.
        /// </summary>
        /// <param name="millisecondsToWait">The number of milliseconds to wait before timing out and returning <b>false</b>.</param>
        /// <returns>Whether or not the signal was received.</returns>
        bool WaitOne(int millisecondsToWait);
        /// <summary>
        /// Waits for the operation to finish, timing out and returning <b>false</b> if the signal doesn't come in the specified time.
        /// </summary>
        /// <param name="timeToWait">A <see cref="TimeSpan"/> indicating how long to wait before timing out and returning <b>false</b>.</param>
        /// <returns>Whether or not the signal was received.</returns>
        bool WaitOne(TimeSpan timeToWait);
        /// <summary>
        /// Waits for completion and gets the result of the operation, throwing any exception that occured during the work.
        /// </summary>
        /// <returns>Whatever the work returned.</returns>
        object GetResult();
    }
    /// <summary>
    /// A worker which contains a thread and various other objects needed to use the thread.  Disposes of itself when the thread is stopped.
    /// </summary>
    class Worker : IntrusiveSinglyLinkedListNode, IDisposable
    {
        private WorkerPool _pool;
        private string _poolName;
        private string _id;
        private long _invokeTicks;
        private Thread _thread;
        private ManualResetEvent _wakeThread;       // interlocked, owned by the worker thread
        private ManualResetEvent _allowDisposal;
        private Delegate _actionToPerform;
        private int _stop;

        public Worker(WorkerPool pool, string id, ThreadPriority priority)
        {
            try
            {
#if DEBUG
                // suppress finalization
                System.GC.SuppressFinalize(this);
#endif
                _pool = pool;
                _poolName = pool.Name;
                _id = id;
                _wakeThread = new ManualResetEvent(false);
                _allowDisposal = new ManualResetEvent(false);
                // start the thread, it should block immediately until a work unit is ready
                _thread = new Thread(new ThreadStart(WorkerFunc));
                _thread.Name = id;
                _thread.IsBackground = true;
                _thread.Priority = priority;
                _thread.Start();
#if DEBUG
                System.Diagnostics.Debug.WriteLine(DateTime.UtcNow.ToShortTimeString() + ": Creating worker: " + id);
                // reregister for finalization
                System.GC.ReRegisterForFinalize(this);
#endif
            }
            catch
            {
                // only dispose if there was an error
                Dispose();
                throw;
            }
        }
#if DEBUG
        private string fStackAtConstruction = new StackTrace().ToString();
        ~Worker()
        {
            System.Diagnostics.Debug.Fail("Agilix.Shared.Worker in " + _poolName + " pool not disposed!  Constructed at: " + fStackAtConstruction);
            Dispose();
        }
#endif

        public void Dispose()
        {
            // dispose of the events
            using (System.Threading.Interlocked.Exchange(ref _wakeThread, null)) { }
            using (System.Threading.Interlocked.Exchange(ref _allowDisposal, null)) { }
#if DEBUG
            // suppress finalization
            System.GC.SuppressFinalize(this);
#endif
        }

        /// <summary>
        /// Gets whether or not this worker is currently executing a job.
        /// </summary>
        internal bool IsBusy { get { return _actionToPerform != null; } }
        /// <summary>
        /// Schedules the specified delegate on this worker's thread and returns <b>true</b>, or returns <b>false</b> if the worker is already busy servicing another job.
        /// </summary>
        /// <param name="action">The action to perform asynchronously.</param>
        internal void RunAsync(Action action)
        {
            // are we already stopped?
            if (_stop != 0)
            {
                throw new InvalidOperationException("The Worker has already been stopped!");
            }
            // try to put in this work--if there is something already there, we must be busy!
            if (System.Threading.Interlocked.CompareExchange(ref _actionToPerform, action, null) != null)
            {
                // the worker was busy so we couldn't marshal the action to it!
                throw new InvalidOperationException("Worker thread already in use!");
            }
            // start the timer
            System.Threading.Interlocked.Exchange(ref _invokeTicks, WorkerPool.Ticks);
            // signal the thread to begin the work
            _wakeThread.Set();
            // give up our timeslice here, hoping that the other thread will pick up and run the action right now
            Thread.Sleep(0);
            // we successfully issued the work request
        }
        /// <summary>
        /// Tells the thread to stop and exit.
        /// </summary>
        internal void Stop()
        {
            // record when we invoked the stop command
            System.Threading.Interlocked.Exchange(ref _invokeTicks, WorkerPool.Ticks);
            // mark for stopping
            System.Threading.Interlocked.Exchange(ref _stop, 1);
            // worker has retired!
            System.Diagnostics.Debug.WriteLine(DateTime.UtcNow.ToShortTimeString() + ": Retiring worker: " + _id);
            // wake thread so it can exit gracefully
            _wakeThread.Set();
            // tell the thread it can dispose and exit
            _allowDisposal.Set();
        }

        private void RunWithLogException(Action a, string exceptionFormatString)
        {
            try
            {
                a();
            }
            catch (Exception e)
            {
                if (!(e is ThreadAbortException)) Program.Error(String.Format(System.Globalization.CultureInfo.InvariantCulture, exceptionFormatString, _thread.Name, e.ToString()));
            }
        }

        private void WorkerFunc()
        {
            try
            {
                RunWithLogException(
                    () =>
                    {
                        long maxInvokeTicks = 0;
                        System.Diagnostics.Debug.WriteLine("Starting " + _id);
                        long startTicks = WorkerPool.Ticks;
                        long completionTicks = startTicks;
                        // loop until we're told to stop
                        while (_stop == 0)
                        {
                            startTicks = WorkerPool.Ticks;
                            completionTicks = startTicks;
                            // wait for the wake thread event to be signalled so we can start some work
                            _pool.WaitForWork(_wakeThread);
                            // stop now?
                            if (_stop != 0)
                            {
                                break;
                            }
                            // record the start time
                            startTicks = WorkerPool.Ticks;
                            // NO work to execute? (just in case--I don't think this can ever really happen)
                            if (_actionToPerform == null)
                            {
                                // nothing to do, so we're done
                                completionTicks = WorkerPool.Ticks;
                            }
                            else
                            {
                                RunWithLogException(
                                    () =>
                                    {
                                        // perform the work
                                        _actionToPerform.DynamicInvoke();
                                    },
                                    "An unexpected error occured during a '{0}' thread worker dynamic invocation: {1}");
                                // mark the time
                                completionTicks = WorkerPool.Ticks;
                            }
                            // finish work in succes case
                            FinishWork();
                            // record statistics
                            long invokeTime = _invokeTicks;
                            // ignore how long it took if we never got invoked
                            if (invokeTime > 0)
                            {
                                long invokeTicks = startTicks - invokeTime;
                                long executionTicks = completionTicks - startTicks;
                                if (invokeTicks > maxInvokeTicks)
                                {
                                    maxInvokeTicks = invokeTicks;
                                    InterlockedHelper.Max(ref sSlowestInvocation, invokeTicks);
                                }
                            }
                        }
                        System.Diagnostics.Debug.WriteLine("Exiting '" + _id + "' worker thread: max invocation wait=" + (maxInvokeTicks * 1000.0f / WorkerPool.TicksPerSecond).ToString() + "ms");
                    }, "Exception in '{0}' WorkerFunc: {1}");
            }
            finally
            {
                // wait for up to one second for the stopper to be done accessing us
                _allowDisposal.WaitOne(1000);
                Dispose();
            }
        }

        private void FinishWork()
        {
            // we're done with the work, so get rid of it so we're ready for the next one
            System.Threading.Interlocked.Exchange(ref _actionToPerform, null);
            // not stopping?
            if (_stop == 0)
            {
                // release the worker back to the pool
                _pool.OnWorkerReady(this);
            }
        }

        private static long sSlowestInvocation;
    }
    public class WorkerPool : IDisposable
    {
        private const int HighThreadCountWarningEnvironmentTicks = 60 * 60 * 1000;
        private readonly static int LogicalCpuCount = GetProcessorCount();
#if DEBUG
        private readonly static int PoolChunkSize = 1;
        private const int RetireCheckAfterCreationMilliseconds = 60 * 1000;
        private const int RetireCheckMilliseconds = 3 * 1000;
        private const int MaxThreadsPerLogicalCpu = 50;
        private readonly bool executeDisposalCheck = false;
#else
        private readonly static int PoolChunkSize = LogicalCpuCount;
        private const int RetireCheckAfterCreationMilliseconds = 5 * 60 * 1000;
        private const int RetireCheckMilliseconds = 60 * 1000;
        private const int MaxThreadsPerLogicalCpu = 150;
#endif

        private readonly static int MaxWorkerThreads = LogicalCpuCount * MaxThreadsPerLogicalCpu;
        private static List<WorkerPool> _Pools = new List<WorkerPool>();

        private string _poolName;   // constant after construction
        private Thread _poolMasterThread;
        private ThreadPriority _poolThreadPriority;
        private ManualResetEvent _wakePoolMasterThread; // controls the master thread waking up
        private AutoResetEvent _retireThreads;          // controls the master thread retiring threads early

        // a list of workers that are ready to do some work
        private InterlockedStack<Worker> _readyWorkerList = new InterlockedStack<Worker>();
        // an incrementing worker id used to name the worker threads
        private int _workerId;
        // the number of busy workers (interlocked)
        private int _busyWorkers;
        // the total number of workers
        private int _workers;
        // the peak concurrent usage since the last change in worker count
        private int _peakConcurrentUsageSinceLastRetirementCheck;
        // the most workers we've ever seen (interlocked)
        private int _workersHighWaterMark;
        // the last time we warned about too many threads (interlocked)
        private int _highThreadsWarningTickCount;
        // the last time we had to execute something synchronously because no workers were available (interlocked)
        private int _lastSynchronousExecutionTicks;

#if DEBUG
        private Random _random = new Random(0);
#endif

        private static WorkerPool _DefaultWorkerPool = new WorkerPool("Default", ThreadPriority.Normal, false);

        /// <summary>
        /// Gets the default <see cref="WorkerPool"/>, one with normal priorities.
        /// </summary>
        public static WorkerPool Default { get { return _DefaultWorkerPool; } }

        /// <summary>
        /// Gets the number of currently busy workers.
        /// </summary>
        public int BusyWorkers { get { return _busyWorkers; } }

        public static void Stop()
        {
            List<WorkerPool> pools;
            lock (_Pools)
            {
                pools = new List<WorkerPool>(_Pools);
            }
            foreach (WorkerPool pool in pools)
            {
                pool.Dispose();
            }
        }

        /// <summary>
        /// Gets the name of the pool.
        /// </summary>
        public string Name { get { return _poolName; } }

        /// <summary>
        /// Gets the ticks used internally for performance tracking.
        /// </summary>
        public static long Ticks
        {
            get
            {
                return Stopwatch.GetTimestamp();
            }
        }

        /// <summary>
        /// Gets the number of ticks per second used internally for performance tracking.
        /// </summary>
        public static long TicksPerSecond
        {
            get
            {
                return Stopwatch.Frequency;
            }
        }

        private static int GetProcessorCount()
        {
            try
            {
                return Environment.ProcessorCount;
            }
            catch
            {
                // default to sixteen if we can't query
                return 16;
            }
        }

        public WorkerPool(string poolName, ThreadPriority priority = ThreadPriority.Normal, bool executeDisposalCheck = true)
            : this(poolName, priority)
        {
#if DEBUG
            this.executeDisposalCheck = executeDisposalCheck;
#endif
        }

        public WorkerPool(string poolName, ThreadPriority priority = ThreadPriority.Normal)
        {
            try
            {
#if DEBUG
                // suppress finalization
                System.GC.SuppressFinalize(this);
#endif
                // save the pool name and priority
                _poolName = poolName;
                _poolThreadPriority = priority;
                // initialize some workers
                for (int i = 0; i < PoolChunkSize; ++i)
                {
                    Worker worker = CreateWorker();
                    System.Diagnostics.Debug.Assert(!worker.IsBusy);
                    _readyWorkerList.Push(worker);
                }
                _workersHighWaterMark = _readyWorkerList.Count;
                _wakePoolMasterThread = new ManualResetEvent(false);
                _retireThreads = new AutoResetEvent(false);
                _poolMasterThread = new Thread(new ThreadStart(PoolMaster));
                _poolMasterThread.Name = "Worker Pool '" + poolName + "' Master";
                _poolMasterThread.IsBackground = true;
                _poolMasterThread.Priority = (priority >= ThreadPriority.Highest) ? priority : (priority + 1);
                _poolMasterThread.Start();
                // give up our timeslice so the workers and the pool master thread can start
                Thread.Sleep(0);
                // NOTE: We used to have a loop here that waited for at least half the threads to start
                // That loop appears to deadlock under certain conditions such as when called during static construction
                // I'm not sure why it worked previously but no longer does, but I think this way will probably be fine
                lock (_Pools)
                {
                    _Pools.Add(this);
                }
#if DEBUG
                // reregister for finalization
                System.GC.ReRegisterForFinalize(this);
#endif
            }
            catch
            {
                Dispose();
                throw;
            }
        }


#if DEBUG
        private string fStackAtConstruction = new StackTrace().ToString();
        ~WorkerPool()
        {
            if (executeDisposalCheck)
            {
                Debug.Fail(String.Format(System.Globalization.CultureInfo.InvariantCulture, "Failed to dispose/close WorkerPool " + _poolName + ": Stack at construction was:\n{0}", fStackAtConstruction));
            }

            Dispose();
        }
#endif

        public void Dispose()
        {
            // signal the master thread to stop
            using (System.Threading.Interlocked.Exchange(ref _wakePoolMasterThread, null))
            {
                // wait for the pool master thread to recieve the message and shut down
                _poolMasterThread.Join();
            }
            // sleep up to 100ms while there are busy workers
            for (int count = 0; _busyWorkers > 0 && count < 10; ++count)
            {
                System.Threading.Thread.Sleep(10);
            }
            // retire all the the workers (now that the master pool thread has stopped, it shouldn't be able to start any more workers)
            while (RetireOneWorker()) { }
            System.Diagnostics.Debug.Assert(_readyWorkerList.Count == 0);
            System.Diagnostics.Debug.Assert(this._busyWorkers == 0);
            System.Diagnostics.Debug.Assert(this._workers == 0);
            // remove us from the master list of pools
            lock (_Pools)
            {
                _Pools.Remove(this);
            }
#if DEBUG
            // we're being disposed properly, so no need to call the finalizer
            GC.SuppressFinalize(this);
#endif
        }

        private string ThreadName(int thread)
        {
            return "Worker Pool '" + _poolName + "' " + ("Thread " + (thread + 1).ToString());
        }

        internal void WaitForWork(ManualResetEvent wakeWorkerThreadEvent)
        {
            RunWithLogException(() =>
            {
                // wait (forever) for an operation or a stop
                wakeWorkerThreadEvent.WaitOne();
                // reset the event so we're ready to go again
                wakeWorkerThreadEvent.Reset();

            }, "Unexpected '{0}' Worker Thread Exception: {1}");
        }

        private void RunWithLogException(Action a, string exceptionFormatString)
        {
            try
            {
                a();
            }
            catch (Exception e)
            {
                if (!(e is ThreadAbortException)) Program.Error(String.Format(System.Globalization.CultureInfo.InvariantCulture, exceptionFormatString, _poolName, e.ToString()));
            }
        }

        private Worker CreateWorker()
        {
            int id = System.Threading.Interlocked.Increment(ref _workerId);
            // initialize the worker (starts their threads)
            Worker worker = new Worker(this, ThreadName(id), _poolThreadPriority);
            System.Threading.Interlocked.Increment(ref _workers);
            return worker;
        }

        private static int TimeElapsed(int startTime, int endTime)
        {
            unchecked
            {
                return endTime - startTime;
            }
        }

        /// <summary>
        /// Tells the thread pool master thread to retire threads if possible because the caller thinks a massively parallel operation has finished and the threads are no longer needed.
        /// </summary>
        public void RetireThreads()
        {
            // trigger thread retirement
            _retireThreads.Set();
            // wake up the master thread so it does it immediately
            _wakePoolMasterThread.Set();
            // give up our timeslice so it can run
            System.Threading.Thread.Sleep(0);
        }

        private void PoolMaster()
        {
            RunWithLogException(
                () =>
                {
                    int lastRetirementTicks = Environment.TickCount;
                    int lastCreationTicks = Environment.TickCount;
                    // loop forever (we're a background thread, so if the process exits, no problem!)
                    for (bool stop = false; !stop;)
                    {
                        RunWithLogException(
                            () =>
                            {
                                ManualResetEvent wakePoolMasterThread = _wakePoolMasterThread;
                                // have we already been disposed of?
                                if (wakePoolMasterThread == null)
                                {
                                    // exit now!
                                    stop = true;
                                    return;
                                }
                                // sleep for up to one second or until we are awakened
                                if (wakePoolMasterThread.WaitOne(1000))
                                {
                                    wakePoolMasterThread.Reset();
                                }
                                // do we need to add more workers?
                                int totalWorkers = _workers;
                                int readyWorkers = _readyWorkerList.Count;
                                int busyWorkers = _busyWorkers;
                                if (readyWorkers <= Math.Min(1, PoolChunkSize / 2))
                                {
                                    // too many workers already?
                                    if (totalWorkers > MaxWorkerThreads)
                                    {
                                        // have we NOT already logged that we are using an insane number of threads in the past 60 minutes?
                                        int now = Environment.TickCount;
                                        int lastWarning = _highThreadsWarningTickCount;
                                        if (TimeElapsed(lastWarning, now) > HighThreadCountWarningEnvironmentTicks)
                                        {
                                            // race to log a warning--did we win the race?
                                            if (System.Threading.Interlocked.CompareExchange(ref _highThreadsWarningTickCount, now, lastWarning) == lastWarning)
                                            {
                                                Program.Log("'" + _poolName + "' Worker Pool Warning: " + (readyWorkers + busyWorkers).ToString() + " threads exceeds the limit of " + MaxWorkerThreads.ToString() + " for this computer!");
                                            }
                                        }
                                        // now we will just carry on because we will not expand beyond the number of threads we have now
                                    }
                                    else
                                    {
                                        // initialize some workers
                                        for (int i = 0; i < PoolChunkSize; ++i)
                                        {
                                            // create a new worker
                                            Worker worker = CreateWorker();
                                            System.Diagnostics.Debug.Assert(!worker.IsBusy);
                                            _readyWorkerList.Push(worker);
                                        }
                                        // update the high water mark if needed
                                        InterlockedHelper.Max(ref _workersHighWaterMark, _workers);
                                        System.Diagnostics.Debug.WriteLine(_poolName + " workers:" + _workers.ToString());
                                        // record the expansion time so we don't retire anything for at least a minute
                                        lastCreationTicks = Environment.TickCount;
                                    }
                                }
                                // enough ready workers that we could get by with less 
                                else if (readyWorkers > Math.Max(3, PoolChunkSize * 2))
                                {
                                    int ticksNow = Environment.TickCount;
                                    int ticksSinceCreation = TimeElapsed(lastCreationTicks, ticksNow);
                                    int ticksSinceSynchronousExecution = TimeElapsed(_lastSynchronousExecutionTicks, ticksNow);
                                    int ticksSinceRetirement = TimeElapsed(lastRetirementTicks, ticksNow);
                                    bool retireThreadsImmediately = !_retireThreads.WaitOne(0);
                                    // haven't had a synchronous execution or new worker creation or removed any workers for a while?
                                    if (retireThreadsImmediately || (
                                            ticksSinceCreation > RetireCheckAfterCreationMilliseconds &&
                                            ticksSinceSynchronousExecution > RetireCheckAfterCreationMilliseconds &&
                                            ticksSinceRetirement > RetireCheckMilliseconds
                                        ))
                                    {
                                        // was the highest concurrency since the last time we checked such that we didn't need many of the threads?
                                        if (retireThreadsImmediately || _peakConcurrentUsageSinceLastRetirementCheck < Math.Max(1, totalWorkers - PoolChunkSize))
                                        {
                                            // remove some workers
                                            for (int i = 0; i < PoolChunkSize; ++i)
                                            {
                                                // retire one worker--did we remove too many (this could happen if there was a sudden upsurge in worker need while we were stopping workers)
                                                if (!RetireOneWorker())
                                                {
                                                    // bail out now--we removed too many (this could happen if there was a sudden upsurge in worker need while we were stopping workers)
                                                    break;
                                                }
                                            }
                                            System.Diagnostics.Debug.WriteLine(_poolName + " workers:" + _workers.ToString());
                                            // record when we retired threads
                                            lastRetirementTicks = Environment.TickCount;
                                        }
                                        // reset the concurrency
                                        System.Threading.Interlocked.Exchange(ref _peakConcurrentUsageSinceLastRetirementCheck, 0);
                                    }
                                }
                            }, "'{0}' Pool Master Exception: {1}");
                    }
                }, "Critical '{0}' Pool Master Exception: {1}");
        }

        private bool RetireOneWorker()
        {
            Worker workerToStop = _readyWorkerList.Pop();
            System.Diagnostics.Debug.Assert(workerToStop == null || !workerToStop.IsBusy);
            // none left?
            if (workerToStop == null)
            {
                return false;
            }
            System.Threading.Interlocked.Decrement(ref _workers);
            workerToStop.Stop();
            return true;
        }

        internal void OnWorkerReady(Worker worker)
        {
            // add the worker back to the ready worker list
            System.Diagnostics.Debug.Assert(!worker.IsBusy);
            _readyWorkerList.Push(worker);
        }
        /// <summary>
        /// Asynchronously runs the specified action.
        /// </summary>
        /// <param name="work">The action to perform asynchronously.</param>
        /// <returns><b>true</b> if the item was scheduled to run asynchronously, or <b>false</b> if no worker threads were available and the operation was run synchronously on a dedicated thread.</returns>
        public bool RunAsync(Action work)
        {
        Retry:
            bool synchronous;
            // try to get a ready thread
            Worker worker = _readyWorkerList.Pop();
            System.Diagnostics.Debug.Assert(worker == null || !worker.IsBusy);
            // no ready workers and not too many workers already?
            if ((synchronous = (worker == null)))
            {
                // too many busy workers already?
                if (_busyWorkers > MaxWorkerThreads)
                {
                    // just wait for things to change--we're overloaded and we're not going to make things better by starting up more threads!
                    System.Threading.Thread.Sleep(100);
                    goto Retry;
                }
                // create a new one right now--it will be added to the ready worker list when we're done
                worker = CreateWorker();
                // record this miss
                System.Threading.Interlocked.Exchange(ref _lastSynchronousExecutionTicks, Environment.TickCount);
                // wake the pool master immediately
                _wakePoolMasterThread.Set();
            }
            worker.RunAsync(
                delegate ()
                {
                    // we now have a worker that is busy
                    System.Threading.Interlocked.Increment(ref _busyWorkers);
                    // keep track of the maximum concurrent usage
                    InterlockedHelper.Max(ref _peakConcurrentUsageSinceLastRetirementCheck, _busyWorkers);
                    try
                    {
                        work();
                    }
                    finally
                    {
                        // the worker is no longer busy
                        System.Threading.Interlocked.Decrement(ref _busyWorkers);
                    }
                });
            return !synchronous;
        }
        /// <summary>
        /// Runs an action asynchronously.  No assumption is made about the identity under which the action is run.
        /// </summary>
        /// <param name="action">The action to perform asynchronously.</param>
        /// <param name="exceptionCallback">An optional second action to call if the first action throws an exception.</param>
        /// <returns><b>true</b> if the item was scheduled to run asynchronously, or <b>false</b> if no worker threads were available and the operation was run synchronously.</returns>
        /// <remarks>Does not throw if the specified action throws an exception.</remarks>
        public bool RunAsync(Action work, Action<Exception> exceptionCallback = null)
        {
        Retry:
            bool synchronous;
            // try to get a ready thread
            Worker worker = _readyWorkerList.Pop();
            System.Diagnostics.Debug.Assert(worker == null || !worker.IsBusy);
            // no ready workers and not too many workers already?
            if ((synchronous = (worker == null)))
            {
                // too many busy workers already?
                if (_busyWorkers > MaxWorkerThreads)
                {
                    // just wait for things to change--we're overloaded and we're not going to make things better by starting up more threads!
                    System.Threading.Thread.Sleep(100);
                    goto Retry;
                }
                // create a new one right now--it will be added to the ready worker list when we're done
                worker = CreateWorker();
                // record this miss
                System.Threading.Interlocked.Exchange(ref _lastSynchronousExecutionTicks, Environment.TickCount);
                // wake the pool master immediately
                _wakePoolMasterThread.Set();
            }
            worker.RunAsync(
                delegate ()
                {
                    // we now have a worker that is busy
                    System.Threading.Interlocked.Increment(ref _busyWorkers);
                    // keep track of the maximum concurrent usage
                    InterlockedHelper.Max(ref _peakConcurrentUsageSinceLastRetirementCheck, _busyWorkers);
                    try
                    {
                        work();
                    }
                    catch (Exception ex)
                    {
                        if (exceptionCallback != null)
                        {
                            exceptionCallback(ex);
                        }
                        else
                        {
                            Program.Error("ex, Exception running '" + _poolName + "' pool async operation with no exception handler");
                        }
                    }
                    finally
                    {
                        // the worker is no longer busy
                        System.Threading.Interlocked.Decrement(ref _busyWorkers);
                    }
                });
            return !synchronous;
        }
        public static bool IsIdleWorkerPoolThread(System.Diagnostics.StackTrace trace)
        {
            // not blocked?
            if (trace.GetFrame(0).GetMethod().Name != "WaitOneNative" && trace.GetFrame(0).GetMethod().Name != "WaitOne" && trace.GetFrame(1).GetMethod().Name != "InternalWaitOne")
            {
                return false;
            }
            // skip more blocking functions
            int frame = 2;
            while (trace.GetFrame(frame).GetMethod().Name == "WaitOne" || trace.GetFrame(frame).GetMethod().Name == "InternalWaitOne" || trace.GetFrame(frame).GetMethod().Name.StartsWith("<WaitForWork>") || trace.GetFrame(frame).GetMethod().Name == "WaitForWork" || trace.GetFrame(frame).GetMethod().Name == "RunWithLogException" || trace.GetFrame(frame).GetMethod().Name.StartsWith("<WorkerFunc>"))
            {
                ++frame;
            }
            // worker function or worker pool master?
            if ((trace.GetFrame(frame).GetMethod().Name == "WorkerFunc" &&
                trace.GetFrame(frame).GetMethod().DeclaringType.ToString() == "Agilix.Shared.Worker") ||
                (trace.GetFrame(frame).GetMethod().Name == "PoolMaster" &&
                trace.GetFrame(frame).GetMethod().DeclaringType.ToString() == "Agilix.Shared.WorkerPool"))
            {
                return true;
            }
            return false;
        }
    }

    public class IntrusiveSinglyLinkedListNode
    {
        internal IntrusiveSinglyLinkedListNode _nextNode;
    }

    /// <summary>
    /// A thread-safe intrusive stack.  All methods are thread-safe.
    /// </summary>
    /// <typeparam name="TYPE">The type of item to store in the stack.  Must inherit from <see cref="IntrusiveSinglyLinkedListNode"/>.</typeparam>
    public class InterlockedStack<TYPE> where TYPE : IntrusiveSinglyLinkedListNode
    {
        private int _count;
        private int _popping;
        private IntrusiveSinglyLinkedListNode _root; // we use a circular list instead of null because it allows us to detect if the node has been removed without traversing the entire list, because all removed nodes will have their next node set to null, while no nodes in the list will have their next node set to null.

        /// <summary>
        /// Constructs the list.
        /// </summary>
        public InterlockedStack()
        {
            _root = new IntrusiveSinglyLinkedListNode();
            _root._nextNode = _root;
            Validate();
        }

        /// <summary>
        /// Pushes the specified node onto the top of the stack.
        /// </summary>
        /// <param name="node">The node to push onto the top of the stack.</param>
        /// <remarks>The specified node must NOT already be in another stack and must not be simultaneously added or removed by another thread.</remarks>
        public void Push(TYPE node)
        {
            Validate();
            // already in another list?
            if (node._nextNode != null)
            {
                Validate();
                throw new ApplicationException("The specified node is already in a list!");
            }
            // get the current top, which will be the old one if we succeed
            IntrusiveSinglyLinkedListNode oldTop = _root._nextNode;
            // loop until we win the race to insert us at the top
            do
            {
                // set this node's next node as the node we just got (we'll change it if we don't win the race)
                node._nextNode = oldTop;
                // race to place this node in as the top node--did we win?
                if (System.Threading.Interlocked.CompareExchange(ref _root._nextNode, node, oldTop) == oldTop)
                {
                    // increment the node count (it will be one off temporarily, but since the list can change at any time, nobody can reliably detect this anyway)
                    System.Threading.Interlocked.Increment(ref _count);
                    Validate();
                    return;
                }
                // else do it all again, get the current top, which will be the old one if we succeed
                oldTop = _root._nextNode;
            } while (true);
        }

        /// <summary>
        /// Pops off the top node on the stack and returns it.
        /// </summary>
        /// <returns>The top node, or <b>null</b> if there are no items in the stack.</returns>
        public TYPE Pop()
        {
            Validate();
            IntrusiveSinglyLinkedListNode ret;
            // wait until no other threads are popping
            while (System.Threading.Interlocked.CompareExchange(ref _popping, 1, 0) != 0) { }
            try
            {
                // loop while there something in the list to remove
                while ((ret = _root._nextNode) != _root)
                {
                    // get the new top
                    IntrusiveSinglyLinkedListNode newTop = ret._nextNode;
                    // another thread better not have already popped this node!
                    System.Diagnostics.Debug.Assert(newTop != null);
                    // try to exchange the new top into the top position--did we win the race against a pusher?
                    if (System.Threading.Interlocked.CompareExchange(ref _root._nextNode, newTop, ret) == ret)
                    {
                        // decrement the node count (it will be one off temporarily, but since the list can change at any time, nobody can reliably detect this anyway)
                        System.Threading.Interlocked.Decrement(ref _count);
                        // clear the next pointer because we are no longer in the list
                        ret._nextNode = null;
                        Validate();
                        // return the node that was removed
                        return (TYPE)ret;
                    }
                    // try again from the beginning
                }
            }
            finally
            {
                // we are no longer popping
                System.Threading.Interlocked.Exchange(ref _popping, 0);
            }
            Validate();
            // nothing there to pop!
            return null;
        }

        [Conditional("DEBUG")]
        private void Validate()
        {
            // we better still have a valid root!
            System.Diagnostics.Debug.Assert(_root is IntrusiveSinglyLinkedListNode && !(_root is TYPE));
            // root node better not have a null next pointer
            System.Diagnostics.Debug.Assert(_root._nextNode != null);
        }

        /// <summary>
        /// Clear all items from the list.  May remove nodes that are added after this function starts, and items may be added before the function returns, but at some point before the call returns, the list will be empty.
        /// </summary>
        public void Clear()
        {
            Validate();
            // pop from the top until the list is empty
            while (Pop() != null)
            {
                // do nothing
            }
            Validate();
        }

        /// <summary>
        /// Gets the number of items in the list, which could change before if could be useful.
        /// </summary>
        public int Count
        {
            get
            {
                Validate();
                return _count;
            }
        }

    }
    /// <summary>
    /// A static class to hold enhanced functions for performing interlocked operations.
    /// </summary>
    public static class InterlockedHelper
    {
        /// <summary>
        /// Replaces the value with the specified value if the specified value is greater.
        /// </summary>
        /// <param name="valueReference">A reference to the value being manipulated.</param>
        /// <param name="possibleNewMax">The value to replace the value with if it is greater.</param>
        /// <returns>The new maximum value.</returns>
        public static int Max(ref int valueReference, int possibleNewMax)
        {
            int oldValue = valueReference;
            // loop attempting to put it in until we win the race
            while (possibleNewMax > oldValue)
            {
                // try to put in our value--did we win the race?
                if (oldValue == System.Threading.Interlocked.CompareExchange(ref valueReference, possibleNewMax, oldValue))
                {
                    // we're done and we were the new max
                    return possibleNewMax;
                }
                // update our value
                oldValue = valueReference;
            }
            // we're done and we were not the max value
            return oldValue;
        }
        /// <summary>
        /// Replaces the value with the specified value if the specified value is greater.
        /// </summary>
        /// <param name="valueReference">A reference to the value being manipulated.</param>
        /// <param name="possibleNewMax">The value to replace the value with if it is greater.</param>
        /// <returns>The new maximum value.</returns>
        public static long Max(ref long valueReference, long possibleNewMax)
        {
            long oldValue = valueReference;
            // loop attempting to put it in until we win the race
            while (possibleNewMax > oldValue)
            {
                // try to put in our value--did we win the race?
                if (oldValue == System.Threading.Interlocked.CompareExchange(ref valueReference, possibleNewMax, oldValue))
                {
                    // we're done and we were the new max
                    return possibleNewMax;
                }
                // update our value
                oldValue = valueReference;
            }
            // we're done and we were NOT the new max
            return oldValue;
        }
    }
}