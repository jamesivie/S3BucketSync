using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

/*
Copyright (c) 2016 James Ivie

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

This code may not be incorporated into any source or binary code distributed by Amazon or its subsidiareis, subcontractors, parent companies, etc. without a separate license.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
namespace S3BucketSync
{
    class Program
    {
        static private int _Abort = 0;       // interlocked
        static private int _SourceProcessingComplete = 0;       // interlocked
        static private BucketObjectsWindow _SourceBucketObjectsWindow;
        static private BucketObjectsWindow _TargetBucketObjectsWindow;
        static private State _State;    // only written during single-thread phase of startup
        static private TextWriter _Log;
        static private TextWriter _Error;
        static private string _StateFilePath;
        static private string _LogFilePath;
        static private string _ErrorFilePath;
        static private bool _Verbose;
        static private bool _GrantTargetOwnerFullControl;
        static private S3Grant _Grant;
        static private int _SourceObjectsReadThisRun;    // interlocked
        static private int _TargetObjectsReadThisRun;    // interlocked
        static private int _ObjectsProcessedThisRun;    // interlocked

        /// <summary>
        /// Gets the <see cref="State"/> object so we can save where we are and what we've accomplished.
        /// </summary>
        static public State State { get { return _State; } }


        static void Main(string[] args)
        {
            try
            {
                ThreadPool.SetMaxThreads(1000, 1000);
                string sourceRegionBucketAndPrefix = null;
                string targetRegionBucketAndPrefix = null;
                string commonPrefix = null;
                bool reset = false;
                // no arguments or not enough arguments?
                if (args.Length < 2)
                {
                    Console.WriteLine(@"
S3BucketSync Usage:
    S3BucketSync <source_region>:<source_bucket>[source_prefix] <target_region>:<target_bucket>[target_prefix] [common_prefix] [-b] [-v] [-g<account_email>]

Purpose:
    Copies or updates objects from the source bucket to the destination bucket if the object doesn't exist in the destination bucket or the Etag of the corresponding object does not match

Options:
    common_prefix is an optional prefix string that is appended to both the source_prefix and target_prefix just to save typing the same prefixes for two different buckets
    -b means restart at the beginning, ignore any saved state
    -v means output extra messages to see more details of what's going on
    -g grants the account represented by the email full rights
    -ofc grants the target account owner full control of their copy

Examples:
    S3BucketSync us-east-1:main us-west-2:backup /files
    S3BucketSync us-east-1:main/files us-west-2:backup/files
        synchronizes objects with keys starting with '/files' from the main bucket in the us-east-1 region to the backup bucket in the us-west-2 region
    S3BucketSync us-east-1:main us-west-2:backup/subpath /files
        synchronizes objects with keys starting with '/files' from the main bucket in the us-east-1 region to the backup bucket in the us-west-2 region, prepending '/subpath' to the keys as it goes
    S3BucketSync us-east-1:main us-west-2:backup -b
        synchronizes all objects from the main bucket in the us-east-1 region to the backup bucket in the us-west-2 region, ignoring any saved state from previous runs
    S3BucketSync us-east-1:main us-west-2:main -g it@agilix.com
        Give the account represented by the email it@agilix.com full access to the copied objects.

Interaction:
    ESC: exit (state is saved at the last fully-completed bucket, so you can restart near where you left off)
    F1: dump basic statistics
    F2: also basic state information (what various control threads are busy doing)
    F3: also dump all in-progress copies

Logging and Saved State:
    Errors are logged into a file named error.<region>_<bucket>_<source_prefix>.txt
    Everything output to the console is also logged into a file named log.<region>_<bucket>_<source_prefix>.txt
    State is saved as batches are completed in a file named state.<region>_<bucket>_<source_prefix>.bin
");
                    return;
                }
                // process args
                foreach (string argument in args)
                {
                    if (argument.StartsWith("-"))
                    {
                        if (argument.ToLowerInvariant().StartsWith("-b"))
                        {
                            reset = true;
                        }
                        if (argument.ToLowerInvariant().StartsWith("-v"))
                        {
                            _Verbose = true;
                        }
                        if (argument.ToLowerInvariant().StartsWith("-g"))
                        {
                            _Grant = CreateS3Grant(argument.Substring(2));
                        }
                        if (argument.ToLowerInvariant().StartsWith("-ofc"))
                        {
                            _GrantTargetOwnerFullControl = true;
                        }
                    }
                    else if (string.IsNullOrEmpty(sourceRegionBucketAndPrefix)) sourceRegionBucketAndPrefix = argument;
                    else if (string.IsNullOrEmpty(targetRegionBucketAndPrefix)) targetRegionBucketAndPrefix = argument;
                    else if (string.IsNullOrEmpty(commonPrefix)) commonPrefix = argument;
                }
                // a common prefix (append to source and destination)
                if (!string.IsNullOrEmpty(commonPrefix))
                {
                    sourceRegionBucketAndPrefix += commonPrefix;
                    targetRegionBucketAndPrefix += commonPrefix;
                }
                string currentDirectory = Environment.CurrentDirectory;
                _StateFilePath = Path.Combine(currentDirectory, "state." + sourceRegionBucketAndPrefix.Replace(":", "_").Replace("/", "_") + ".bin");
                _LogFilePath = Path.Combine(currentDirectory, "log." + sourceRegionBucketAndPrefix.Replace(":", "_").Replace("/", "_") + ".txt");
                _ErrorFilePath = Path.Combine(currentDirectory, "error." + sourceRegionBucketAndPrefix.Replace(":", "_").Replace("/", "_") + ".txt");
                // open the log file and error file
                using (_Log = new StreamWriter(_LogFilePath, true, Encoding.UTF8))
                using (_Error = new StreamWriter(_ErrorFilePath, true, Encoding.UTF8))
                {
                    Program.Log(Environment.NewLine + "Start Sync from " + sourceRegionBucketAndPrefix + " to " + targetRegionBucketAndPrefix);
                    // possible resume?
                    if (!reset)
                    {
                        // read the previous state (if it's a state for the same source and target)
                        State resumeState = State.Read(_StateFilePath, sourceRegionBucketAndPrefix, targetRegionBucketAndPrefix);
                        if (resumeState != null)
                        {
                            _State = resumeState;
                            Program.Log("Resuming previous run: " + State.ToString());
                        }
                    }
                    // no state yet?
                    if (_State == null)
                    {
                        // use a default state
                        _State = new State(sourceRegionBucketAndPrefix, targetRegionBucketAndPrefix, (_Grant == null ? "none" : _Grant.Grantee.EmailAddress), _GrantTargetOwnerFullControl);
                    }
                    // initialize the source bucket objects window
                    _SourceBucketObjectsWindow = new BucketObjectsWindow(sourceRegionBucketAndPrefix, _State.SourceBatchId, _State.LastKeyOfLastBatchCompleted);
                    // initialize the target bucket objects window
                    _TargetBucketObjectsWindow = new BucketObjectsWindow(targetRegionBucketAndPrefix, new BatchIdCounter(), _State.LastKeyOfLastBatchCompleted, _Grant, _GrantTargetOwnerFullControl);
                    // fire up a thread to dump the status every few seconds
                    Thread statusDumper = new Thread(new ThreadStart(StatusDumper));
                    statusDumper.Name = "StatusDumper";
                    statusDumper.Priority = ThreadPriority.AboveNormal;
                    statusDumper.IsBackground = true;  // if we abort by hitting a key, don't wait for this thread to finish
                    statusDumper.Start();
                    // fire up a thread to expand the target window
                    Thread targetExpander = new Thread(new ThreadStart(TargetWindowRangeSynchronizer));
                    targetExpander.Name = "TargetExpander";
                    targetExpander.Priority = ThreadPriority.AboveNormal;
                    targetExpander.IsBackground = true;  // if we abort by hitting a key, don't wait for this thread to finish
                    targetExpander.Start();
                    // fire up a thread to spawn processing tasks
                    Thread processor = new Thread(new ThreadStart(Processor));
                    processor.Name = "Processor";
                    processor.Priority = ThreadPriority.AboveNormal;
                    processor.IsBackground = true;  // if we abort by hitting a key, don't wait for this thread to finish
                    processor.Start();
                    // bump up our priority (we're the most important because we feed everything else)
                    Thread.CurrentThread.Priority = ThreadPriority.Highest;
                    // keep making requests for more lists of objects until we have 100 buffered (100,000 pending objects)
                    int batchesToQueue = 100;
                    // loop until we have processed all the objects or a key is pressed
                    while (!_SourceBucketObjectsWindow.LastBatchHasBeenRead)
                    {
                        if (ExitKeyPressed()) return;
                        // loop until we have the desired number of batches in the window or we hit the end
                        while (_SourceBucketObjectsWindow.BatchesQueued < batchesToQueue && !_SourceBucketObjectsWindow.LastBatchHasBeenRead)
                        {
                            if (ExitKeyPressed()) return;
                            // read the next batch
                            _State.RecordQueries(true);
                            using (TrackOperation("MAIN: Reading batch " + (_SourceBucketObjectsWindow.LastQueuedBatchId + 1).ToString()))
                            {
                                int objectsRead = _SourceBucketObjectsWindow.ReadNextBatch();
                                Interlocked.Add(ref _SourceObjectsReadThisRun, objectsRead);
                            }
                        }
                        // too full?
                        if (_SourceBucketObjectsWindow.BatchesQueued >= batchesToQueue)
                        {
                            using (TrackOperation("MAIN: Waiting for processing"))
                            {
                                Thread.Sleep(100);
                            }
                        }
                    }
                    using (TrackOperation("MAIN: Queueing complete: Waiting for processing"))
                    {
                        // we're done getting all the objects, but we still need to finish processing them
                        while (!processor.Join(100))
                        {
                            if (ExitKeyPressed())
                            {
                                // log the end state
                                Program.Log(_State.ToString());
                                return;
                            }
                        }
                    }
                    // the source processing is now complete
                    Interlocked.Exchange(ref _SourceProcessingComplete, 1);
                    _State.Delete(_StateFilePath);
                    Program.Log("");
                    Program.Log("This run synchronized " + _ObjectsProcessedThisRun + "/" + _SourceObjectsReadThisRun + " objects against " + _TargetObjectsReadThisRun + " objects: ");
                    Program.Log(_State.Report());
                }
            }
            catch (Exception ex)
            {
                Debugger.Break();
                Console.WriteLine("Fatal Error: " + ex.ToString());
            }
        }
        /// <summary>
        /// Creates an S3 Grant for the given account email address.
        /// </summary>
        private static S3Grant CreateS3Grant(string accountEmailAddress)
        {
            return new S3Grant
            {
                Grantee = new S3Grantee { EmailAddress = accountEmailAddress },
                Permission = S3Permission.FULL_CONTROL
            };
        }
        /// <summary>
        /// A static function that loops expanding the target window as needed.
        /// </summary>
        static void StatusDumper()
        {
            while (true)
            {
                try
                {
                    // wait for a bit
                    Thread.Sleep(5000);
                    // write out the status
                    string state = Program.State.ToString();
                    state += " BQ:" + _SourceBucketObjectsWindow.BatchesQueued.ToString();
                    state += " BP:" + _batchesProcessing.ToString();
                    state += " CP:" + _TargetBucketObjectsWindow.CopiesInProgress.ToString();
                    Console.WriteLine(state);
                }
                catch (Exception e)
                {
                    Program.Error("Exception dumping status: " + e.ToString());
                }
            }
        }
        /// <summary>
        /// A static function that loops expanding the target window to follow the source items being processed.
        /// </summary>
        static void TargetWindowRangeSynchronizer()
        {
            string lastTestedSourceWindowLeastKey = null;
            // loop until there are no more source items to process
            while (_SourceProcessingComplete == 0 && _Abort == 0)
            {
                try
                {
                    // do we have a source bucket range to compare to (we may get here before the first source batch has been read)
                    if (!string.IsNullOrEmpty(_SourceBucketObjectsWindow.UnprefixedGreatestKey))
                    {
                        // do we need to expand the target window?
                        if (String.CompareOrdinal(_SourceBucketObjectsWindow.UnprefixedGreatestKey, _TargetBucketObjectsWindow.UnprefixedGreatestKey) > 0)
                        {
                            int queries = 0;
                            int objectsRead = 0;
                            using (TrackOperation("RANGESYNC: Expanding target window to include batch " + _TargetBucketObjectsWindow.LastQueuedBatchId.ToString() + " (" + _TargetBucketObjectsWindow.UnprefixedGreatestKey + " to " + _SourceBucketObjectsWindow.UnprefixedGreatestKey + ")"))
                            {
                                do
                                {
                                    objectsRead += _TargetBucketObjectsWindow.ReadNextBatch();
                                    if (objectsRead > 0) ++queries;
                                } while (String.CompareOrdinal(_SourceBucketObjectsWindow.UnprefixedGreatestKey, _TargetBucketObjectsWindow.UnprefixedGreatestKey) > 0 && _Abort == 0);
                            }
                            Interlocked.Add(ref _TargetObjectsReadThisRun, objectsRead);
                            _State.RecordQueries(false, queries);
                            // don't sleep, we may need to expand more right now!
                            continue;
                        }
                        // do we need to shrink the target window?
                        else if (_SourceBucketObjectsWindow.UnprefixedLeastKey != lastTestedSourceWindowLeastKey && String.CompareOrdinal(_SourceBucketObjectsWindow.UnprefixedLeastKey, _TargetBucketObjectsWindow.UnprefixedLeastKey) > 0)
                        {
                            lastTestedSourceWindowLeastKey = _SourceBucketObjectsWindow.UnprefixedLeastKey;
                            using (TrackOperation("RANGESYNC: Shrinking target window to " + lastTestedSourceWindowLeastKey))
                            {
                                _TargetBucketObjectsWindow.ShrinkWindow(lastTestedSourceWindowLeastKey);
                            }
                        }
                    }
                    using (TrackOperation("RANGESYNC: Waiting for work"))
                    {
                        // wait a bit so we don't monopolize the CPU
                        Thread.Sleep(100);
                    }
                }
                catch (Exception e)
                {
                    Program.Error("Exception synchronizing target window: " + e.ToString());
                }
            }
        }
        static int _batchesProcessing;
        /// <summary>
        /// A static function that loops processing batches until we're ready to exist and all batches have been processed.
        /// </summary>
        static void Processor()
        {
            // start with a completed task (we use this to make sure we never bookmark a batch as complete until all previous batches are complete)
            Task previousBatchCompleted = Task.FromResult(0);
            System.Diagnostics.Debug.Assert(previousBatchCompleted.IsCompleted);
            // loop until we've processed all the items
            while (_Abort == 0)
            {
                try
                {
                    // wait for the previous previous batch to complete before we start the next one
                    using (TrackOperation("PROCESS: Waiting for batch processing and copies to complete before continuing"))
                    {
                        while (_batchesProcessing * 100 + _TargetBucketObjectsWindow.CopiesInProgress > 2000)
                        {
                            Thread.Sleep(100);
                            if (_Abort != 0) return;
                        }
                    }
                    // only one batch is in process now, so continue
                    string sourcePrefix = _SourceBucketObjectsWindow.Prefix;
                    BucketObjectsWindow.Batch batch;
                    batch = _SourceBucketObjectsWindow.DequeueBatch();
                    // no more batches to process that have been queued yet?
                    if (batch == null || batch.Response.S3Objects.Count < 1)
                    {
                        // has the last batch been read?  stop now, we're done!
                        if (_SourceBucketObjectsWindow.LastBatchHasBeenRead)
                        {
                            using (TrackOperation("PROCESS: Complete: waiting for pending copies"))
                            {
                                previousBatchCompleted.Wait();
                            }
                            break;
                        }
                        using (TrackOperation("PROCESS: Waiting for batches to queue"))
                        {
                            // wait for a bit and try again
                            Thread.Sleep(100);
                        }
                        continue;
                    }
                    // get a copy of the previous task
                    Task previousBatchCompletedCopy = previousBatchCompleted;
                    // we are now processing this batch (do this synchronously in this loop to make sure that we're keeping track of the number of batch processing tasks that have been issued, not the number that have started processing--this should help prevent later batches from superceding earlier batches)
                    Interlocked.Increment(ref _batchesProcessing);
                    // create a task to check all the objects, queue copy operations if needed, and wait until the batch is completely synchronized
                    Task batchCompleteTask = Task.Run(async () =>
                        {
                            try
                            {
                                List<Task> tasks = new List<Task>();
                                // loop for retries (the actual number of retries should be controlled inside the loop)
                                for (int retry = 0; retry < 10000; ++retry)
                                {
                                    try
                                    {
                                        string retryString = ((retry > 0) ? "" : (" retry " + retry.ToString()));
                                        // get the last (greatest) key in the batch
                                        string lastKey = batch.GreatestKey;
                                        using (TrackOperation("PROCESS: Waiting for target window to include " + lastKey + retryString))
                                        {
                                            // wait until the target window ready to process all the items in this batch (it almost always should be ready to go already)
                                            while (String.CompareOrdinal(lastKey, _TargetBucketObjectsWindow.UnprefixedGreatestKey) > 0)
                                            {
                                                await Task.Delay(100);
                                                if (_Abort != 0) return;
                                            }
                                        }
                                        DateTime modificationCutoffTime = DateTime.UtcNow.AddMinutes(-15);
                                        using (TrackOperation("PROCESS: Checking batch " + batch.BatchId + retryString))
                                        {
                                            List<S3Object> objects = batch.Response.S3Objects;
                                            int count = objects.Count;
                                            for (int n = 0; n < count; ++n)
                                            {
                                                // has this object been created or modified in the past 15 minutes? skip this one, as AWS may be in the process of replicating it
                                                if (objects[n].LastModified > modificationCutoffTime)
                                                {
                                                    Interlocked.Increment(ref _ObjectsProcessedThisRun);
                                                    continue;
                                                }
                                                string unprefixedKey = objects[n].Key.Substring(sourcePrefix.Length);
                                                Task copyOperation = _TargetBucketObjectsWindow.UpdateObjectIfNeeded(batch, n, unprefixedKey);
                                                // if there was async processing necessary, add it to the list of tasks
                                                if (copyOperation != null)
                                                {
                                                    tasks.Add(copyOperation);
                                                }
                                                else // no copy needed--this one has finished being processed so count it now
                                                {
                                                    Interlocked.Increment(ref _ObjectsProcessedThisRun);
                                                }
                                                if (_Abort != 0) return;
                                            }
                                            Program.LogVerbose("Batch " + batch.BatchId + ": " + tasks.Count + " objects need updating (" + batch.LeastKey + "-" + batch.GreatestKey);
                                        }
                                        // this batch is complete only when the previous batch is complete and all the file copies finish
                                        using (TrackOperation("PROCESS: Waiting for batch " + batch.BatchId + retryString))
                                        {
                                            // wait for the tasks and catch any exceptions from them
                                            await Task.WhenAll(tasks);
                                        }
                                        using (TrackOperation("PROCESS: Batch " + batch.BatchId + " waiting for previous batch" + retryString))
                                        {
                                            await previousBatchCompletedCopy;
                                        }
                                        using (TrackOperation("PROCESS: Finishing batch " + batch.BatchId + retryString))
                                        {
                                            // this batch is done, tell the window we're done processing this batch
                                            _SourceBucketObjectsWindow.MarkBatchProcessed(batch);
                                            // save the batch completion information into the state
                                            _State.TrackBatchCompletion(_SourceBucketObjectsWindow.Prefix, batch);
                                            // save the state just in case we crash
                                            _State.Save(_StateFilePath);
                                            // successful finish--no need to rerun
                                            break;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        // retry this batch up to 4 times in addition to the initial run before reporting an error
                                        if (retry < 5)
                                        {
                                            Program.LogVerbose("Batch " + batch.BatchId + " has failed.  Retry #" + (retry + 1).ToString() + " beginning: " + ex.ToString());
                                            // clear the task list so we can try again
                                            tasks.Clear();
                                            continue;
                                        }
                                        else
                                        {
                                            Program.LogVerbose("Batch " + batch.BatchId + " has failed " + retry.ToString() + " times.  The process will need to be rerun after the problem is corrected: " + ex.ToString());
                                            throw;
                                        }
                                    }
                                    finally
                                    {
                                        // add in the objects that needed to be copied
                                        Interlocked.Add(ref _ObjectsProcessedThisRun, tasks.Count);
                                    }
                                }
                            }
                            finally
                            {
                                // we are done processing this batch
                                Interlocked.Decrement(ref _batchesProcessing);
                            }
                        });
                    // this batch is now the previous batch
                    previousBatchCompleted = batchCompleteTask;
                    // just keep processing
                }
                catch (Exception e)
                {
                    Program.Error("Exception processing items: " + e.ToString());
                }
            }
        }

        /// <summary>
        /// Logs a message to the console and the log file.
        /// </summary>
        /// <param name="message">The message to write.</param>
        public static void Log(string message)
        {
            Console.WriteLine(message);
            try
            {
                if (_Log != null) _Log.WriteLine(message);
            }
            catch (ObjectDisposedException)
            {
                // ignore this exception
            }
        }
        /// <summary>
        /// Logs an error message to the console and the error file.
        /// </summary>
        /// <param name="message">The message to write.</param>
        public static void Error(string message)
        {
            Console.WriteLine(message);
            try
            {
                if (_Error != null) _Error.WriteLine(message);
            }
            catch (ObjectDisposedException)
            {
                // ignore this exception
            }
        }
        /// <summary>
        /// Logs a verbose message to the console and the log file.
        /// </summary>
        /// <param name="message">The message to write.</param>
        public static void LogVerbose(string message)
        {
            if (_Verbose)
            {
                Console.WriteLine(message);
                if (_Log != null) _Log.WriteLine(message);
            }
        }

        private static bool ExitKeyPressed()
        {
            if (Console.KeyAvailable)
            {
                ConsoleKeyInfo key = Console.ReadKey(true);
                if (key.Key == ConsoleKey.Escape)
                {
                    Program.Log("ESC pressed--exiting program...");
                    Interlocked.Exchange(ref _Abort, 1);
                    return true;
                }
                else if (key.Key == ConsoleKey.F1)
                {
                    LogOperations(1);
                }
                else if (key.Key == ConsoleKey.F2)
                {
                    LogOperations(2);
                }
                else if (key.Key == ConsoleKey.F3)
                {
                    LogOperations(3);
                }
                else if (key.Key == ConsoleKey.F4)
                {
                    LogOperations(4);
                }
                else if (key.Key == ConsoleKey.F5)
                {
                    LogOperations(5);
                }
                else if (key.Key == ConsoleKey.F6)
                {
                    LogOperations(6);
                }
                else if (key.Key == ConsoleKey.F7)
                {
                    LogOperations(7);
                }
                else if (key.Key == ConsoleKey.F8)
                {
                    LogOperations(8);
                }
                else if (key.Key == ConsoleKey.F9)
                {
                    LogOperations(9);
                }
                else if (key.Key == ConsoleKey.F10)
                {
                    LogOperations(10);
                }
            }
            return false;
        }

        private static void LogOperations(int level)
        {
            StringBuilder operations = new StringBuilder();
            operations.AppendLine();
            operations.AppendLine("Time Used: " + _State.TimeUsed);
            operations.AppendLine("Source: " + _SourceBucketObjectsWindow.Bucket + "/" + _SourceBucketObjectsWindow.Prefix);
            operations.AppendLine("Last source query: " + _SourceBucketObjectsWindow.LastQuery);
            operations.AppendLine("Source window: " + _SourceBucketObjectsWindow.UnprefixedLeastKey + "-" + _SourceBucketObjectsWindow.UnprefixedGreatestKey);
            operations.AppendLine("Target: " + _TargetBucketObjectsWindow.Bucket + "/" + _TargetBucketObjectsWindow.Prefix);
            operations.AppendLine("Last target query: " + _TargetBucketObjectsWindow.LastQuery);
            operations.AppendLine("Target window: " + _TargetBucketObjectsWindow.UnprefixedLeastKey + "-" + _TargetBucketObjectsWindow.UnprefixedGreatestKey);
            operations.AppendLine("Batches Complete: " + _TargetBucketObjectsWindow.MostRecentlyDequeuedBatchId);
            operations.AppendLine("Objects Processed: " + _ObjectsProcessedThisRun + "/" + _SourceObjectsReadThisRun);
            operations.AppendLine("Objects Compared: " + _TargetObjectsReadThisRun);
            if (level > 1)
            {
                List<OperationTracker> list = new List<OperationTracker>();
                foreach (OperationTracker operation in OperationTracker.EnumerateOperationsInProgress)
                {
                    if (level > 2 || !operation.Description.StartsWith("COPY:"))
                    {
                        list.Add(operation);
                    }
                }
                list.Sort((a,b) => String.CompareOrdinal(a.Description, b.Description));
                foreach (OperationTracker operation in list)
                {
                    operations.AppendLine(operation.Description + " " + operation.Stopwatch.ElapsedMilliseconds.ToString() + "ms");
                }
            }
            operations.AppendLine();
            Program.Log(operations.ToString());
        }

        /// <summary>
        /// Tracks an operation using the specified key and description.
        /// </summary>
        /// <param name="description">A description for the operation that is still in progress.</param>
        /// <returns>A <see cref="IDisposable"/> object that will keep the description string in the list of operations in progress until it is disposed.</returns>
        public static IDisposable TrackOperation(string description)
        {
            return new OperationTracker(description);
        }

        class OperationTracker : IDisposable
        {
            private static System.Collections.Concurrent.ConcurrentDictionary<OperationTracker, object> _operationsInProgress = new System.Collections.Concurrent.ConcurrentDictionary<OperationTracker, object>();

            /// <summary>
            /// Enumerates all the operations currently in progress.
            /// </summary>
            public static IEnumerable<OperationTracker> EnumerateOperationsInProgress
            {
                get { return _operationsInProgress.Keys; }
            }

            private Stopwatch _timer = Stopwatch.StartNew();
            private string _description;

            public OperationTracker(string description)
            {
                _description = description;
                _operationsInProgress[this] = null;
            }

            /// <summary>
            /// Gets the <see cref="Stopwatch"/> that is timing this operation.
            /// </summary>
            public Stopwatch Stopwatch { get { return _timer; } }
            /// <summary>
            /// Gets the description of this operation.
            /// </summary>
            public string Description {  get { return _description; } }

            /// <summary>
            /// Disposes of this instance.
            /// </summary>
            public void Dispose()
            {
                object junk;
                _operationsInProgress.TryRemove(this, out junk);
            }
        }
    }
}
