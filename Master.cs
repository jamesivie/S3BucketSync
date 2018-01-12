using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace S3BucketSync
{
    class Master
    {
        static private int _Abort = 0;       // interlocked
        static private int _SourceProcessingComplete = 0;       // interlocked
        static private string _StateFilePath;
        static private BucketObjectsWindow _SourceBucketObjectsWindow;
        static private BucketObjectsWindow _TargetBucketObjectsWindow;
        static private State _State;    // only written during single-thread phase of startup
        static private S3CannedACL _GrantCannedAcl;
        static private S3Grant _Grant;
        static private int _SourceObjectsReadThisRun;   // interlocked
        static private int _TargetObjectsReadThisRun;   // interlocked
        static private int _ObjectsProcessedThisRun;    // interlocked
        static private int _AsyncCopiesInProgress;      // interlocked
        static private int _NonAsyncCopiesInProgress;   // interlocked
        static private int _FailedCopies;               // interlocked
        static private int _PendingCompares;            // interlocked
        static private int _TimeoutSeconds = 60;        // the default timeout is 1 minute
        static private int _RetryCount = 4;
        static private int _BatchesToQueue = 250;
        static private int _ConcurrentAsyncCopiesMax = 10000;
        static private int _ConcurrentNonAsyncCopiesMax = 1000;

        /// <summary>
        /// Gets the <see cref="State"/> object so we can save where we are and what we've accomplished.
        /// </summary>
        static public State State { get { return _State; } }
        /// <summary>
        /// Gets the number of seconds to wait before timing out on a request to S3 to copy a file.
        /// </summary>
        static public int TimeoutSeconds { get { return _TimeoutSeconds; } }

        public static int Process(string sourceRegionBucketAndPrefix, string targetRegionBucketAndPrefix, bool resetFromBeginning, string grantAccessAccountOrAcl,
            int copyTimeoutMilliseconds, int copyRetryCount, int maxBatchesToQueue, int maxAsyncCopies, int maxSyncCopies, string stateFilePath)
        {
            _TimeoutSeconds = copyTimeoutMilliseconds / 1000;
            _RetryCount = copyRetryCount;
            _BatchesToQueue = maxBatchesToQueue;
            _ConcurrentAsyncCopiesMax = maxAsyncCopies;
            _ConcurrentNonAsyncCopiesMax = maxSyncCopies;
            _StateFilePath = stateFilePath;
            // possible resume?
            if (!resetFromBeginning)
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
                _State = new State(sourceRegionBucketAndPrefix, targetRegionBucketAndPrefix, grantAccessAccountOrAcl);
            }
            S3CannedACL cannedAcl = CreateS3CannedACL(_State.GrantString);
            // A supported canned ACL?
            if (cannedAcl != null)
            {
                _GrantCannedAcl = cannedAcl;
            }
            else if (!string.IsNullOrEmpty(grantAccessAccountOrAcl))
            {
                _Grant = CreateS3Grant(grantAccessAccountOrAcl);
            }
            string grantDisplayString = (_Grant != null) ? (" with grant to " + _Grant.Grantee.EmailAddress) : ((_GrantCannedAcl != null) ? (" with canned ACL " + _GrantCannedAcl.Value) : string.Empty);
            Program.Log(Environment.NewLine + "Start Sync from " + sourceRegionBucketAndPrefix + " to " + targetRegionBucketAndPrefix + grantDisplayString);
            // initialize the source bucket objects window
            _SourceBucketObjectsWindow = new BucketObjectsWindow(sourceRegionBucketAndPrefix, _State.SourceBatchId, _State.LastKeyOfLastBatchCompleted, _GrantCannedAcl);
            // initialize the target bucket objects window
            _TargetBucketObjectsWindow = new BucketObjectsWindow(targetRegionBucketAndPrefix, new BatchIdCounter(), _State.LastKeyOfLastBatchCompleted, _GrantCannedAcl, _Grant);
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
            try
            {
                // bump up our priority (we're the most important because we feed everything else)
                Thread.CurrentThread.Priority = ThreadPriority.Highest;
                // keep making requests for more lists of objects until we have _BatchesToQueue buffered (1,000 x _BatchesToQueue pending objects)
                int batchesToQueue = _BatchesToQueue;
                // loop until we have processed all the objects or a key is pressed
                while (!_SourceBucketObjectsWindow.LastBatchHasBeenRead)
                {
                    if (ExitKeyPressed()) return 2;
                    // loop until we have the desired number of batches in the window or we hit the end
                    while (_SourceBucketObjectsWindow.BatchesQueued < batchesToQueue && !_SourceBucketObjectsWindow.LastBatchHasBeenRead)
                    {
                        if (ExitKeyPressed()) return 2;
                        // read the next batch
                        _State.RecordQueries(true);
                        if (ExitKeyPressed()) return 2;
                        using (Program.TrackOperation("MAIN: Reading batch " + (_SourceBucketObjectsWindow.LastQueuedBatchId + 1).ToString()))
                        {
                            int objectsRead;
                            try
                            {
                                objectsRead = _SourceBucketObjectsWindow.ReadNextBatch();
                            }
                            catch (Amazon.S3.AmazonS3Exception awsException)
                            {
                                if (awsException.ErrorCode == "AccessDenied") throw new ApplicationException("Access Denied attempting to read object batch from source bucket.  This may happen if ListBucket rights have not been granted to the current user or role.", awsException);
                                else if (awsException.ErrorCode == "PermanentRedirect") throw new ApplicationException("PermanentRedirect attempting to read object batch from source bucket.  It looks like you may have specified the wrong region for the source bucket.", awsException);
                                throw;
                            }
                            Interlocked.Add(ref _SourceObjectsReadThisRun, objectsRead);
                        }
                    }
                    // too full?
                    if (_SourceBucketObjectsWindow.BatchesQueued >= batchesToQueue)
                    {
                        if (ExitKeyPressed()) return 2;
                        using (Program.TrackOperation("MAIN: Waiting for processing"))
                        {
                            Thread.Sleep(100);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
#if DEBUG
                Debugger.Break();
#endif
                Program.Exception("Error reading source bucket: ", ex);
                return -2;
            }
            finally
            {
                WorkerPool.Stop();
            }
            using (Program.TrackOperation("MAIN: Queueing complete: Waiting for processing"))
            {
                // we're done getting all the objects, but we still need to finish processing them
                while (!processor.Join(100))
                {
                    if (ExitKeyPressed())
                    {
                        // log the end state
                        Program.Log(_State.ToString());
                        return 2;
                    }
                }
            }
            // the source processing is now complete
            Interlocked.Exchange(ref _SourceProcessingComplete, 1);
            _State.Delete(_StateFilePath);
            Program.Log("");
            Program.Log("This run synchronized " + _ObjectsProcessedThisRun + "/" + _SourceObjectsReadThisRun + " objects against " + _TargetObjectsReadThisRun + " objects: ");
            Program.Log(_State.Report());
            return 0;
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
        /// Creates an <see cref="S3CannedACL"/> from the specified canned ACL name.
        /// </summary>
        private static S3CannedACL CreateS3CannedACL(string cannedAclName)
        {
            switch ((cannedAclName ?? string.Empty).ToLowerInvariant())
            {
                case "authenticated-read": return S3CannedACL.AuthenticatedRead;
                case "aws-exec-read": return S3CannedACL.AWSExecRead;
                case "bucket-owner-full-control": return S3CannedACL.BucketOwnerFullControl;
                case "bucket-owner-read": return S3CannedACL.BucketOwnerRead;
                case "log-delivery-write": return S3CannedACL.LogDeliveryWrite;
                case "noacl": return S3CannedACL.NoACL;
                case "private": return S3CannedACL.Private;
                case "public-read": return S3CannedACL.PublicRead;
                case "public-read-write": return S3CannedACL.PublicReadWrite;
                default: return null;
            }
        }
        /// <summary>
        /// A static function that loops expanding the target window as needed.
        /// </summary>
        static void StatusDumper()
        {
            DateTime lastLoopTime = DateTime.UtcNow;
            long lastSourceQueries = Master.State.SourceQueries;
            long lastTargetQueries = Master.State.TargetQueries;
            long lastObjectsProcessed = Master.State.ObjectsProcessed;
            long lastBytesProcessed = Master.State.BytesProcessed;
            long lastBytesCopiedOrUpdated = Master.State.BytesCopied + Master.State.BytesUpdated;
            while (true)
            {
                try
                {
                    DateTime loopTime = DateTime.UtcNow;
                    double seconds = (loopTime - lastLoopTime).TotalSeconds;
                    long sourceQueries = Master.State.SourceQueries;
                    long targetQueries = Master.State.TargetQueries;
                    long objectsProcessed = Master.State.ObjectsProcessed;
                    long bytesProcessed = Master.State.BytesProcessed;
                    long bytesCopiedOrUpdated = Master.State.BytesCopied + Master.State.BytesUpdated;
                    // wait for a bit
                    Thread.Sleep(5000);

                    // write out the status
                    string state = Master.State.ToString();
                    if (sourceQueries != lastSourceQueries) state += " S:" + (sourceQueries - lastSourceQueries).ToString();
                    if (targetQueries != lastTargetQueries) state += " T:" + (targetQueries - lastTargetQueries).ToString();
                    state += " Q:" + _SourceBucketObjectsWindow.BatchesQueued.ToString();
                    state += " P:" + _batchesProcessing.ToString();
                    state += " A:" + _AsyncCopiesInProgress.ToString();
                    state += " C:" + _NonAsyncCopiesInProgress.ToString();
                    if (_FailedCopies > 0)
                    {
                        state += " F:" + _FailedCopies.ToString();
                    }
                    if (seconds > 1.0)
                    {
                        state += " OP/s:" + State.MagnitudeConvert((objectsProcessed - lastObjectsProcessed) / seconds, 2);
                        state += " BP/s:" + State.MagnitudeConvert((bytesProcessed - lastBytesProcessed) / seconds, 2);
                        state += " BC/s:" + State.MagnitudeConvert((bytesCopiedOrUpdated - lastBytesCopiedOrUpdated) / seconds, 2);
                    }
                    Console.WriteLine(state);

                    lastSourceQueries = sourceQueries;
                    lastTargetQueries = targetQueries;
                    lastLoopTime = loopTime;
                    lastObjectsProcessed = objectsProcessed;
                    lastBytesProcessed = bytesProcessed;
                    lastBytesCopiedOrUpdated = bytesCopiedOrUpdated;
                }
                catch (OutOfMemoryException e)
                {
                    Program.RecordAsyncException(e);
                }
                catch (Exception e)
                {
                    Program.Error("Exception dumping status", e.ToString());
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
                    bool sleep = true;
                    // do we have a source bucket range to compare to (we may get here before the first source batch has been read)
                    if (!string.IsNullOrEmpty(_SourceBucketObjectsWindow.UnprefixedGreatestKey))
                    {
                        // do we need to expand the target window?
                        if (String.CompareOrdinal(_SourceBucketObjectsWindow.UnprefixedGreatestKey, _TargetBucketObjectsWindow.UnprefixedGreatestKey) > 0)
                        {
                            using (Program.TrackOperation("TARGET: Expanding target window to include batch " + _TargetBucketObjectsWindow.LastQueuedBatchId.ToString() + " (" + _TargetBucketObjectsWindow.UnprefixedGreatestKey + " to " + _SourceBucketObjectsWindow.UnprefixedGreatestKey + ")"))
                            {
                                do
                                {
                                    int objectsRead = _TargetBucketObjectsWindow.ReadNextBatch();
                                    if (objectsRead > 0)
                                    {
                                        _State.RecordQueries(false);
                                        Interlocked.Add(ref _TargetObjectsReadThisRun, objectsRead);
                                    }
                                } while (String.CompareOrdinal(_SourceBucketObjectsWindow.UnprefixedGreatestKey, _TargetBucketObjectsWindow.UnprefixedGreatestKey) > 0 && _Abort == 0);
                            }
                            // don't sleep, we may need to expand more right now!
                            sleep = false;
                        }
                        // do we need to shrink the target window?
                        if (_SourceBucketObjectsWindow.UnprefixedLeastKey != lastTestedSourceWindowLeastKey && String.CompareOrdinal(_SourceBucketObjectsWindow.UnprefixedLeastKey, _TargetBucketObjectsWindow.UnprefixedLeastKey) > 0)
                        {
                            lastTestedSourceWindowLeastKey = _SourceBucketObjectsWindow.UnprefixedLeastKey;
                            using (Program.TrackOperation("TARGET: Shrinking target window to " + lastTestedSourceWindowLeastKey))
                            {
                                _TargetBucketObjectsWindow.ShrinkWindow(lastTestedSourceWindowLeastKey);
                            }
                        }
                    }
                    if (sleep)
                    {
                        using (Program.TrackOperation("TARGET: Waiting for work"))
                        {
                            // wait a bit so we don't monopolize the CPU
                            Thread.Sleep(100);
                        }
                    }
                }
                catch (OutOfMemoryException e)
                {
                    Program.RecordAsyncException(e);
                }
                catch (Exception e)
                {
                    // is this a non-recoverable error?  kill us!
                    AmazonS3Exception awsS3Exception = e as AmazonS3Exception;
                    if (awsS3Exception != null && awsS3Exception.ErrorCode == "AccessDenied")
                    {
                        // this should trigger a fatal exception on the main thread
                        Program.RecordAsyncException(e);
                    }
                    else
                    {
                        Program.Exception("Error reading target bucket: ", e);
                    }
                }
            }
        }
        static WorkerPool _processingPool = new WorkerPool("BatchProcessingPool", ThreadPriority.Normal, false);
        static WorkerPool _copyPool = new WorkerPool("CopyPool", ThreadPriority.Normal, false);
        static int _batchesProcessing;
        /// <summary>
        /// A static function that loops processing batches until we're ready to exist and all batches have been processed.
        /// </summary>
        static void Processor()
        {
            // start with a completed task (we use this to make sure we never bookmark a batch as complete until all previous batches are complete)
            ManualResetEvent previousBatchCompleted = ManualResetEventPool.Take(true);
            try
            {
                // loop until we abort or break out of the loop (we do that when the last source bucket window has been read)
                for (; (_Abort == 0);)
                {
                    try
                    {
                        // wait for the previous previous batch to complete before we start the next one
                        using (Program.TrackOperation("PROCESSOR: Too many concurrent copies"))
                        {
                            // add up the number of pending compares (each of which could turn into a copy) and the number of actual copies in progress and limit the total concurrency using that number
                            while (_PendingCompares + _AsyncCopiesInProgress > _ConcurrentAsyncCopiesMax
                                || _NonAsyncCopiesInProgress > _ConcurrentNonAsyncCopiesMax)
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
                                using (Program.TrackOperation("PROCESSOR: Waiting for final batch"))
                                {
                                    previousBatchCompleted.WaitOne();
                                }
                                break;
                            }
                            using (Program.TrackOperation("PROCESSOR: Waiting for source window expansion"))
                            {
                                // wait for a bit and try again
                                Thread.Sleep(100);
                            }
                            continue;
                        }
                        // get a copy of the previous task completion event
                        ManualResetEvent previousBatchCompletedCopy = previousBatchCompleted;
                        // we are now processing this batch (do this synchronously in this loop to make sure that we're keeping track of the number of batch processing tasks that have been issued, not the number that have started processing--this should help prevent later batches from superceding earlier batches)
                        Interlocked.Increment(ref _batchesProcessing);
                        // add pending compares (these could turn into copies and we don't want to overshoot the concurrency by starting up millions of copies from thousands of batches before any of them start
                        int pendingSubtract = batch.Response.S3Objects.Count;
                        Interlocked.Add(ref _PendingCompares, pendingSubtract);
                        // create a task to check all the objects, queue copy operations if needed, and wait until the batch is completely synchronized
                        ManualResetEvent batchCompleted = ManualResetEventPool.Take(false);
                        _processingPool.RunAsync(() =>
                        {
                            try
                            {
                                bool finishedSync = false;
                                int batchItems = batch.Response.S3Objects.Count;
                                int alreadyUpToDate = 0;
                                string paddedBatchId = batch.BatchId.ToString().PadLeft(6, ' ');
                                string batchPrefix = "BATCH " + paddedBatchId + " ";
                                // keep track of the work that needs doing
                                ConcurrentDictionary<int, BucketObjectsWindow.NeededCopyInfo> workToDo = new ConcurrentDictionary<int, BucketObjectsWindow.NeededCopyInfo>();
                                using (Program.TrackOperation(batchPrefix + "STATUS: ", () => BatchStatus(batchItems, alreadyUpToDate, workToDo)))
                                {
                                    int objectsToBeCopied = 0;
                                    // first compute the work that needs doing and start a task for each one asynchronously
                                    CancellationTokenSource batchFirstPassCancellationToken = new CancellationTokenSource(Master.TimeoutSeconds * 1000);
                                    try
                                    {
                                        // get the last (greatest) key in the batch
                                        string lastKey = batch.GreatestKey;
                                        using (Program.TrackOperation(batchPrefix + "WAIT: Target window expansion"))
                                        {
                                            // wait until the target window ready to process all the items in this batch (it almost always should be ready to go already)
                                            while (String.CompareOrdinal(lastKey, _TargetBucketObjectsWindow.UnprefixedGreatestKey) > 0)
                                            {
                                                System.Threading.Thread.Sleep(250);
                                                if (_Abort != 0) return;
                                            }
                                        }
                                        // don't copy anything updated in the last 15 minutes (this prevents us copying things that AWS is already replicating--without it, we seem to copy items that were generated during the processing)
                                        DateTime modificationCutoffTime = DateTime.UtcNow.AddMinutes(-15);
                                        string batchCopyPrefix = batchPrefix + "COPY: ";
                                        using (Program.TrackOperation(batchPrefix + "PROCESS: Comparing files first try"))
                                        {
                                            List<S3Object> objects = batch.Response.S3Objects;
                                            int count = objects.Count;
                                            for (int n = 0; n < count; ++n)
                                            {
                                                if (_Abort != 0) return;
                                                // has this object been created or modified in the past 15 minutes? skip this one, as AWS may be in the process of replicating it
                                                if (objects[n].LastModified.ToUniversalTime() > modificationCutoffTime)
                                                {
                                                    Interlocked.Increment(ref alreadyUpToDate);
                                                    Interlocked.Increment(ref _ObjectsProcessedThisRun);
                                                    continue;
                                                }
                                                string unprefixedKey = objects[n].Key.Substring(sourcePrefix.Length);
                                                BucketObjectsWindow.NeededCopyInfo copyInfo = _TargetBucketObjectsWindow.CheckIfObjectNeedsUpdate(batch, n, unprefixedKey);
                                                // no copy needed?  this one has finished being processed so count it now
                                                if (copyInfo == null)
                                                {
                                                    Interlocked.Increment(ref alreadyUpToDate);
                                                    Interlocked.Increment(ref _ObjectsProcessedThisRun);
                                                    continue;
                                                }
                                                int objectNumber = n;
                                                string key = unprefixedKey;
                                                Interlocked.Increment(ref _AsyncCopiesInProgress);
                                                Task copyTask = Task.Run(async () =>
                                                {
                                                    // create the copy request object RIGHT before we issue the command because otherwise AWS may think that system clocks are out of sync
                                                    try
                                                    {
                                                        Stopwatch timer = Stopwatch.StartNew();
                                                        // create the copy request object RIGHT before we issue the command because otherwise AWS may think that system clocks are out of sync
                                                        CopyObjectRequest copyRequest = copyInfo.CopyObjectRequestBuilder();
                                                        Master.State.AddChargeForCopies(1);
                                                        using (Program.TrackOperation(batchCopyPrefix + copyInfo.BatchObjectNumber.ToString("000") + ": ", () => copyInfo.Key + " (" + (copyInfo.SourceObject.Size + 500000) / 1000000 + "MB)"))
                                                        {
                                                            await copyInfo.S3.CopyObjectAsync(copyRequest, batchFirstPassCancellationToken.Token);
                                                        }
                                                        string operation = String.Format("{0}.{1} {2} ({3:F0}MB:{4})", batch.BatchId, copyInfo.BatchObjectNumber, (copyInfo.TargetObject == null) ? "copied" : "updated", copyInfo.SourceObject.Size / 1000000.0, copyInfo.Key);
                                                        if (copyInfo.TargetObject != null) operation += " " + copyInfo.SourceObject.ETag + " vs " + copyInfo.TargetObject.ETag;
                                                        Program.LogVerbose(operation);
                                                        // track what the operation we just completed successfully
                                                        Master.State.TrackObject(copyInfo.SourceObject, copyInfo.TargetObject == null, copyInfo.TargetObject != null);
                                                    }
                                                    catch (AmazonS3Exception ex)
                                                    {
                                                        // if the key no longer exists, just bail out now--there is no need to copy the object if it's gone
                                                        if (ex.Message == "The specified key does not exist.") return;
                                                        // else just ignore this--someone deleted a source file before we were able to get to it to copy it
                                                        string operation = String.Format("{0}.{1} {2} ({3:F0}MB:{4})", batch.BatchId, objectNumber, "disappeared", copyInfo.SourceObject.Size / 1000000.0, key);
                                                        if (copyInfo.TargetObject != null) operation += " " + copyInfo.SourceObject.ETag + " vs " + copyInfo.TargetObject.ETag;
                                                        Program.LogVerbose(operation);
                                                        // track what the operation we just completed successfully
                                                        Master.State.TrackObject(copyInfo.SourceObject, copyInfo.TargetObject == null, copyInfo.TargetObject != null);
                                                    }
                                                }, batchFirstPassCancellationToken.Token);
                                                // save the task with the operation
                                                copyInfo.Task = copyTask;
                                                // add this task to the list of pending tasks
                                                workToDo.TryAdd(n, copyInfo);
                                            }
                                            Program.LogVerbose("Batch " + batch.BatchId.ToString() + ": " + workToDo.Count + " objects need updating (" + batch.LeastKey + "-" + batch.GreatestKey);
                                        }
                                        // we've now either started a copy or know that we don't need one for each source item
                                        Interlocked.Add(ref _PendingCompares, -pendingSubtract);
                                        pendingSubtract = 0;
                                        // save the count in case we need to retry
                                        objectsToBeCopied = workToDo.Count;
                                        // this batch is complete only when the previous batch is complete and all the file copies finish
                                        using (Program.TrackOperation(batchPrefix + "WAIT: pass 1: " + objectsToBeCopied.ToString()))
                                        {
                                            // wait for the tasks and catch any exceptions from them
                                            if (!Task.WaitAll(workToDo.Values.Select(w => w.Task).ToArray(), Master.TimeoutSeconds * 1000, batchFirstPassCancellationToken.Token))
                                            {
                                                throw new OperationCanceledException();
                                            }
                                            // these copies are all finished
                                            Interlocked.Add(ref _AsyncCopiesInProgress, -workToDo.Count);
                                            finishedSync = true;
                                        }
                                    }
                                    catch (System.OperationCanceledException)
                                    {
                                        if (_Abort != 0) return;
                                        Program.LogVerbose("Batch " + batch.BatchId + " first pass has timed out.  Retrying failed operations synchronously.");
                                    }
                                    catch (System.AggregateException)
                                    {
                                        if (_Abort != 0) return;
                                        batchFirstPassCancellationToken.Cancel();
                                        Program.LogVerbose("Batch " + batch.BatchId + " first pass has timed out with " + workToDo.Count + " operations in progress.  Retrying failed operations synchronously.");
                                    }
                                    catch (Exception ex)
                                    {
                                        if (_Abort != 0) return;
                                        Program.Error("Batch " + batch.BatchId + " has failed.  The process will need to be rerun after the problem is corrected.", ex.ToString());
                                        throw;
                                    }
                                    if (!finishedSync)
                                    {
                                        using (Program.TrackOperation(batchPrefix + "PROCESS: Requeueing: " + workToDo.Count.ToString()))
                                        {
                                            // still more work to do?
                                            if (workToDo.Count > 0)
                                            {
                                                // save the count while we attempt to process items synchronously
                                                BucketObjectsWindow.NeededCopyInfo[] leftToDo = workToDo.Values.Where(c => c != null && c.Task != null && (c.Task.IsCanceled || c.Task.IsFaulted)).ToArray();
                                                Program.LogVerbose("Batch " + batch.BatchId + " retrying " + leftToDo.Length + " failed operations without async.");
                                                foreach (BucketObjectsWindow.NeededCopyInfo neededCopyInfo in leftToDo)
                                                {
                                                    BucketObjectsWindow.NeededCopyInfo copyInfo = neededCopyInfo;
                                                    _copyPool.RunAsync(() =>
                                                    {
                                                        bool success = false;
                                                        Exception lastException = new System.Exception("No exception");
                                                        for (int attempt = 0; attempt < _RetryCount; ++attempt)
                                                        {
                                                            try
                                                            {
                                                                Interlocked.Increment(ref _NonAsyncCopiesInProgress);
                                                                Stopwatch timer = Stopwatch.StartNew();
                                                                // create the copy request object RIGHT before we issue the command because otherwise AWS may think that system clocks are out of sync
                                                                CopyObjectRequest copyRequest = copyInfo.CopyObjectRequestBuilder();
                                                                // increase the timeout on each retry
                                                                copyRequest.Timeout = TimeSpan.FromTicks((copyRequest.Timeout ?? TimeSpan.FromSeconds(30)).Ticks * (1 + attempt));
                                                                Master.State.AddChargeForCopies(1);
                                                                using (Program.TrackOperation(batchPrefix + "RETRYCOPY" + (attempt + 1) + ": " + copyInfo.BatchObjectNumber.ToString("000") + ": ", () => copyInfo.Key + " (" + (copyInfo.SourceObject.Size + 500000) / 1000000 + "MB)"))
                                                                {
                                                                    try
                                                                    {
                                                                        copyInfo.S3.CopyObject(copyRequest);
                                                                    }
                                                                    catch (TargetInvocationException ex)
                                                                    {
                                                                        throw ex.InnerException;
                                                                    }
                                                                }
                                                                // remove the task to signal successful non-async completion
                                                                copyInfo.Task = null;
                                                                // record the operation
                                                                string operation = String.Format("{0}.{1} {2} ({3:F0}MB:{4})", batch.BatchId, copyInfo.BatchObjectNumber, (copyInfo.TargetObject == null) ? "copied" : "updated", copyInfo.SourceObject.Size / 1000000.0, copyInfo.Key);
                                                                if (copyInfo.TargetObject != null) operation += " " + copyInfo.SourceObject.ETag + " vs " + copyInfo.TargetObject.ETag;
                                                                Program.LogVerbose(operation);
                                                                // track what the operation we just completed successfully
                                                                Master.State.TrackObject(copyInfo.SourceObject, copyInfo.TargetObject == null, copyInfo.TargetObject != null);
                                                                // success-no need to loop any more
                                                                success = true;
                                                                break;
                                                            }
                                                            catch (AmazonServiceException ex)
                                                            {
                                                                lastException = ex;
                                                                string message = ex.Message;
                                                                // if the source object no longer exists, there is no need to copy it!
                                                                if (message.Contains("The specified key does not exist.")) return;
                                                                // if we timed out, try again with a slightly longer timeout
                                                                if (message.Contains("The operation has timed out")) continue;
                                                                // else just ignore this--someone deleted a source file before we were able to get to it to copy it
                                                                string operation = String.Format("{0}.{1} {2} ({3:F0}MB:{4})", batch.BatchId, copyInfo.BatchObjectNumber, "disappeared", copyInfo.SourceObject.Size / 1000000.0, copyInfo.Key);
                                                                if (copyInfo.TargetObject != null) operation += " " + copyInfo.SourceObject.ETag + " vs " + copyInfo.TargetObject.ETag;
                                                                Program.LogVerbose(operation);
                                                                // track what the operation we just completed successfully
                                                                Master.State.TrackObject(copyInfo.SourceObject, copyInfo.TargetObject == null, copyInfo.TargetObject != null);
                                                            }
                                                            catch (Exception ex)
                                                            {
                                                                Program.Error("Exception in sync copy", ex.ToString());
                                                            }
                                                            finally
                                                            {
                                                                Interlocked.Decrement(ref _NonAsyncCopiesInProgress);
                                                            }
                                                        }
                                                        if (!success)
                                                        {
                                                            // remove the task to signal non-async completion
                                                            copyInfo.Task = null;
                                                            Interlocked.Increment(ref _FailedCopies);
                                                            Program.LogVerbose("Batch " + batch.BatchId + " " + copyInfo.BatchObjectNumber.ToString("000") + ": " + copyInfo.Key + " has failed.  The process will need to be rerun to ensure that this object is copied: " + lastException.ToString());
                                                        }
                                                    });
                                                }
                                            }
                                        }
                                        // wait for synchronous copies to complete
                                        int remaining = -1;
                                        using (Program.TrackOperation(batchPrefix + "WAIT: pass 2: " + ((remaining < 0) ? "unknown" : remaining.ToString())))
                                        {
                                            while ((remaining = workToDo.Values.Where(c => c != null && c.Task != null && (c.Task.IsCanceled || c.Task.IsFaulted)).Count()) > 0)
                                            {
                                                System.Threading.Thread.Sleep(500);
                                            }
                                        }
                                        // all the copies that were pending are now complete for this batch
                                        Interlocked.Add(ref _AsyncCopiesInProgress, -workToDo.Count);
                                    }
                                    // we're now done, but we may need to wait for the previous batch before marking ourselves as processed (this way we only have to keep one bookmark)
                                    using (Program.TrackOperation(batchPrefix + "WAIT: Previous batch"))
                                    {
                                        previousBatchCompletedCopy.WaitOne();
                                        // we no longer need the event from the previous batch (and we should have been the only ones waiting on it)
                                        ManualResetEventPool.Return(previousBatchCompletedCopy); previousBatchCompletedCopy = null;
                                    }
                                    using (Program.TrackOperation(batchPrefix + "WAIT: Cleanup"))
                                    {
                                        // this batch is done, tell the window we're done processing this batch
                                        _SourceBucketObjectsWindow.MarkBatchProcessed(batch);
                                        // save the batch completion information into the state
                                        _State.TrackBatchCompletion(_SourceBucketObjectsWindow.Prefix, batch);
                                        // save the state just in case we crash
                                        _State.Save(_StateFilePath);
                                    }
                                    // the objects that needed to be copied are now done!
                                    Interlocked.Add(ref _ObjectsProcessedThisRun, objectsToBeCopied);
                                    // this batch has been completed
                                    batchCompleted.Set();
                                }
                            }
                            catch (Exception ex)
                            {
                                if (ex is OutOfMemoryException) throw;
                                Program.Error("Processor exception", ex.ToString());
                            }
                            finally
                            {
                                // we are done processing this batch
                                Interlocked.Decrement(ref _batchesProcessing);
                                // subtract off any remaining pending items in case we didn't get to the part where we queued them
                                Interlocked.Add(ref _PendingCompares, -pendingSubtract);
                            }
                        });
                        // this batch is now the previous batch
                        previousBatchCompleted = batchCompleted;
                        // just keep processing
                    }
                    catch (OutOfMemoryException e)
                    {
                        Program.RecordAsyncException(e);
                    }
                    catch (Exception e)
                    {
                        Program.Error("Exception processing items", e.ToString());
                    }
                }
            }
            finally
            {
                ManualResetEventPool.Return(previousBatchCompleted);
            }
        }
        private static string BatchStatus(int batchItems, int alreadyUpToDate, ConcurrentDictionary<int, BucketObjectsWindow.NeededCopyInfo> workToDo)
        {
            float total = batchItems;
            int waiting;
            int running;
            int retry;
            int completed;
            TaskStatuses(workToDo.Values.Select(w => w.Task), out waiting, out running, out retry, out completed);
            return String.Format(System.Globalization.CultureInfo.InvariantCulture, "{0:D4}/{1:D4}/{2:D4}/{3:D4}/{4:D4}/{5:D4}", batchItems - alreadyUpToDate - workToDo.Count, waiting, running, retry, completed, alreadyUpToDate);
        }
        private static void TaskStatuses(IEnumerable<Task> tasks, out int waiting, out int running, out int syncRetry, out int completed)
        {
            waiting = 0;
            running = 0;
            syncRetry = 0;
            completed = 0;
            foreach (Task t in tasks)
            {
                if (t == null) ++completed;
                else
                {
                    switch (t.Status)
                    {
                        case TaskStatus.Created:
                        case TaskStatus.WaitingForActivation:
                        case TaskStatus.WaitingToRun:
                            ++waiting;
                            break;
                        case TaskStatus.Running:
                        case TaskStatus.WaitingForChildrenToComplete:
                            ++running;
                            break;
                        case TaskStatus.Faulted:
                        case TaskStatus.RanToCompletion:
                            ++completed;
                            break;
                        case TaskStatus.Canceled:
                            ++syncRetry;
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        private static bool ExitKeyPressed()
        {
            Program.ThrowAsyncException();
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
            operations.AppendLine("Source window (" + _SourceBucketObjectsWindow.ItemsQueued + "): " + _SourceBucketObjectsWindow.UnprefixedLeastKey + "-" + _SourceBucketObjectsWindow.UnprefixedGreatestKey);
            operations.AppendLine("Target: " + _TargetBucketObjectsWindow.Bucket + "/" + _TargetBucketObjectsWindow.Prefix);
            operations.AppendLine("Last target query: " + _TargetBucketObjectsWindow.LastQuery);
            operations.AppendLine("Target window (" + _TargetBucketObjectsWindow.ItemsQueued + "): " + _TargetBucketObjectsWindow.UnprefixedLeastKey + "-" + _TargetBucketObjectsWindow.UnprefixedGreatestKey);
            operations.AppendLine("Batches Complete: " + _TargetBucketObjectsWindow.MostRecentlyDequeuedBatchId);
            operations.AppendLine("Objects Processed: " + _ObjectsProcessedThisRun + "/" + _SourceObjectsReadThisRun);
            operations.AppendLine("Objects Compared: " + _TargetObjectsReadThisRun);
            if (!string.IsNullOrEmpty(_State.LastCopyCompleted))
            {
                operations.AppendLine("Last Object Copied: " + _State.LastCopyCompleted);
                operations.AppendLine("Modified Date of Last Object Copied: " + _State.LastCopyCompletedModifiedDate.ToString());
            }
            if (!string.IsNullOrEmpty(_State.LastUpdateCompleted))
            {
                operations.AppendLine("Last Object Updated: " + _State.LastUpdateCompleted);
                operations.AppendLine("Modified Date of Last Object Updated: " + _State.LastUpdateCompletedModifiedDate.ToString());
            }

            if (level > 1)
            {
                List<Program.OperationTracker> list = new List<Program.OperationTracker>();
                Program.OperationTracker leastCopy = null;
                Program.OperationTracker greatestCopy = null;
                foreach (Program.OperationTracker operation in Program.OperationTracker.EnumerateOperationsInProgress)
                {
                    bool isCopy = (operation.Description.Contains(" COPY: ") || operation.Description.Contains(" RETRYCOPY"));
                    bool isWait = operation.Description.Contains("WAIT: ");
                    if (level > 4
                            || (level > 3 && !isCopy)
                            || (level > 2 && !isWait && !isCopy)
                        )
                    {
                        list.Add(operation);
                    }
                    else if (isCopy)
                    {
                        if (leastCopy == null || String.CompareOrdinal(operation.Description, leastCopy.Description) < 0) leastCopy = operation;
                        if (greatestCopy == null || String.CompareOrdinal(operation.Description, greatestCopy.Description) > 0) greatestCopy = operation;
                    }
                }
                if (level >= 2 && level < 4)
                {
                    if (leastCopy != null && greatestCopy != null)
                    {
                        list.Add(leastCopy);
                        if (!String.Equals(leastCopy.Description, greatestCopy.Description)) list.Add(greatestCopy);
                    }
                }
                list.Sort((a, b) => String.CompareOrdinal(a.Description, b.Description));
                foreach (Program.OperationTracker operation in list)
                {
                    operations.AppendLine(operation.Description + " " + operation.DynamicDescription + " " + operation.Stopwatch.ElapsedMilliseconds.ToString() + "ms");
                }
                if (level >= 2)
                {
                    foreach (WorkerPool pool in new WorkerPool[] { _processingPool, _copyPool })
                        operations.AppendLine(pool.Name + ": " + pool.Workers + " workers, " + pool.BusyWorkers + " busy, " + pool.ReadyWorkers + " ready, " + pool.PeakConcurrentWorkersUsedRecently + " peak");
                }
            }
            operations.AppendLine();
            Program.Log(operations.ToString());
        }
    }
}
