using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

/*
Copyright (c) 2016 James Ivie

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

This code may not be incorporated into any source or binary code distributed by Amazon or its subsidiaries, subcontractors, parent companies, etc. without a separate license.

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
        static private S3CannedACL _GrantCannedAcl;
        static private S3Grant _Grant;
        static private ServerSideEncryptionMethod _TargetEncryptionMethod;
        static private Exception _AsyncException;
        static private int _SourceObjectsReadThisRun;   // interlocked
        static private int _TargetObjectsReadThisRun;   // interlocked
        static private int _ObjectsProcessedThisRun;    // interlocked
        static private int _CopiesInProgress;           // interlocked
        static private int _PendingCompares;            // interlocked
        static private int _TimeoutSeconds = 10 * 60;   // the default timeout is 10 minutes
        static private int _RetryCount = 4;
        static private int _BatchesToQueue = 250;
        static private int _ConcurrentCopies = 10000;

        /// <summary>
        /// Gets the <see cref="State"/> object so we can save where we are and what we've accomplished.
        /// </summary>
        static public State State { get { return _State; } }
        /// <summary>
        /// Gets the number of seconds to wait before timing out on a request to S3 to copy a file.
        /// </summary>
        static public int TimeoutSeconds { get { return _TimeoutSeconds; } }


        static int Main(string[] args)
        {
            try
            {
                ThreadPool.SetMaxThreads(1000, 1000);
                string sourceRegionBucketAndPrefix = null;
                string targetRegionBucketAndPrefix = null;
                string commonPrefix = null;
                string grantString = null;
                string encryptionMethodString = null;
                bool reset = false;
                // no arguments or not enough arguments?
                if (args.Length < 2)
                {
                    Console.WriteLine(@"
S3BucketSync Usage:
    S3BucketSync <source_region>:<source_bucket>[source_prefix] <target_region>:<target_bucket>[target_prefix] [common_prefix] [-b] [-v] [-ofc] [-g <account_email>] [-g <canned_acl_name>]

Purpose:
    Copies or updates objects from the source bucket to the destination bucket if the object doesn't exist in the destination bucket or the Etag of the corresponding object does not match

Options:
    common_prefix is an optional prefix string that is appended to both the source_prefix and target_prefix just to save typing the same prefixes for two different buckets
    -b means restart at the beginning, ignore any saved state
    -v means output extra messages to see more details of what's going on
    -g grants the account represented by the email full rights
    -t is the copy timeout in seconds (the default is 10 minutes)
    -r is the batch retry count (the default is 4)
    -q is the maximum number of batches to queue (default is 250)
    -c is the maximum number of concurrent copies to allow before preventing new batches (default is 10,000)

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
    S3BucketSync us-east-1:main us-west-2:main -g bucket-owner-full-control
        Give the target account full access to the copied objects.

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
                    return 1;
                }
                // process args
                for (int narg = 0; narg < args.Length; ++narg)
                {
                    string argument = args[narg];
                    if (argument.StartsWith("-"))
                    {
                        if (argument.ToLowerInvariant().StartsWith("-b"))
                        {
                            reset = true;
                        }
                        else if (argument.ToLowerInvariant().StartsWith("-v"))
                        {
                            _Verbose = true;
                        }
                        else if (argument.ToLowerInvariant().StartsWith("-g"))
                        {
                            if (narg + 1 >= args.Length) throw new ArgumentException("-g must be followed by the email address of the account to grant rights to or the name of a canned ACL!");
                            string arg = args[narg + 1];
                            grantString = arg;
                            ++narg;
                        }
                        else if (argument.ToLowerInvariant().StartsWith("-e"))
                        {
                            if (narg + 1 >= args.Length) throw new ArgumentException("-e must be followed by the encruption method for the target (probably \"AES256\") !");
                            string arg = args[narg + 1];
                            encryptionMethodString = arg;
                            ++narg;
                        }
                        else if (argument.ToLowerInvariant().StartsWith("-t"))
                        {
                            if (narg + 1 >= args.Length) throw new ArgumentException("-t must be followed by the number of seconds to wait for a copy before timing out!");
                            string arg = args[narg + 1];
                            if (!Int32.TryParse(arg, out _TimeoutSeconds)) throw new ArgumentException("The timeout seconds must contain only decimal digits!");
                            ++narg;
                        }
                        else if (argument.ToLowerInvariant().StartsWith("-r"))
                        {
                            if (narg + 1 >= args.Length) throw new ArgumentException("-r must be followed by the number of times to retry a batch before logging an error!");
                            string arg = args[narg + 1];
                            if (!Int32.TryParse(arg, out _RetryCount)) throw new ArgumentException("The retry count must contain only decimal digits!");
                            ++narg;
                        }
                        else if (argument.ToLowerInvariant().StartsWith("-q"))
                        {
                            if (narg + 1 >= args.Length) throw new ArgumentException("-q must be followed by the number of batches to queue before waiting for previous batches to finish!");
                            string arg = args[narg + 1];
                            if (!Int32.TryParse(arg, out _BatchesToQueue)) throw new ArgumentException("The batch queue count must contain only decimal digits!");
                            ++narg;
                        }
                        else if (argument.ToLowerInvariant().StartsWith("-c"))
                        {
                            if (narg + 1 >= args.Length) throw new ArgumentException("-c must be followed by the number of concurrent copies to allow before waiting to start new batches!");
                            string arg = args[narg + 1];
                            if (!Int32.TryParse(arg, out _ConcurrentCopies)) throw new ArgumentException("The concurrent copy count must contain only decimal digits!");
                            ++narg;
                        }
                    }
                    else if (string.IsNullOrEmpty(sourceRegionBucketAndPrefix)) sourceRegionBucketAndPrefix = argument;
                    else if (string.IsNullOrEmpty(targetRegionBucketAndPrefix)) targetRegionBucketAndPrefix = argument;
                    else if (string.IsNullOrEmpty(commonPrefix)) commonPrefix = argument;
                }
                // a common prefix (append to source and destination)
                if (!string.IsNullOrEmpty(commonPrefix))
                {
                    // add the common prefix to each bucket/prefix, ensuring that there is a slash (/) separator
                    sourceRegionBucketAndPrefix = AddCommonPrefix(sourceRegionBucketAndPrefix, commonPrefix);
                    targetRegionBucketAndPrefix = AddCommonPrefix(targetRegionBucketAndPrefix, commonPrefix);
                }

                string currentDirectory = Environment.CurrentDirectory;
                _StateFilePath = Path.Combine(currentDirectory, "state." + sourceRegionBucketAndPrefix.Replace(":", "_").Replace("/", "_") + ".bin");
                _LogFilePath = Path.Combine(currentDirectory, "log." + sourceRegionBucketAndPrefix.Replace(":", "_").Replace("/", "_") + ".txt");
                _ErrorFilePath = Path.Combine(currentDirectory, "error." + sourceRegionBucketAndPrefix.Replace(":", "_").Replace("/", "_") + ".txt");
                // open the log file and error file
                using (_Log = new StreamWriter(_LogFilePath, true, Encoding.UTF8))
                using (_Error = new StreamWriter(_ErrorFilePath, true, Encoding.UTF8))
                {
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
                        _State = new State(sourceRegionBucketAndPrefix, targetRegionBucketAndPrefix, grantString, encryptionMethodString);
                    }
                    S3CannedACL cannedAcl = CreateS3CannedACL(_State.GrantString);
                    // A supported canned ACL?
                    if (cannedAcl != null)
                    {
                        _GrantCannedAcl = cannedAcl;
                    }
                    else if (!string.IsNullOrEmpty(grantString))
                    {
                        _Grant = CreateS3Grant(grantString);
                    }
                    string grantDisplayString = (_Grant != null) ? (" with grant to " + _Grant.Grantee.EmailAddress) : ((_GrantCannedAcl != null) ? (" with canned ACL " + _GrantCannedAcl.Value) : string.Empty);
                    // target encryption?
                    _TargetEncryptionMethod = CreateTargetEncryptionMethod(_State.EncryptionMethod);

                    Program.Log(Environment.NewLine + "Start Sync from " + sourceRegionBucketAndPrefix + " to " + targetRegionBucketAndPrefix + grantDisplayString);
                    // initialize the source bucket objects window
                    _SourceBucketObjectsWindow = new BucketObjectsWindow(sourceRegionBucketAndPrefix, _State.SourceBatchId, _State.LastKeyOfLastBatchCompleted, _GrantCannedAcl);
                    // initialize the target bucket objects window
                    _TargetBucketObjectsWindow = new BucketObjectsWindow(targetRegionBucketAndPrefix, new BatchIdCounter(), _State.LastKeyOfLastBatchCompleted, _GrantCannedAcl, _Grant, _TargetEncryptionMethod);
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
                            if (_AsyncException != null) throw _AsyncException;
                            if (ExitKeyPressed()) return 2;
                            // loop until we have the desired number of batches in the window or we hit the end
                            while (_SourceBucketObjectsWindow.BatchesQueued < batchesToQueue && !_SourceBucketObjectsWindow.LastBatchHasBeenRead)
                            {
                                if (ExitKeyPressed()) return 2;
                                // read the next batch
                                _State.RecordQueries(true);
                                using (TrackOperation("MAIN: Reading batch " + (_SourceBucketObjectsWindow.LastQueuedBatchId + 1).ToString()))
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
                                using (TrackOperation("MAIN: Waiting for processing"))
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
                        _Log.Flush();
                        _Error.Flush();
                        return -2;
                    }
                    finally
                    {
                        WorkerPool.Stop();
                    }
                    using (TrackOperation("MAIN: Queueing complete: Waiting for processing"))
                    {
                        // we're done getting all the objects, but we still need to finish processing them
                        while (!processor.Join(100))
                        {
                            if (_AsyncException != null) throw _AsyncException;
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
                    _Log.Flush();
                    _Error.Flush();
                }
                return 0;
            }
            catch (Exception ex)
            {
#if DEBUG
                Debugger.Break();
#endif
                Program.Error("FATAL PROGRAM ERROR: " + ex.ToString());
                return -1;
            }
        }
        private static string AddCommonPrefix(string bucketAndPrefix, string cp)
        {
            bool trailingSlash = bucketAndPrefix.EndsWith("/");
            bool leadingSlash = cp.StartsWith("/");

            if (trailingSlash && leadingSlash)
            {
                return bucketAndPrefix.TrimEnd('/') + cp;
            }
            else if (trailingSlash || leadingSlash)
            {
                return bucketAndPrefix + cp;
            }
            return bucketAndPrefix + "/" + cp;
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
        private static ServerSideEncryptionMethod CreateTargetEncryptionMethod(string encryptionMethod)
        {
            switch (encryptionMethod)
            {
                default:
                    throw new ArgumentException("Unknown encryption method: " + encryptionMethod + "!");
                case null:
                case "None":
                    return null;
                case "AES256":
                    return new ServerSideEncryptionMethod("AES256");
            }
        }
        /// <summary>
        /// A static function that loops expanding the target window as needed.
        /// </summary>
        static void StatusDumper()
        {
            DateTime lastLoopTime = DateTime.UtcNow;
            long lastObjectsProcessed = Program.State.ObjectsProcessed;
            long lastBytesProcessed = Program.State.BytesProcessed;
            long lastBytesCopiedOrUpdated = Program.State.BytesCopied + Program.State.BytesUpdated;
            while (true)
            {
                try
                {
                    DateTime loopTime = DateTime.UtcNow;
                    double seconds = (loopTime - lastLoopTime).TotalSeconds;
                    long objectsProcessed = Program.State.ObjectsProcessed;
                    long bytesProcessed = Program.State.BytesProcessed;
                    long bytesCopiedOrUpdated = Program.State.BytesCopied + Program.State.BytesUpdated;
                    // wait for a bit
                    Thread.Sleep(5000);
                    // write out the status
                    string state = Program.State.ToString();
                    state += " Q:" + _SourceBucketObjectsWindow.BatchesQueued.ToString();
                    state += " P:" + _batchesProcessing.ToString();
                    state += " C:" + _CopiesInProgress.ToString();
                    if (seconds > 1.0)
                    {
                        state += " OP/s:" + State.MagnitudeConvert((objectsProcessed - lastObjectsProcessed) / seconds, 2);
                        state += " BP/s:" + State.MagnitudeConvert((bytesProcessed - lastBytesProcessed) / seconds, 2);
                        state += " BC/s:" + State.MagnitudeConvert((bytesCopiedOrUpdated - lastBytesCopiedOrUpdated) / seconds, 2);
                    }
                    Console.WriteLine(state);

                    lastLoopTime = loopTime;
                    lastObjectsProcessed = objectsProcessed;
                    lastBytesProcessed = bytesProcessed;
                    lastBytesCopiedOrUpdated = bytesCopiedOrUpdated;
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
                            using (TrackOperation("TARGET: Expanding target window to include batch " + _TargetBucketObjectsWindow.LastQueuedBatchId.ToString() + " (" + _TargetBucketObjectsWindow.UnprefixedGreatestKey + " to " + _SourceBucketObjectsWindow.UnprefixedGreatestKey + ")"))
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
                            continue;
                        }
                        // do we need to shrink the target window?
                        else if (_SourceBucketObjectsWindow.UnprefixedLeastKey != lastTestedSourceWindowLeastKey && String.CompareOrdinal(_SourceBucketObjectsWindow.UnprefixedLeastKey, _TargetBucketObjectsWindow.UnprefixedLeastKey) > 0)
                        {
                            lastTestedSourceWindowLeastKey = _SourceBucketObjectsWindow.UnprefixedLeastKey;
                            using (TrackOperation("TARGET: Shrinking target window to " + lastTestedSourceWindowLeastKey))
                            {
                                _TargetBucketObjectsWindow.ShrinkWindow(lastTestedSourceWindowLeastKey);
                            }
                        }
                    }
                    using (TrackOperation("TARGET: Waiting for work"))
                    {
                        // wait a bit so we don't monopolize the CPU
                        Thread.Sleep(100);
                    }
                }
                catch (Exception e)
                {
                    // is this a non-recoverable error?  kill us!
                    AmazonS3Exception awsS3Exception = e as AmazonS3Exception;
                    if (awsS3Exception != null && awsS3Exception.ErrorCode == "AccessDenied")
                    {
                        System.Threading.Interlocked.Exchange(ref _AsyncException, awsS3Exception);
                        // this should trigger a fatal exception on the main thread
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
            ManualResetEvent previousBatchCompleted = new ManualResetEvent(true);
            // loop until we've processed all the items
            for (long previousStarvedTasks = 0; (_Abort == 0); previousStarvedTasks = State.StarvedTasks)
            {
                try
                {
                    // wait due to task starvation?
                    long starvedTasks = State.StarvedTasks - previousStarvedTasks;
                    if (starvedTasks > 10)
                    {
                        using (TrackOperation("SOURCE: Waiting for tasks to stop starving (" + starvedTasks.ToString() + " tasks starved this round)"))
                        {
                            Thread.Sleep(5000);
                        }
                    }
                    // wait for the previous previous batch to complete before we start the next one
                    using (TrackOperation("SOURCE: Waiting for batch processing and copies to complete before continuing"))
                    {
                        // add up 1000x the starved tasks in the last loop, the number of pending compares (each of which could turn into a copy) to the number of actual copies in progress and limit the total concurrency using that number
                        while (_PendingCompares + _CopiesInProgress > _ConcurrentCopies)
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
                            using (TrackOperation("SOURCE: Complete: waiting for pending copies"))
                            {
                                previousBatchCompleted.WaitOne();
                            }
                            break;
                        }
                        using (TrackOperation("SOURCE: Waiting for batches to queue"))
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
                    ManualResetEvent batchCompleted = new ManualResetEvent(false);
                    _processingPool.RunAsync(() =>
                        {
                            try
                            {
                                int batchItems = 1000;
                                int alreadyUpToDate = 0;
                                string paddedBatchId = batch.BatchId.ToString().PadLeft(6, ' ');
                                // keep track of the work that needs doing
                                ConcurrentDictionary<int, BucketObjectsWindow.NeededCopyInfo> workToDo = new ConcurrentDictionary<int, BucketObjectsWindow.NeededCopyInfo>();
                                using (TrackOperation("BATCH " + paddedBatchId + " COMPLETION: ", () => BatchStatus(batchItems, alreadyUpToDate, workToDo)))
                                {
                                    int objectsToBeCopied = 0;
                                    // first compute the work that needs doing and start a task for each one asynchronously
                                    CancellationTokenSource batchFirstPassCancellationToken = new CancellationTokenSource(Program.TimeoutSeconds * 1000);
                                    try
                                    {
                                        // get the last (greatest) key in the batch
                                        string lastKey = batch.GreatestKey;
                                        using (TrackOperation("BATCH " + paddedBatchId + " WAIT: Waiting for target window to include " + lastKey + " first try"))
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
                                        using (TrackOperation("BATCH " + paddedBatchId + " COMPARE: Comparing files first try"))
                                        {
                                            List<S3Object> objects = batch.Response.S3Objects;
                                            int count = objects.Count;
                                            batchItems = count;
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
                                                Task copyTask = Task.Run(async () =>
                                                    {
                                                        // create the copy request object RIGHT before we issue the command because otherwise AWS may think that system clocks are out of sync
                                                        try
                                                        {
                                                            Interlocked.Increment(ref _CopiesInProgress);
                                                            Stopwatch timer = Stopwatch.StartNew();
                                                            // create the copy request object RIGHT before we issue the command because otherwise AWS may think that system clocks are out of sync
                                                            CopyObjectRequest copyRequest = copyInfo.CopyObjectRequestBuilder();
                                                            Program.State.AddChargeForCopies(1);
                                                            using (Program.TrackOperation("BATCH " + paddedBatchId + " COPY: " + copyInfo.BatchObjectNumber.ToString("000") + ": " + copyInfo.Key + " (" + (copyInfo.SourceObject.Size + 500000) / 1000000 + "MB)"))
                                                            {
                                                                await copyInfo.S3.CopyObjectAsync(copyRequest, batchFirstPassCancellationToken.Token);
                                                            }
                                                            string operation = String.Format("{0}.{1} {2} ({3:F0}MB:{4})", batch.BatchId, copyInfo.BatchObjectNumber, (copyInfo.TargetObject == null) ? "copied" : "updated", copyInfo.SourceObject.Size / 1000000.0, copyInfo.Key);
                                                            if (copyInfo.TargetObject != null) operation += " " + copyInfo.SourceObject.ETag + " vs " + copyInfo.TargetObject.ETag;
                                                            Program.LogVerbose(operation);
                                                            // track what the operation we just completed successfully
                                                            Program.State.TrackObject(copyInfo.SourceObject, copyInfo.TargetObject == null, copyInfo.TargetObject != null);
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
                                                            Program.State.TrackObject(copyInfo.SourceObject, copyInfo.TargetObject == null, copyInfo.TargetObject != null);
                                                        }
                                                        finally
                                                        {
                                                            Interlocked.Decrement(ref _CopiesInProgress);
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
                                        using (TrackOperation("BATCH " + paddedBatchId + " PASS1: Waiting for first pass to timeout or complete"))
                                        {
                                            // wait for the tasks and catch any exceptions from them
                                            Task.WaitAll(workToDo.Values.Select(w => w.Task).ToArray());
                                            // if we get here without throwing, it means we successfully copied all the files, so we can clear the work to do list so we skip the non-async 2nd pass below
                                            workToDo.Clear();
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
                                        Program.Error("Batch " + batch.BatchId + " has failed.  The process will need to be rerun after the problem is corrected: " + ex.ToString());
                                        throw;
                                    }
                                    // still more work to do?
                                    if (workToDo.Count > 0)
                                    {
                                        // save the count while we attempt to process items synchronously
                                        BucketObjectsWindow.NeededCopyInfo[] leftToDo = workToDo.Values.Where(c => c.Task.IsCanceled || c.Task.IsFaulted).ToArray();
                                        Program.LogVerbose("Batch " + batch.BatchId + " retrying " + leftToDo.Length + " failed operations without async.");
                                        foreach (BucketObjectsWindow.NeededCopyInfo neededCopyInfo in leftToDo)
                                        {
                                            BucketObjectsWindow.NeededCopyInfo copyInfo = neededCopyInfo;
                                            _copyPool.RunAsync(() =>
                                               {
                                                   for (int attempt = 0; attempt < 10; ++attempt)
                                                   {
                                                       try
                                                       {
                                                           Interlocked.Increment(ref _CopiesInProgress);
                                                           Stopwatch timer = Stopwatch.StartNew();
                                                           // create the copy request object RIGHT before we issue the command because otherwise AWS may think that system clocks are out of sync
                                                           CopyObjectRequest copyRequest = copyInfo.CopyObjectRequestBuilder();
                                                           // increase the timeout on each retry
                                                           copyRequest.Timeout = TimeSpan.FromTicks((copyRequest.Timeout ?? TimeSpan.FromSeconds(30)).Ticks * (1 + attempt));
                                                           Program.State.AddChargeForCopies(1);
                                                           using (Program.TrackOperation("BATCH " + paddedBatchId + " RETRYCOPY" + (attempt + 1) + ": " + copyInfo.BatchObjectNumber.ToString("000") + ": " + copyInfo.Key + " (" + (copyInfo.SourceObject.Size + 500000) / 1000000 + "MB)"))
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
                                                           Program.State.TrackObject(copyInfo.SourceObject, copyInfo.TargetObject == null, copyInfo.TargetObject != null);
                                                           // success-no need to loop any more
                                                           break;
                                                       }
                                                       catch (AmazonServiceException ex)
                                                       {
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
                                                           Program.State.TrackObject(copyInfo.SourceObject, copyInfo.TargetObject == null, copyInfo.TargetObject != null);
                                                       }
                                                       finally
                                                       {
                                                           Interlocked.Decrement(ref _CopiesInProgress);
                                                       }
                                                   }
                                               });
                                        }
                                    }
                                    // wait for synchronous copies to complete
                                    int remaining = -1;
                                    using (TrackOperation("BATCH " + paddedBatchId + " RETRYWAIT: Waiting for non-async copies: ", () => (remaining < 0) ? "unknown" : remaining.ToString()))
                                    {
                                        while ((remaining = workToDo.Values.Where(c => c != null && c.Task != null && (c.Task.IsCanceled || c.Task.IsFaulted)).Count()) > 0)
                                        {
                                            System.Threading.Thread.Sleep(500);
                                        }
                                    }
                                    // we're now done, but we may need to wait for the previous batch before marking ourselves as processed (this way we only have to keep one bookmark)
                                    using (TrackOperation("BATCH " + paddedBatchId + " PREVBATCH: Waiting for previous batch"))
                                    {
                                        previousBatchCompletedCopy.WaitOne();
                                    }
                                    using (TrackOperation("BATCH " + paddedBatchId + " CLEANUP: Finishing batch"))
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
                catch (Exception e)
                {
                    Program.Error("Exception processing items: " + e.ToString());
                }
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
            return String.Format(System.Globalization.CultureInfo.InvariantCulture, "{0:0%}/{1:0%}/{2:0%}/{3:0%}", waiting / total, running / total, retry / total, (completed + alreadyUpToDate) / total);
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
        /// <param name="message">A message to include with the exception.</param>
        /// <param name="ex">The <see cref="System.Exception"/> that occurred.</param>
        public static void Exception(string message, Exception ex)
        {
            Console.WriteLine((message ?? "ERROR: ") + ex.Message + " (see error log file for details)");
            try
            {
                if (_Error != null) _Error.WriteLine(ex.ToString());
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
                List<OperationTracker> list = new List<OperationTracker>();
                OperationTracker leastCopy = null;
                OperationTracker greatestCopy = null;
                foreach (OperationTracker operation in OperationTracker.EnumerateOperationsInProgress)
                {
                    if (level > 2 || (!operation.Description.Contains(" COPY: ") && !operation.Description.Contains(" RETRYCOPY")))
                    {
                        list.Add(operation);
                    }
                    else
                    {
                        if (leastCopy == null || String.CompareOrdinal(operation.Description, leastCopy.Description) < 0) leastCopy = operation;
                        if (greatestCopy == null || String.CompareOrdinal(operation.Description, greatestCopy.Description) > 0) greatestCopy = operation;
                    }
                }
                if (level == 2)
                {
                    if (leastCopy != null && greatestCopy != null)
                    {
                        list.Add(leastCopy);
                        if (!String.Equals(leastCopy.Description, greatestCopy.Description)) list.Add(greatestCopy);
                    }
                }
                list.Sort((a,b) => String.CompareOrdinal(a.Description, b.Description));
                foreach (OperationTracker operation in list)
                {
                    operations.AppendLine(operation.Description + " " + operation.DynamicDescription + " " + operation.Stopwatch.ElapsedMilliseconds.ToString() + "ms");
                }
                if (level >= 2)
                {
                    foreach (WorkerPool pool in new WorkerPool[] { _processingPool, _copyPool })
                    operations.AppendLine(pool.Name + ": " + pool.BusyWorkers + " workers, " + pool.Workers + " busy, " + pool.ReadyWorkers + " ready, " + pool.PeakConcurrentWorkersUsedRecently + " peak");
                }
            }
            operations.AppendLine();
            Program.Log(operations.ToString());
        }

        /// <summary>
        /// Tracks an operation using the specified key and description.
        /// </summary>
        /// <param name="description">A description for the operation that is still in progress.</param>
        /// <param name="dynamicDescription">An optional function that provides a separate dynamically generated description.</param>
        /// <returns>A <see cref="IDisposable"/> object that will keep the description string in the list of operations in progress until it is disposed.</returns>
        public static IDisposable TrackOperation(string description, Func<string> dynamicDescription = null)
        {
            return new OperationTracker(description, dynamicDescription);
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

            private readonly Stopwatch _timer = Stopwatch.StartNew();
            private readonly string _description;
            private readonly Func<string> _dynamicDescription;

            public OperationTracker(string description, Func<string> dynamicDescription = null)
            {
                _operationsInProgress[this] = null;
                _description = description;
                _dynamicDescription = dynamicDescription;
            }

            /// <summary>
            /// Gets the <see cref="Stopwatch"/> that is timing this operation.
            /// </summary>
            public Stopwatch Stopwatch { get { return _timer; } }
            /// <summary>
            /// Gets the description of this operation.  This item is stable, so it can be used for sorting.
            /// </summary>
            public string Description { get { return _description; } }
            /// <summary>
            /// Gets the dynamic description of this operation.  This value is computed as it is called, so it can change and cannot not be used for sorting.
            /// </summary>
            public string DynamicDescription { get { return (_dynamicDescription == null) ? "" : _dynamicDescription(); } }

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
