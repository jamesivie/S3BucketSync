using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
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
    [Serializable]
    class State
    {
        private static IFormatter _Serializer = new BinaryFormatter();
        private readonly string _sourceRegionBucketAndPrefix;
        private readonly string _targetRegionBucketAndPrefix;
        private readonly string _grant;
        private readonly DateTime _startDate;
        // everything after this is interlocked
        private long _unrecordedTimeStartTicks;
        private long _ticksUsed;
        private long _nanocents;
        private long _earliestDate = DateTime.MaxValue.Ticks;
        private long _earliestCopiedDate = DateTime.MaxValue.Ticks;
        private long _earliestUpdatedDate = DateTime.MaxValue.Ticks;
        private long _latestDate = DateTime.MinValue.Ticks;
        private long _latestCopiedDate = DateTime.MinValue.Ticks;
        private long _latestUpdatedDate = DateTime.MinValue.Ticks;
        private long _bytesProcessed;
        private long _bytesCopied;
        private long _bytesUpdated;
        private long _bytes;
        private long _objectsProcessed;
        private long _objectsCopied;
        private long _objectsUpdated;
        private long _objects;
        private long _starvedTasks;
        private int _sourceQueries;
        private int _targetQueries;
        private string _sourceContinuationToken;
        private int _lastCompletedBatchId;
        private string _lastKeyOfLastBatchCompleted;
        private BatchIdCounter _sourceBatchId;

        /// <summary>
        /// Gets the number of bytes processed so far.
        /// </summary>
        public long BytesProcessed { get { return _bytesProcessed; } }
        /// <summary>
        /// Gets the number of bytes copied so far.
        /// </summary>
        public long BytesCopied { get { return _bytesCopied; } }
        /// <summary>
        /// Gets the number of bytes updated so far.
        /// </summary>
        public long BytesUpdated { get { return _bytesUpdated; } }
        /// <summary>
        /// Gets the number of objects processed so far.
        /// </summary>
        public long ObjectsProcessed { get { return _objectsProcessed; } }
        /// <summary>
        /// Gets the number of objects copied so far.
        /// </summary>
        public long ObjectsCopied { get { return _objectsCopied; } }
        /// <summary>
        /// Gets the number of objects updated so far.
        /// </summary>
        public long ObjectsUpdated { get { return _objectsUpdated; } }

        private static string AdjustAndAddSuffix(double value, double adjustFactor, string suffix, int lengthLimit = 4)
        {
            string s = ((value + (adjustFactor / 2000)) / adjustFactor).ToString();
            s = s.Substring(0, Math.Min(s.Length, lengthLimit)).TrimEnd('0').TrimEnd('.');
            return (string.IsNullOrEmpty(s) ? "0" : s) + suffix;
        }
        public static string MagnitudeConvert(double value, int digits = 3)
        {
            if (value < 1000) return AdjustAndAddSuffix(value, 1, "", digits + 2);
            if (value < 1000000) return AdjustAndAddSuffix(value, 1000, "K", digits + 1);
            if (value < 1000000000) return AdjustAndAddSuffix(value, 1000000, "M", digits + 1);
            if (value < 1000000000000) return AdjustAndAddSuffix(value, 1000000000, "G", digits + 1);
            if (value < 1000000000000000) return AdjustAndAddSuffix(value, 1000000000000, "T", digits + 1);
            if (value < 1000000000000000000) return AdjustAndAddSuffix(value, 1000000000000000, "P", digits + 1);
            if (value < 1000000000000000000000.0) return AdjustAndAddSuffix(value, 1000000000000000000, "E", digits + 1);
            return value.ToString("E3");
        }
        /// <summary>
        /// Constructs a state object for the sync operation from the specified source to the specified destination.
        /// </summary>
        /// <param name="sourceRegionBucketAndPrefix">The source region, bucket, and prefix.</param>
        /// <param name="targetRegionBucketAndPrefix">The target region, bucket, and prefix.</param>
        /// <param name="grant">A string indicating rights being granted to the target file.</param>
        public State(string sourceRegionBucketAndPrefix, string targetRegionBucketAndPrefix, string grant)
        {
            _sourceRegionBucketAndPrefix = sourceRegionBucketAndPrefix;
            _targetRegionBucketAndPrefix = targetRegionBucketAndPrefix;
            _grant = grant;
            _startDate = DateTime.UtcNow;
            _unrecordedTimeStartTicks = _startDate.Ticks;
            _ticksUsed = 0;
            _sourceBatchId = new BatchIdCounter();
        }
        /// <summary>
        /// Gets a string representation of the state.
        /// </summary>
        /// <returns>A string representation of the state.</returns>
        public override string ToString()
        {
            string output = String.Format(
                "${0:F2} QUERY:{1},{2} O:{3}c/{4}u/{5}p B:{6}c/{7}u/{8}p",
                DollarCostSoFar,
                _sourceQueries,
                _targetQueries,
                MagnitudeConvert(_objectsCopied),
                MagnitudeConvert(_objectsUpdated),
                MagnitudeConvert(_objectsProcessed),
                MagnitudeConvert(_bytesCopied),
                MagnitudeConvert(_bytesUpdated),
                MagnitudeConvert(_bytesProcessed)
                );
            return output;
        }
        /// <summary>
        /// Gets a string representation of the state.
        /// </summary>
        /// <returns>A string representation of the state.</returns>
        public string Report()
        {
            DateTime utcNow = DateTime.UtcNow;
            StringBuilder str = new StringBuilder();
            str.AppendLine("====================================================================");
            str.AppendFormat("{0}{1}", utcNow.ToLocalTime(), Environment.NewLine);
            str.AppendFormat("Total Time: {0}{1}", TimeUsed, Environment.NewLine);
            str.AppendFormat("Cost: ${0:F2}{1}", DollarCostSoFar, Environment.NewLine);
            str.AppendFormat("Source: {0}{1}", _sourceRegionBucketAndPrefix, Environment.NewLine);
            str.AppendFormat("Source Queries Run: {0}{1}", _sourceQueries, Environment.NewLine);
            str.AppendFormat("Target: {0}{1}", _targetRegionBucketAndPrefix, Environment.NewLine);
            str.AppendFormat("Target Queries Run: {0}{1}", _targetQueries, Environment.NewLine);
            str.AppendFormat("Objects Copied: {0} ({1:F0}MB){2}", _objectsCopied, _bytesCopied / 1000000.0, Environment.NewLine);
            str.AppendFormat("Objects Updated: {0} ({1:F0}MB){2}", _objectsUpdated, _bytesUpdated / 1000000.0, Environment.NewLine);
            str.AppendFormat("Objects Processed: {0} ({1:F0}MB){2}", _objectsProcessed, _bytesProcessed / 1000000.0, Environment.NewLine);
            str.AppendFormat("Total Objects: {0} ({1:F0}MB){2}", _objects, _bytes / 1000000.0, Environment.NewLine);
            str.AppendFormat("Earliest Object Date: {0}{1}", new DateTime(_earliestDate), Environment.NewLine);
            str.AppendFormat("Earliest Copied Object Date: {0}{1}", new DateTime(_earliestCopiedDate), Environment.NewLine);
            str.AppendFormat("Earliest Updated Object Date: {0}{1}", new DateTime(_earliestUpdatedDate), Environment.NewLine);
            str.AppendFormat("Latest Object Date: {0}{1}", new DateTime(_latestDate), Environment.NewLine);
            str.AppendFormat("Latest Copied Object Date: {0}{1}", new DateTime(_latestCopiedDate), Environment.NewLine);
            str.AppendFormat("Latest Updated Object Date: {0}{1}", new DateTime(_latestUpdatedDate), Environment.NewLine);
            if (_grant != null) str.AppendFormat("Grant: {0}{1}", _grant, Environment.NewLine);
            str.AppendLine();
            return str.ToString();
        }
        /// <summary>
        /// Gets the amount of time used doing the synchronization.
        /// </summary>
        public TimeSpan TimeUsed {  get { return new TimeSpan(_ticksUsed + DateTime.UtcNow.Ticks - _unrecordedTimeStartTicks); } }
        /// <summary>
        /// Gets the grant string.
        /// </summary>
        public string GrantString
        {
            get { return _grant; }
        }
        /// <summary>
        /// Gets the source continuation token, which tracks the next batch of source items needing to be processed.
        /// </summary>
        public string SourceContinuationToken
        {
            get { return _sourceContinuationToken; }
        }
        /// <summary>
        /// Gets the last key of the last batch that was completed.
        /// </summary>
        public string LastKeyOfLastBatchCompleted
        {
            get { return _lastKeyOfLastBatchCompleted; }
        }
        /// <summary>
        /// Gets the <see cref="BatchIdCounter"/> for the source batch window.
        /// </summary>
        public BatchIdCounter SourceBatchId { get { return _sourceBatchId; } }
        /// <summary>
        /// Gets the number of tasks that have starved.
        /// </summary>
        public long StarvedTasks { get { return _starvedTasks; } }

        /// <summary>
        /// Records that a query took place.
        /// </summary>
        /// <param name="source">Whether or not the query was on the source bucket (as opposed to the target bucket).</param>
        /// <param name="count">The number of queries performed.</param>
        public void RecordQueries(bool source, int count = 1)
        {
            if (source) Interlocked.Add(ref _sourceQueries, count); else Interlocked.Add(ref _targetQueries, count);
        }

        /// <summary>
        /// Tracks charges for copy operations.
        /// </summary>
        public void AddChargeForCopies(int numberOfCopies = 1)
        {
            // $.005 / 1000 COPY requests  (we're assuming standard access files here)
            Interlocked.Add(ref _nanocents, 500L * numberOfCopies);
        }
        /// <summary>
        /// Tracks charges for query operations.
        /// </summary>
        public void AddChargeForQueries(int numberOfQueries = 1)
        {
            // $.005 / 1000 LIST requests  (we're assuming standard access files here)
            Interlocked.Add(ref _nanocents, 500L * numberOfQueries);
        }
        /// <summary>
        /// Record an instance of task starvation.
        /// </summary>
        public void RecordTaskStarvation()
        {
            Interlocked.Increment(ref _starvedTasks);
        }
        /// <summary>
        /// Tracks the specified <see cref="S3Object"/>.
        /// </summary>
        /// <param name="o">The <see cref="S3Object"/> being synchronized.</param>
        /// <param name="copied">Whether or not the item needed to be copied.</param>
        /// <param name="updated">Whether or not the item needed to be udpated.</param>
        public void TrackObject(S3Object o, bool copied, bool updated)
        {
            InterlockedMin(ref _earliestDate, o.LastModified.Ticks);
            InterlockedMax(ref _latestDate, o.LastModified.Ticks);
            Interlocked.Add(ref _bytesProcessed, o.Size);
            Interlocked.Add(ref _objectsProcessed, 1);
            if (copied)
            {
                InterlockedMin(ref _earliestCopiedDate, o.LastModified.Ticks);
                InterlockedMax(ref _latestCopiedDate, o.LastModified.Ticks);
                Interlocked.Add(ref _bytesCopied, o.Size);
                Interlocked.Add(ref _objectsCopied, 1);
            }
            if (updated)
            {
                InterlockedMin(ref _earliestUpdatedDate, o.LastModified.Ticks);
                InterlockedMax(ref _latestUpdatedDate, o.LastModified.Ticks);
                Interlocked.Add(ref _bytesUpdated, o.Size);
                Interlocked.Add(ref _objectsUpdated, 1);
            }
        }
        /// <summary>
        /// Tracks the completion of a batch of objects.
        /// </summary>
        /// <param name="prefix">The prefix for the batch window the batch belongs to.</param>
        /// <param name="batch">The <see cref="BucketObjectsWindow.Batch"/> that was completed</param>
        public void TrackBatchCompletion(string prefix, BucketObjectsWindow.Batch batch)
        {
            Interlocked.Exchange(ref _sourceContinuationToken, batch.Response.ContinuationToken);
            Interlocked.Exchange(ref _lastCompletedBatchId, batch.BatchId);
            Interlocked.Exchange(ref _lastKeyOfLastBatchCompleted, batch.Response.S3Objects[batch.Response.S3Objects.Count - 1].Key.Substring(prefix.Length));
            long bytes = 0;
            foreach (S3Object o in batch.Response.S3Objects)
            {
                bytes += o.Size;
            }
            Interlocked.Add(ref _objects, batch.Response.S3Objects.Count);
            Interlocked.Add(ref _bytes, bytes);
        }
        /// <summary>
        /// Gets the number of dollars in cost accumulated since the synchronization started (carried across restarts).
        /// </summary>
        public double DollarCostSoFar { get { return _nanocents / 100000000.0; } }
        /// <summary>
        /// Reads the state from the specified file.
        /// </summary>
        /// <param name="filename">The name of the file to read the state from.</param>
        /// <param name="sourceRegionBucketAndPrefix">The source region, bucket, and prefix.</param>
        /// <param name="targetRegionBucketAndPrefix">The target region, bucket, and prefix.</param>
        /// <returns>The <see cref="State"/> object.</returns>
        public static State Read(string filename, string sourceRegionBucketAndPrefix, string targetRegionBucketAndPrefix)
        {
            try
            {
                using (FileStream f = new FileStream(filename, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None))
                {
                    // no state to read
                    if (f.Length < 1) return null;
                    // read the state from the state file
                    State read = (State)_Serializer.Deserialize(f);
                    // we haven't been doing anything since we saved, so skip the unrecorded time ahead to now
                    read._unrecordedTimeStartTicks = DateTime.UtcNow.Ticks;
                    read._sourceBatchId = new BatchIdCounter(read._lastCompletedBatchId);
                    // source or target mismatch?  don't use this one!
                    if (!String.Equals(sourceRegionBucketAndPrefix, read._sourceRegionBucketAndPrefix)
                        || !String.Equals(targetRegionBucketAndPrefix, read._targetRegionBucketAndPrefix)) return null;
                    // source and target match, so we're good to use this state to pick up where we left off
                    return read;
                }
            }
            catch (Exception e)
            {
                string message = "Exception attempting to read state resume information: " + e.ToString();
                Program.Error(message);
            }
            return null;
        }
        /// <summary>
        /// Saves the state
        /// </summary>
        /// <param name="filename"></param>
        public void Save(string filename)
        {
            for (int attempt = 0; ; ++attempt)
            {
                try
                {
                    _ticksUsed += DateTime.UtcNow.Ticks - _unrecordedTimeStartTicks;
                    _unrecordedTimeStartTicks = DateTime.UtcNow.Ticks; // this compensates for having just added up the time, just in case someone asks for the time sent after this
                    using (FileStream f = new FileStream(filename, FileMode.Create, FileAccess.ReadWrite, FileShare.None))
                    {
                        _Serializer.Serialize(f, this);
                        f.Flush();
                        f.Close();
                    }
                    Program.LogVerbose("Bookmarking completion of batch " + _lastCompletedBatchId);
                    return;
                }
                catch (IOException ex)
                {
                    if (attempt > 10)
                    {
                        Program.Error("Exception saving state: " + ex.ToString());
                        throw;
                    }
                    // wait a bit and try again
                    Thread.Sleep(100);
                    continue;
                }
            }
        }
        /// <summary>
        /// Deletes the state so the next run will start from the beginning.
        /// </summary>
        /// <param name="filename">The filename for the state information.</param>
        public void Delete(string filename)
        {
            for (int attempt = 0; ; ++attempt)
            {
                try
                {
                    File.Delete(filename);
                    return;
                }
                catch (IOException)
                {
                    if (attempt > 10) throw;
                    // wait a bit and try again
                    Thread.Sleep(100);
                    continue;
                }
            }
        }
        /// <summary>
        /// Replaces the value with the specified value if the specified value is greater.
        /// </summary>
        /// <param name="valueReference">A reference to the value being manipulated.</param>
        /// <param name="possibleNewMax">The value to replace the value with if it is greater.</param>
        /// <returns>The new maximum value.</returns>
        private static long InterlockedMax(ref long valueReference, long possibleNewMax)
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
        /// <summary>
        /// Replaces the value with the specified value if the specified value is greater.
        /// </summary>
        /// <param name="valueReference">A reference to the value being manipulated.</param>
        /// <param name="possibleNewMin">The value to replace the value with if it is greater.</param>
        /// <returns>The new minimum value.</returns>
        private static long InterlockedMin(ref long valueReference, long possibleNewMin)
        {
            long oldValue = valueReference;
            // loop attempting to put it in until we win the race
            while (possibleNewMin < oldValue)
            {
                // try to put in our value--did we win the race?
                if (oldValue == System.Threading.Interlocked.CompareExchange(ref valueReference, possibleNewMin, oldValue))
                {
                    // we're done and we were the new min
                    return possibleNewMin;
                }
                // update our value
                oldValue = valueReference;
            }
            // we're done and we were NOT the new min
            return oldValue;
        }
    }
}
