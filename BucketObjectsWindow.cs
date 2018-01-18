using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
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
    class BucketObjectsWindow
    {
        // Amazon S3 connection info
        private readonly IAmazonS3 _s3;
        private readonly string _bucket;
        private readonly string _prefix;
        private readonly S3Grant _grant;
        private readonly S3CannedACL _grantCannedAcl;
        private readonly ServerSideEncryptionMethod _targetEncryptionMethod;
        /// <summary>
        /// A monitor to serialize removing batches from the queue.
        /// </summary>
        private object _removeFromQueueMonitor = new object();
        /// <summary>
        /// A FIFO queue of objects each containing the S3 objects from one query.
        /// Items are removed from the queue as their processing is completed and added to the queue as new items are needed.
        /// There should be enough items in the queue to fully utilize the parallel processing available.
        /// </summary>
        private ConcurrentQueue<Batch> _queue;
        private BatchIdCounter _batchIdCounter;
        // various state-keeping interlocked data:
        private int _lastBatchHasBeenRead;
        private int _dequeuedBatchId;
        private int _lastQueuedBatchId;
        private string _unprefixedLeastKey;
        private string _unprefixedGreatestKey;
        private string _startAtKey;
        private string _stopAtKey;
        private string _continuationToken;
        private string _lastQuery;

        /// <summary>
        /// Constructs a new empty BucketObjectsWindow.
        /// </summary>
        /// <param name="regionBucketAndPrefix">The region, bucket, and prefix, in the following form: [region:]bucket/prefix.</param>
        /// <param name="batchIdCounter">The <see cref="BatchIdCounter"/> for this window.</param>
        /// <param name="unprefixedStartAtKey">The key to start at or <b>null</b> to start at the beginning.</param>
        /// <param name="unprefixedStopAtKey">The key to stop at or <b>null</b> to start at the beginning.</param>
        /// <param name="cannedAcl">A <see cref="S3CannedACL"/> to use for the target file.</param>
        /// <param name="grant">A <see cref="S3Grant"/> indicating rights grants to apply to the target file.</param>
        public BucketObjectsWindow(string regionBucketAndPrefix, BatchIdCounter batchIdCounter, string unprefixedStartAtKey = null, string unprefixedStopAtKey = null, S3CannedACL cannedAcl = null, S3Grant grant = null, ServerSideEncryptionMethod targetEncryptionMethod = null)
        {
            _batchIdCounter = batchIdCounter;
            Tuple<string, string, string> parsedRegionBucketAndPrefix = ParseRegionBucketAndPrefix(regionBucketAndPrefix);
            Amazon.RegionEndpoint region = Amazon.RegionEndpoint.GetBySystemName(string.IsNullOrEmpty(parsedRegionBucketAndPrefix.Item1)
                ? GetBucketRegion(parsedRegionBucketAndPrefix.Item2)
                : parsedRegionBucketAndPrefix.Item1);
            _s3 = new AmazonS3Client(region);
            _bucket = parsedRegionBucketAndPrefix.Item2;
            _prefix = parsedRegionBucketAndPrefix.Item3;
            _grant = grant;
            _grantCannedAcl = cannedAcl;
            _queue = new ConcurrentQueue<Batch>();
            if (!string.IsNullOrEmpty(unprefixedStartAtKey))
            {
                _startAtKey = _prefix + unprefixedStartAtKey;
                _unprefixedLeastKey = unprefixedStartAtKey;
            }
            else
            {
                _unprefixedLeastKey = string.Empty;
            }
            if (!string.IsNullOrEmpty(unprefixedStopAtKey))
            {
                _stopAtKey = _prefix + unprefixedStopAtKey;
            }
            _unprefixedGreatestKey = string.Empty;
            _targetEncryptionMethod = targetEncryptionMethod;
        }

        public static string NormalizeRegionBucketAndPrefix(string regionBucketAndPrefix)
        {
            Tuple<string, string, string> parsedRegionBucketAndPrefix = ParseRegionBucketAndPrefix(regionBucketAndPrefix);
            return (string.IsNullOrEmpty(parsedRegionBucketAndPrefix.Item1) ? GetBucketRegion(parsedRegionBucketAndPrefix.Item2) : parsedRegionBucketAndPrefix.Item1) + ":" + parsedRegionBucketAndPrefix.Item2 + "/" + parsedRegionBucketAndPrefix.Item3;
        }

        public static byte[] ComputeMD5Hash(Stream stream)
        {
            using (MD5 hash = MD5.Create())
            {
                stream.Position = 0;
                byte[] data = hash.ComputeHash(stream);
                stream.Position = 0;
                return data;
            }
        }

        /// <summary>
        /// Marks a batch processed (it was removed from the queue when processing began, but this updates the least key and subtracts the items).
        /// </summary>
        /// <param name="batch">The batch whose processing has been completed.</param>
        public void MarkBatchProcessed(Batch batch)
        {
            string lastKey = batch.GreatestKey;
            // move the least key forward
            Interlocked.Exchange(ref _unprefixedLeastKey, lastKey);
        }
        /// <summary>
        /// Moves the least side of the window forward, shrinking the size of the window so that it no longer includes keys less than or equal to the specified key.
        /// </summary>
        /// <param name="key">The last key of the window to be removed.</param>
        /// <returns>The continuation token for the batch that was removed from the front of the collection, which represents the continuation token that would be needed if processing were interrupted and needed to be restarted.</returns>
        public string ShrinkWindow(string key)
        {
            Batch lastBatchDequeued = null;
            if (key == null) key = String.Empty;
            lock (_removeFromQueueMonitor)
            {
                Batch batch;
                while (_queue.TryPeek(out batch))
                {
                    // is this batch's greatest key less than or equal to the specified key?
                    if (String.CompareOrdinal(batch.GreatestKey, key) <= 0)
                    {
                        // remove this batch
                        _queue.TryDequeue(out batch);
                        // save this as the last batch dequeued
                        lastBatchDequeued = batch;
                        // mark this batch as dequeued
                        Interlocked.Exchange(ref _dequeuedBatchId, batch.BatchId);
                        // move the least key forward
                        Interlocked.Exchange(ref _unprefixedLeastKey, batch.GreatestKey);
                        // just to be sure--we shouldn't have removed a batch with keys greater than or equal to the specified key (must be some kind of bug if we did because this is the only place we remove and we entered the monitor here)
                        System.Diagnostics.Debug.Assert(String.CompareOrdinal(batch.GreatestKey, key) <= 0);
                    }
                    // we still have more data to process in this batch (and possibly other batches as well)
                    else break;
                }
            }
            // return the next continuation key after the batch that has been fully processed
            return (lastBatchDequeued == null) ? null : lastBatchDequeued.Response.NextContinuationToken;
        }
        /// <summary>
        /// Gets the next batch in the queue in order to process it.
        /// </summary>
        /// <returns>A <see cref="Batch"/> containing the list of object to process and the continuation token to save when processing is complete.</returns>
        public Batch DequeueBatch()
        {
            Batch batch;
            if (!_queue.TryDequeue(out batch))
            {
                return null;
            }
            // mark this batch as dequeued
            Interlocked.Exchange(ref _dequeuedBatchId, batch.BatchId);
            if (batch.Response.S3Objects.Count < 1) return null;
            System.Diagnostics.Debug.Assert(batch.Response.S3Objects[0].Key.StartsWith(batch.Response.Prefix));
            System.Diagnostics.Debug.Assert(batch.Response.S3Objects[batch.Response.S3Objects.Count - 1].Key.StartsWith(batch.Response.Prefix));
            return batch;
        }

        /// <summary>
        /// Reads the next batch and returns how many object were in it.
        /// </summary>
        /// <returns>The number of objects in the batch.</returns>
        public int ReadNextBatch()
        {
            Batch batch = GetNextBatch();
            return batch == null ? 0 : batch.Response.S3Objects.Count;
        }

        private Batch GetNextBatch()
        {
            // already queued the last batch? return an indicator that we're at the end!
            if (_lastBatchHasBeenRead != 0) return null;
            ListObjectsV2Request request = BuildRequest();
            Program.State.AddChargeForQueries(1);
            int batchId = _batchIdCounter.Next;
            string operation = "QUERY: Get batch " + batchId + " from " + _bucket + "/" + _prefix;
            Interlocked.Exchange(ref _lastQuery, operation);
            Stopwatch timer = Stopwatch.StartNew();
            ListObjectsV2Response response = null;
            using (Program.TrackOperation(operation))
            {
                response = _s3.ListObjectsV2(request);
            }
            if (!response.IsTruncated)
            {
                Program.LogVerbose("Last batch from " + _bucket + "/" + _prefix + "!");
            }
            using (Program.TrackOperation("QUERY: processing batch " + batchId + " from " + _bucket + "/" + _prefix + " (" + response.S3Objects.Count + ")"))
            {
                return ProcessResponse(batchId, response);
            }
        }

        private Batch ProcessResponse(int batchId, ListObjectsV2Response response)
        {
            // move to the next batch
            Interlocked.Exchange(ref _continuationToken, response.NextContinuationToken);
            Interlocked.Exchange(ref _startAtKey, null);
            // build the batch for the target objects
            Batch batch = new Batch(_prefix, batchId, _stopAtKey, response);
            // we should only ever add batches whose key ranges are after the ones we already had
            if (!string.IsNullOrEmpty(_unprefixedGreatestKey) && String.CompareOrdinal(batch.LeastKey, _unprefixedGreatestKey) < 0) throw new InvalidOperationException("The specified response is for objects that are lesser than the ones already processing.  Responses must be processed in order (" + batch.LeastKey + "/" + _unprefixedGreatestKey + ").");
            if (!string.IsNullOrEmpty(_unprefixedGreatestKey) && String.CompareOrdinal(batch.GreatestKey, _unprefixedGreatestKey) < 0) throw new InvalidOperationException("The specified response is for objects that are lesser than the ones already processing.  Responses must be processed in order (" + batch.GreatestKey + "/" + _unprefixedGreatestKey + ").");
            // no least key specified yet? use the one from the batch
            if (string.IsNullOrEmpty(_unprefixedLeastKey)) Interlocked.Exchange(ref _unprefixedLeastKey, batch.LeastKey);
            // the greatest key should be the one from this new batch
            Interlocked.Exchange(ref _unprefixedGreatestKey, batch.GreatestKey);
            // add this batch to the queue
            _queue.Enqueue(batch);
            Interlocked.Exchange(ref _lastQueuedBatchId, batchId);
            // is this the last batch?
            Interlocked.Exchange(ref _lastBatchHasBeenRead, batch.Response.IsTruncated ? 0 : 1);
            return batch;
        }

        private ListObjectsV2Request BuildRequest()
        {
            // get the next batch of objects in the target bucket with the target prefix
            ListObjectsV2Request request = new ListObjectsV2Request();
            request.BucketName = _bucket;
            request.Prefix = _prefix;
            request.ContinuationToken = _continuationToken;
            if (_startAtKey != null) request.StartAfter = _startAtKey;
            return request;
        }

        public class NeededCopyInfo
        {
            public Func<CopyObjectRequest> CopyObjectRequestBuilder { get; set; }
            public int BatchObjectNumber { get; set; }
            public string Key { get; set; }
            public S3Object SourceObject { get; set; }
            public S3Object TargetObject { get; set; }
            public IAmazonS3 S3 { get; set; }
            public Task Task { get; set; }
        }
        /// <summary>
        /// Checks to see if the object needs updating and returns a asynchronous delegate that will do the job.
        /// </summary>
        /// <param name="batchNumber">The batch number.</param>
        /// <param name="objectNumber">The object number within the batch.</param>
        /// <param name="key">The unprefixed key for the object to be checked.</param>
        /// <param name="sourceObject">The source object.</param>
        /// <param name="cancel">A <see cref="CancellationToken"/> the caller can use to cancel the operation before it completes.</param>
        /// <returns>A <see cref="NeededCopyInfo"/> that contains the data needed to create the copy request and track it's operation, or <param name="batch">null</param> if no action was needed.</returns>
        internal NeededCopyInfo CheckIfObjectNeedsUpdate(Batch batch, int objectNumber, string key)
        {
            S3Object sourceObject = batch.Response.S3Objects[objectNumber];
            // the item should be within the current bounds of the window
            System.Diagnostics.Debug.Assert(String.CompareOrdinal(key, _unprefixedLeastKey) >= 0);
            System.Diagnostics.Debug.Assert(String.CompareOrdinal(key, _unprefixedGreatestKey) <= 0);
            // first lookup the item in this window
            S3Object targetObject = FindObject(key);
            // do we need to copy or update this item
            if (targetObject == null || sourceObject.ETag != targetObject.ETag)
            {
                // is this too big for a single copy request?
                if (sourceObject.Size > UInt32.MaxValue)
                {
                    Program.Error("File " + batch.BatchId + "." + objectNumber + " (" + sourceObject.BucketName + "/" + sourceObject.Key + ") needs to be synchronized, but cannot be because it is too large!");
                }
                else
                {
                    return new NeededCopyInfo
                    {
                        CopyObjectRequestBuilder = () =>
                            {
                                // create the copy request object RIGHT before we issue the command because otherwise AWS may think that system clocks are out of sync
                                CopyObjectRequest request = new CopyObjectRequest();
                                request.Timeout = TimeSpan.FromSeconds(Program.TimeoutSeconds);
                                request.ReadWriteTimeout = TimeSpan.FromSeconds(Program.TimeoutSeconds);
                                request.SourceBucket = sourceObject.BucketName;
                                request.SourceKey = sourceObject.Key;
                                request.DestinationBucket = _bucket;
                                request.DestinationKey = _prefix + key;
                                if (_grantCannedAcl != null)
                                {
                                    request.CannedACL = _grantCannedAcl;
                                }
                                else if (_grant != null)
                                {
                                    request.Grants.Add(_grant);
                                }
                                if (_targetEncryptionMethod != null)
                                {
                                    request.ServerSideEncryptionMethod = _targetEncryptionMethod;
                                }
                                // return the CopyObjectRequest for the copy operation
                                return request;
                            },
                        BatchObjectNumber = objectNumber,
                        Key = key,
                        SourceObject = sourceObject,
                        TargetObject = targetObject,
                        S3 = _s3
                    };
                }
            }
            // else already synchronized
            else
            {
                Program.State.TrackObject(_prefix, targetObject, false, false);
            }
            // no processing was required, so return null (this should keep everything except for the actual copy operations out of the thread pool)
            return null;
        }
        /// <summary>
        /// Gets the last query that was made.
        /// </summary>
        public string LastQuery { get { return _lastQuery; } }

        /// <summary>
        /// Gets the bucket we are scanning.
        /// </summary>
        public string Bucket { get { return _bucket; } }
        /// <summary>
        /// Gets the prefix common to all objects that might appear in the window.
        /// </summary>
        public string Prefix { get { return _prefix; } }
        /// <summary>
        /// Gets the smallest (least) key for all the objects in all the batches in the queue.
        /// </summary>
        public string UnprefixedLeastKey { get { return _unprefixedLeastKey; } }
        /// <summary>
        /// Gets the largest (greatest) key for all the objects in all the batches in the queue.
        /// </summary>
        public string UnprefixedGreatestKey { get { return _unprefixedGreatestKey; } }
        /// <summary>
        /// Gets the number of batches queued.
        /// </summary>
        public long BatchesQueued { get { return _queue.Count; } }
        /// <summary>
        /// Gets the most recently dequeued batch id.
        /// </summary>
        public long MostRecentlyDequeuedBatchId { get { return _dequeuedBatchId; } }
        /// <summary>
        /// Gets the last queued batch id.
        /// </summary>
        public long LastQueuedBatchId { get { return _lastQueuedBatchId; } }
        /// <summary>
        /// Gets the total number of items queued.
        /// </summary>
        public long ItemsQueued { get { return _queue.Sum(q => q.Response.S3Objects.Count); } }
        /// <summary>
        /// Gets whether or not the last batch of objects has already been read.
        /// </summary>
        public bool LastBatchHasBeenRead { get { return _lastBatchHasBeenRead != 0; } }
        /// <summary>
        /// Gets the <see cref="S3Object"/> for the specified key (which is the object key without the prefix).
        /// </summary>
        /// <param name="key">The key to look for.</param>
        /// <returns>The <see cref="S3Object"/>, or <b>null</b> if no such object was found.</returns>
        private S3Object FindObject(string key)
        {
            foreach (Batch batch in _queue)
            {
                if (batch.DoesKeyBelong(key))
                {
                    return batch[key];
                }
            }
            return null;
        }
        /// <summary>
        /// An immutable object that holds the response objects for one query.
        /// </summary>
        internal class Batch
        {
            private readonly int _batchId;
            private readonly ListObjectsV2Response _response;
            private readonly Dictionary<string, S3Object> _objects;
            private readonly string _leastKey;
            private readonly string _greatestKey;
            private readonly string _continuationToken;
            private readonly string _nextContinuationToken;

            public Batch(string prefix, int batchId, string stopBeforeKey, ListObjectsV2Response response)
            {
                _batchId = batchId;
                _response = response;
                _objects = new Dictionary<string, S3Object>();
                _leastKey = response.S3Objects.Count > 0 ? "\uffff" : string.Empty;
                _greatestKey = string.Empty;
                _continuationToken = response.ContinuationToken;
                _nextContinuationToken = response.NextContinuationToken;
                // hash all the objects and keep track of the least and greatest keys
                foreach (S3Object o in response.S3Objects)
                {
                    System.Diagnostics.Debug.Assert(o.Key.StartsWith(prefix));
                    // if a stop key was specified, was this object's key past the end we care about?  stop here
                    if (!string.IsNullOrEmpty(stopBeforeKey) && String.CompareOrdinal(o.Key, stopBeforeKey) >= 0)
                    {
                        // treat this as if we hit the end because we're at the end of the range specified by the caller
                        response.IsTruncated = false;
                        break;
                    }
                    // the key is the S3 key with the specified prefix removed (in case we're copying from one prefix to another)
                    string key = o.Key.Substring(prefix.Length);
                    _objects.Add(key, o);
                    if (_leastKey == null || String.CompareOrdinal(key, _leastKey) < 0) _leastKey = key;
                    if (_greatestKey == null || String.CompareOrdinal(key, _greatestKey) > 0) _greatestKey = key;
                }
                // are we at the end of everything?
                if (!response.IsTruncated)
                {
                    // set the greatest key to the maximum possible value so we know that everything has been read
                    _greatestKey = "\uffff";
                }
            }
            /// <summary>
            /// Gets the <see cref="ListObjectsV2Response"/> the objects in this batch came from.
            /// </summary>
            public ListObjectsV2Response Response { get { return _response; } }
            /// <summary>
            /// Gets the batch ID for this batch.
            /// </summary>
            public int BatchId { get { return _batchId; } }
            /// <summary>
            /// Gets the smallest (least) key for all the objects in this batch.
            /// </summary>
            public string LeastKey { get { return _leastKey; } }
            /// <summary>
            /// Gets the largest (greatest) key for all the objects in this batch.
            /// </summary>
            public string GreatestKey { get { return _greatestKey; } }
            /// <summary>
            /// Checks to see if the specified key belongs to this batch or not.
            /// </summary>
            /// <param name="key">The key in question.</param>
            /// <returns><b>true</b> if the specified key should be in this batch, or <b>false</b> if the specified key falls outside the bounds of this batch.</returns>
            public bool DoesKeyBelong(string key)
            {
                return String.CompareOrdinal(key, _leastKey) >= 0 && String.CompareOrdinal(key, _greatestKey) <= 0;
            }
            /// <summary>
            /// Gets the <see cref="S3Object"/> for the specified key (which is the object key without the prefix).
            /// </summary>
            /// <param name="key">The key to look for.</param>
            /// <returns>The <see cref="S3Object"/>, or <b>null</b> if no such object was found.</returns>
            public S3Object this[string key]
            {
                get
                {
                    S3Object o;
                    if (_objects.TryGetValue(key, out o)) return o;
                    return null;
                }
            }
        }
        public static string GetBucketRegion(string bucketName)
        {
            try
            {
                AmazonS3Client s3 = new AmazonS3Client(Amazon.RegionEndpoint.USEast1);
                GetBucketLocationResponse response = s3.GetBucketLocation(new GetBucketLocationRequest { BucketName = bucketName });
                return string.IsNullOrEmpty(response.Location.Value) ? Amazon.RegionEndpoint.USEast1.SystemName : response.Location.Value;
            }
            catch (Exception ex)
            {
                throw new ApplicationException("Unable to query region for bucket '" + bucketName + "'", ex);
            }
        }
        /// <summary>
        /// Separates the region, bucket, and prefix from the combined string.
        /// </summary>
        /// <param name="regionBucketAndPrefix">A string with the optional region, bucket, and prefix, with a colon as the region/bucket delimiter and slash as the bucket/prefix delimiter.</param>
        /// <returns>A 3-tuple with the region, bucket, and prefix separated out.</returns>
        public static Tuple<string, string, string> ParseRegionBucketAndPrefix(string regionBucketAndPrefix)
        {
            int slashOffset = regionBucketAndPrefix.IndexOf('/');
            string regionAndBucket;
            string prefix;
            if (slashOffset < 0)
            {
                regionAndBucket = regionBucketAndPrefix;
                prefix = string.Empty;
            }
            else
            {
                regionAndBucket = regionBucketAndPrefix.Substring(0, slashOffset);
                prefix = regionBucketAndPrefix.Substring(slashOffset + 1);
            }
            Tuple<string, string> parsedRegionAndBucket = ParseRegionAndBucket(regionAndBucket);
            return new Tuple<string, string, string>(parsedRegionAndBucket.Item1, parsedRegionAndBucket.Item2, prefix);
        }
        public static Tuple<string, string> ParseRegionAndBucket(string regionAndBucket)
        {
            int colonOffset = regionAndBucket.IndexOf(':');
            if (colonOffset < 0) return new Tuple<string, string>(string.Empty, regionAndBucket);
            return new Tuple<string, string>(regionAndBucket.Substring(0, colonOffset), regionAndBucket.Substring(colonOffset + 1));
        }
        public static Tuple<string, string> ParseBucketAndPrefix(string regionAndBucket)
        {
            int slashOffset = regionAndBucket.IndexOf('/');
            if (slashOffset < 0) return new Tuple<string, string>(regionAndBucket, string.Empty);
            return new Tuple<string, string>(regionAndBucket.Substring(0, slashOffset), regionAndBucket.Substring(slashOffset + 1));
        }
    }
}
