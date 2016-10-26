using System;
using System.Collections.Generic;
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
    /// <summary>
    /// A class that tracks batch ids.
    /// </summary>
    [Serializable]
    public class BatchIdCounter
    {
        private int _nextBatchId;

        public BatchIdCounter(int initialValue = 0)
        {
            _nextBatchId = initialValue;
        }
        /// <summary>
        /// Gets the next batch id.
        /// </summary>
        public int Next { get { return Interlocked.Increment(ref _nextBatchId); } }
    }
}
