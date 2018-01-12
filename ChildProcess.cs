using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace S3BucketSync
{
    /// <summary>
    /// A class that abstracts running of a child process and passing strings back and forth through stdin, stdout, and stderr.
    /// </summary>
    public class ChildProcess
    {
        private bool _errorsWritten = false;
        private bool _abort;
        private long _lastMessageReceiveTime;   // interlocked
        private StringBuilder _outBuffer = new StringBuilder();
        private StringBuilder _errBuffer = new StringBuilder();
        private System.Diagnostics.ProcessPriorityClass _priority;
        private int _timeoutMilliseconds;
        private string _filename;
        private string _arguments;
        private int _returnCode = Int32.MinValue;
        private System.Diagnostics.Process _process;

        public class MessageReceivedEventArgs : EventArgs
        {
            public string MessageType { get; set; }
            public string MessageValue { get; set; }
        }

        public delegate void MessageReceivedEventHandler(object sender, MessageReceivedEventArgs args);

        public event MessageReceivedEventHandler OutputReceived;
        public event MessageReceivedEventHandler ErrorReceived;

        private static string EncodeMessageString(string decoded)
        {
            return decoded.Replace("\\", "\\\\").Replace(":", "\\~").Replace("\n", "\\\n").Replace("\r", "\\\r");
        }
        private static string DecodeMessageString(string encoded)
        {
            return encoded.Replace("\\\r", "\r").Replace("\\\n", "\n").Replace("\\~", ":").Replace("\\\\", "\\");
        }
        public void SendMessage(string messageType, string value)
        {
            _process.StandardInput.WriteLine(EncodeMessageString(messageType) + ":" + EncodeMessageString(value));
        }

        public System.Diagnostics.ProcessPriorityClass Priority { get { return _priority; } set { _priority = value; } }
        public int TimeoutMilliseconds { get { return _timeoutMilliseconds; } set { _timeoutMilliseconds = value; } }
        public string Filename { get { return _filename; } set { _filename = value; } }
        public string Arguments { get { return _arguments; } set { _arguments = value; } }
        public int ExitCode { get { return _returnCode; } }
        public bool Abort {  get { return _abort; } set { _abort = value; } }
        /// <summary>
        /// Gets whether or not errors were written from this process run.
        /// </summary>
        public bool ErrorsWritten
        {
            get { return _errorsWritten; }
        }
        /// <summary>
        /// Asynchronously starts the child process.
        /// </summary>
        /// <returns>The return code returned from the process.</returns>
        public void Start()
        {
            // Initialize the process and its StartInfo properties.
            using (System.Diagnostics.Process process = new System.Diagnostics.Process())
            {
                _process = process;
                process.StartInfo.FileName = _filename;
                process.StartInfo.Arguments = _arguments;
                // Set UseShellExecute to false for redirection.
                process.StartInfo.UseShellExecute = false;
                process.StartInfo.CreateNoWindow = true;
                process.StartInfo.LoadUserProfile = false;

                // redirect the standard input of the command
                process.StartInfo.RedirectStandardInput = true;

                // Redirect the standard output of the command.  
                // This stream is read asynchronously using an event handler.
                process.StartInfo.RedirectStandardOutput = true;
                process.OutputDataReceived += new System.Diagnostics.DataReceivedEventHandler(NetOutputDataHandler);

                // Redirect the error output of the command. 
                process.StartInfo.RedirectStandardError = true;
                process.ErrorDataReceived += new System.Diagnostics.DataReceivedEventHandler(NetErrorDataHandler);

                // mark the last message receive time
                _lastMessageReceiveTime = System.DateTime.UtcNow.Ticks;

                WorkerPool.Default.RunAsync(() =>
                    {
                        bool stop = false;
                        while (!stop && !_abort)
                            try
                            {
                                // Start the process.
                                process.Start();

                                bool outputConnectionFailed = false;
                                bool errorConnectionFailed = false;
                                try
                                {
                                    // Start the asynchronous read of the standard output stream.
                                    process.BeginOutputReadLine();
                                }
                                catch (Exception e)
                                {
                                    // ignore all exceptions here--it may be that the process just exited too fast to perform these operations
                                    System.Diagnostics.Debug.WriteLine("Error starting process output read: " + e.ToString());
                                    outputConnectionFailed = true;
                                }
                                try
                                {
                                    // Start the asynchronous read of the standard error stream.
                                    process.BeginErrorReadLine();
                                }
                                catch (Exception e)
                                {
                                    // ignore all exceptions here--it may be that the process just exited too fast to perform these operations
                                    System.Diagnostics.Debug.WriteLine("Error starting process error read: " + e.ToString());
                                    errorConnectionFailed = true;
                                }

                                try
                                {
                                    // set the process priority so it doesn't consume all the foreground CPU!
                                    process.PriorityClass = _priority;
                                }
                                catch (Exception e)
                                {
                                    // ignore all exceptions here--it may be that the process just exited too fast to perform these operations
                                    System.Diagnostics.Debug.WriteLine("Error changing process priority: " + e.ToString());
                                }
                                // output connection failed?
                                if (outputConnectionFailed)
                                {
                                    // try to read synchronously
                                    try
                                    {
                                        string output = process.StandardOutput.ReadToEnd();
                                        OutputDataHandler(output);
                                    }
                                    catch
                                    {
                                        // ignore errors because this shouldn't work anyway, but might in rare cases where the process was so fast that it finished before we could begin doing async reads
                                    }
                                }
                                // error connection failed?
                                if (errorConnectionFailed)
                                {
                                    // try to read synchronously
                                    try
                                    {
                                        string output = process.StandardError.ReadToEnd();
                                        ErrorDataHandler(output);
                                    }
                                    catch
                                    {
                                        // ignore errors because this shouldn't work anyway, but might in rare cases where the process was so fast that it finished before we could begin doing async reads
                                    }
                                }
                                // wait until the timeout elapses, make sure we fully process async output if we exited properly
                                if (process.WaitForExit(_timeoutMilliseconds))
                                {
                                    process.WaitForExit();
                                }
                                else // timed out
                                {
                                    // kill the process
                                    process.Kill();
                                    // return the timeout code
                                    _returnCode = System.Int32.MinValue;
                                    return;
                                }
                                // wait until it's been at least 100 ms since the last message was received
                                while ((System.DateTime.UtcNow - new DateTime(_lastMessageReceiveTime)).TotalMilliseconds < 100)
                                {
                                    System.Threading.Thread.Sleep(100);
                                }
                                // get the process exit code
                                _returnCode = process.ExitCode;
                            }
                            finally
                            {
                                // close the process
                                process.Close();
                            }
                        // cancel the reads now
                        try { process.CancelErrorRead(); } catch { }
                        try { process.CancelOutputRead(); } catch { }
                    });
            }
        }


        private void NetOutputDataHandler(object sendingProcess, System.Diagnostics.DataReceivedEventArgs outLine)
        {
            if (!String.IsNullOrEmpty(outLine.Data))
            {
                OutputDataHandler(outLine.Data);
            }
            System.Threading.Interlocked.Exchange (ref _lastMessageReceiveTime, System.DateTime.UtcNow.Ticks);
        }

        private void OutputDataHandler(string data)
        {
            System.Diagnostics.Debug.WriteLine(data);
            _outBuffer.Append(data);
            string buffer = _outBuffer.ToString();
            int prevLineBreak = 0;
            int lineBreak = 0;
            // loop until we hit the last chunk (no more line breaks)
            while ((lineBreak = buffer.IndexOfAny(new char[] { '\r', '\n' }, prevLineBreak)) >= 0)
            {
                string message = buffer.Substring(prevLineBreak, lineBreak - prevLineBreak);
                string[] messageParts = message.Split(':');
                if (messageParts.Length > 2) throw new FormatException("Output received from child process did not conform to message passing standard!");
                string type = (messageParts.Length < 1) ? string.Empty : messageParts[0];
                string value = (messageParts.Length < 1) ? messageParts[0] : messageParts[1];
                MessageReceivedEventArgs messageReceived = new MessageReceivedEventArgs { MessageType = DecodeMessageString(type), MessageValue = DecodeMessageString(value) };
                OutputReceived(this, messageReceived);
                prevLineBreak = lineBreak;
            }
            if (prevLineBreak >= 0)
            {
                _outBuffer.Remove(0, prevLineBreak + 1);
            }
        }

        private void NetErrorDataHandler(object sendingProcess, System.Diagnostics.DataReceivedEventArgs errLine)
        {
            System.Diagnostics.Debug.WriteLine(errLine.Data);
            if (!String.IsNullOrEmpty(errLine.Data))
            {
                ErrorDataHandler(errLine.Data);
            }
            System.Threading.Interlocked.Exchange(ref _lastMessageReceiveTime, System.DateTime.UtcNow.Ticks);
        }

        private void ErrorDataHandler(string data)
        {
            _errorsWritten = true;
            System.Diagnostics.Debug.WriteLine(data);
            _errBuffer.Append(data);
            string buffer = _errBuffer.ToString();
            int prevLineBreak = 0;
            int lineBreak = 0;
            // loop until we hit the last chunk (no more line breaks)
            while ((lineBreak = buffer.IndexOfAny(new char[] { '\r', '\n' }, prevLineBreak)) >= 0)
            {
                string message = buffer.Substring(prevLineBreak, lineBreak - prevLineBreak);
                string[] messageParts = message.Split(':');
                if (messageParts.Length > 2) throw new FormatException("Error received from child process did not conform to message passing standard!");
                string type = (messageParts.Length < 1) ? string.Empty : messageParts[0];
                string value = (messageParts.Length < 1) ? messageParts[0] : messageParts[1];
                MessageReceivedEventArgs messageReceived = new MessageReceivedEventArgs { MessageType = DecodeMessageString(type), MessageValue = DecodeMessageString(value) };
                ErrorReceived(this, messageReceived);
                prevLineBreak = lineBreak;
            }
            if (prevLineBreak >= 0)
            {
                _errBuffer.Remove(0, prevLineBreak + 1);
            }
        }
    }
}