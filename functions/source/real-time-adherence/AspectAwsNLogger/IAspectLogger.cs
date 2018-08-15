using System;
using System.Collections.Generic;
using System.Text;

namespace AspectAwsNLogger
{
    public interface IAspectLogger
    {
        void Fatal(string message);
        void Error(string message);
        void Warn(string message);
        void Info(string message);
        void Debug(string message);
        void Trace(string message);
    }
}
