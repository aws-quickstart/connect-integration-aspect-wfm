using System;
using System.Collections.Generic;
using System.Text;
using Amazon.Lambda.Core;
using NLog;

namespace AspectKinesisLamda.Tests
{
    public class FakeLambdaLogger :ILambdaLogger
    {
        private static readonly Logger Logger = LogManager.GetLogger("testLogger");

        public void Log(string message)
        {
            Logger.Info(message);
        }
        public void LogLine(string message)
        {
            Logger.Info(message);
        }
    }
}
