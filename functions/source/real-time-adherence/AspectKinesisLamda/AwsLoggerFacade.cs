using Amazon.Lambda.Core;
using System;

namespace AspectKinesisLamda
{
    enum AwsLogLevel
    {
        Off,
        Fatal,
        Error,
        Warn,
        Info,
        Debug,
        Trace
    }

    public class AwsLoggerFacade:ILoggerFacade
    {
        private const string LOG_LEVEL_ENVIRONMENT_VARIABLE_LOOKUP = "LogLevel";
        private readonly ILambdaLogger _logger;
        private readonly AwsLogLevel _logLevel;

        public AwsLoggerFacade(ILambdaLogger logger)
        {
            _logger = logger;
            _logLevel = GetLogLevel();
        }

        public void Fatal(string message)
        {
            if
                (_logLevel >= AwsLogLevel.Fatal)
            {
                _logger.LogLine($"Fatal: {message}");
            }
        }
        public void Error(string message)
        {
            if (_logLevel >= AwsLogLevel.Error)
            {
                _logger.LogLine($"Error: {message}");
            }
        }
        public void Warn(string message)
        {
            if (_logLevel >= AwsLogLevel.Warn)
            {
                _logger.LogLine($"Warn: {message}");
            }          
        }
        public void Info(string message)
        {
            if (_logLevel >= AwsLogLevel.Info)
            {
                _logger.LogLine($"Info: {message}");
            }
        }
        public void Debug(string message)
        {
            if (_logLevel >= AwsLogLevel.Debug)
            {
                _logger.LogLine($"Debug: {message}");
            }
        }      
        public void Trace(string message)
        {
            if (_logLevel >= AwsLogLevel.Trace)
            {
                _logger.LogLine($"Trace: {message}");
            }
        }

        private AwsLogLevel GetLogLevel()
        {
            AwsLogLevel logLevel = AwsLogLevel.Info;
            string logLevelVariable = Environment.GetEnvironmentVariable(LOG_LEVEL_ENVIRONMENT_VARIABLE_LOOKUP);
            if (!String.IsNullOrEmpty(logLevelVariable))
            {
                Enum.TryParse(logLevelVariable, true, out logLevel);
            }//NOTE: If not valid level will use Off since 1st option enum
            return logLevel;
        }
    }
}
