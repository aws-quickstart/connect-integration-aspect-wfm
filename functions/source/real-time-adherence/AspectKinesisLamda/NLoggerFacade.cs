using System;
using NLog;
using NLog.AWS.Logger;
using NLog.Config;

namespace AspectKinesisLamda
{
    public class NLoggerFacade:ILoggerFacade
    {
        private const string LOG_LEVEL_ENVIRONMENT_VARIABLE_LOOKUP = "LogLevel";
        private readonly Logger _logger;

        public NLoggerFacade()
        {
            ConfigureNLog();
            _logger = LogManager.GetCurrentClassLogger(); 
        }

        public void Fatal(string message)
        {
            _logger.Fatal(message);
        }
        public void Error(string message)
        {
            _logger.Error(message);
        }
        public void Warn(string message)
        {
            _logger.Warn(message);
        }
        public void Info(string message)
        {
            _logger.Info(message);
        }
        public void Debug(string message)
        {
            _logger.Debug(message);
        }      
        public void Trace(string message)
        {
            _logger.Trace(message);
        }

        private void ConfigureNLog()
        {
            LogLevel logLevel = GetLogLevel();
            var config = new LoggingConfiguration();
            var awsTarget = new AWSTarget()
            {
                LogGroup = "/aws/lambda/APSKinesisLamdaFunction",
                Region = "us-east-1"
            };
            config.AddTarget("aws", awsTarget);
            config.LoggingRules.Add(new LoggingRule("*", logLevel, awsTarget));

            LogManager.Configuration = config;
        }
        private LogLevel GetLogLevel()
        {
            LogLevel logLevel = LogLevel.Info;
            string logLevelVariable = Environment.GetEnvironmentVariable(LOG_LEVEL_ENVIRONMENT_VARIABLE_LOOKUP);
            if (!String.IsNullOrEmpty(logLevelVariable))
            {
                try
                {
                    logLevel = LogLevel.FromString(logLevelVariable);
                }
                catch (Exception)
                {
                    //Uses default above
                }
            }
            return logLevel;
        }
    }
}
