using Amazon;
using System.Configuration;
using System;
namespace AspectKinesisLamda.AwsTests
{
    internal static class AmazonLambdaClientSettings
    {
        public static readonly string AwsAccessKeyId;
        public static readonly string AwsSecretAccessKey;
        public static readonly string AwsRegionDisplayName;
        public static readonly RegionEndpoint AwsRegionEndpoint;
        public static readonly string SendNotificationLambdaFunctionName;

        static AmazonLambdaClientSettings()
        {
            AwsAccessKeyId = "AKIAJKRQEEHQHX24KEYA";//ConfigurationManager.AppSettings["AmazonEnvironmentAccessKeyId"];
            AwsSecretAccessKey = "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";//ConfigurationManager.AppSettings["AmazonEnvironmentSecretAccessKey"];
            string awsRegionString = "us-east-1";//ConfigurationManager.AppSettings["AmazonEnvironmentRegion"];
            if (!String.IsNullOrEmpty(awsRegionString))
            {
                AwsRegionEndpoint = RegionEndpoint.GetBySystemName(awsRegionString);
                if (AwsRegionEndpoint != null)
                {
                    AwsRegionDisplayName = AwsRegionEndpoint.DisplayName;
                }
            }
            SendNotificationLambdaFunctionName = "AspectConnectAgentEventsForWFM";//ConfigurationManager.AppSettings["SendNotificationLambdaFunctionName"];
        }
    }
}