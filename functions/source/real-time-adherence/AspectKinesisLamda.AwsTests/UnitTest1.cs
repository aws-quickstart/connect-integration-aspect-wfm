using System;
using Xunit;
using Amazon.Lambda;
using Amazon.Lambda.Model;

namespace AspectKinesisLamda.AwsTests
{
    public class UnitTest1
    {
        [Fact]
        public async void TestNewAgentLogIn()
        {
            string payLoad = "{\r\n    \"Records\": [\r\n      {\r\n        \"eventID\": \"shardId-000000000000:49545115243490985018280067714973144582180062593244200961\",\r\n        \"eventVersion\": \"1.0\",\r\n        \"kinesis\": {\r\n          \"approximateArrivalTimestamp\": 1428537600,\r\n          \"partitionKey\": \"partitionKey-3\",\r\n          \"data\": \"eyJBV1NBY2NvdW50SWQiOiIyMzY2MzA3MTA2NjgiLCJBZ2VudEFSTiI6ImFybjphd3M6Y29ubmVjdDp1cy1lYXN0LTE6MjM2NjMwNzEwNjY4Omluc3RhbmNlLzZlMzIzMGU1LTJkZDItNDQ2NC1hZGU4LWIwMzJjZGQwYWJlZi9hZ2VudC9kZmU4OTcwYS1lZTc4LTRmYTItYjc5Yi1iNmIzZTkyZDM1YzAiLCJDdXJyZW50QWdlbnRTbmFwc2hvdCI6bnVsbCwiRXZlbnRJZCI6IjhkMmRhYTRmLWE2YjQtNGMzYy04NTk5LTA4YzM1NWEyMGY4MCIsIkV2ZW50VGltZXN0YW1wIjoiMjAxOC0wNS0xNlQxNjoxMjo1OS4wNTlaIiwiRXZlbnRUeXBlIjoiTE9HSU4iLCJJbnN0YW5jZUFSTiI6ImFybjphd3M6Y29ubmVjdDp1cy1lYXN0LTE6MjM2NjMwNzEwNjY4Omluc3RhbmNlLzZlMzIzMGU1LTJkZDItNDQ2NC1hZGU4LWIwMzJjZGQwYWJlZiIsIlByZXZpb3VzQWdlbnRTbmFwc2hvdCI6bnVsbCwiVmVyc2lvbiI6IjIwMTctMTAtMDEifQ==\",\r\n          \"kinesisSchemaVersion\": \"1.0\",\r\n          \"sequenceNumber\": \"49545115243490985018280067714973144582180062593244200961\"\r\n        },\r\n        \"invokeIdentityArn\": \"arn:aws:iam::EXAMPLE\",\r\n        \"eventName\": \"aws:kinesis:record\",\r\n        \"eventSourceARN\": \"arn:aws:kinesis:EXAMPLE\",\r\n        \"eventSource\": \"aws:kinesis\",\r\n        \"awsRegion\": \"us-east-1\"\r\n      }\r\n    ]\r\n  }";
            using (AmazonLambdaClient client = new AmazonLambdaClient(AmazonLambdaClientSettings.AwsAccessKeyId, AmazonLambdaClientSettings.AwsSecretAccessKey, AmazonLambdaClientSettings.AwsRegionEndpoint))
            {
                InvokeRequest ir = new InvokeRequest
                {
                    FunctionName = AmazonLambdaClientSettings.SendNotificationLambdaFunctionName,
                    InvocationType = InvocationType.Event,
                    Payload = payLoad
                };

                await client.InvokeAsync(ir);
            }
        }
    }
}
