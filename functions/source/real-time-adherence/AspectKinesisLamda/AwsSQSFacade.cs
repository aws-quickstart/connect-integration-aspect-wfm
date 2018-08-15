using System;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using AspectAwsLambdaLogger;

namespace AspectKinesisLamda
{
    public class AwsSqsFacade : IAwsSqsFacade
    {
        private const string SQS_QUEUE_NAME_ENVIRONMENT_VARIABLE_LOOKUP = "SqsQueueName";
        private const string SQS_QUEUE_URL_ENVIRONMENT_VARIABLE_LOOKUP = "SqsQueueUrl";
        private const string SQS_QUEUE_OWNER_ENVIRONMENT_VARIABLE_LOOKUP = "SqsQueueOwner";
        private const string SQS_QUEUE_MESSAGE_GROUP_ID_ENVIRONMENT_VARIABLE_LOOKUP = "SqsQueueMessageGroupId";

        private readonly AmazonSQSClient _sqsClient;
        private readonly string _sqsQueueMessageGroupId;
        private readonly string _sqsQueueUrl;
        private readonly IAspectLogger _logger;

        public AwsSqsFacade(IAspectLogger logger)
        {
            _logger = logger;
            _sqsClient = new AmazonSQSClient();

            _sqsQueueUrl = Environment.GetEnvironmentVariable(SQS_QUEUE_URL_ENVIRONMENT_VARIABLE_LOOKUP);
            var sqsQueueName = Environment.GetEnvironmentVariable(SQS_QUEUE_NAME_ENVIRONMENT_VARIABLE_LOOKUP);
            var sqsQueueOwner = Environment.GetEnvironmentVariable(SQS_QUEUE_OWNER_ENVIRONMENT_VARIABLE_LOOKUP);

            if (!string.IsNullOrEmpty(_sqsQueueUrl))
            {
                _sqsQueueUrl = _sqsQueueUrl.Trim();
            }
            if (string.IsNullOrEmpty(_sqsQueueUrl))
            {
                if (string.IsNullOrEmpty(sqsQueueName))
                {
                    throw new Exception($"Missing Lambda Variable {SQS_QUEUE_NAME_ENVIRONMENT_VARIABLE_LOOKUP} or {SQS_QUEUE_URL_ENVIRONMENT_VARIABLE_LOOKUP}");
                }
                sqsQueueName = sqsQueueName.Trim();
                _logger.Debug($"SqsQueueName: {sqsQueueName}");
                if (string.IsNullOrEmpty(sqsQueueOwner))
                {
                    throw new Exception($"Missing Lambda Variable {SQS_QUEUE_OWNER_ENVIRONMENT_VARIABLE_LOOKUP} or {SQS_QUEUE_URL_ENVIRONMENT_VARIABLE_LOOKUP}");
                }
                sqsQueueOwner = sqsQueueOwner.Trim();
                _logger.Debug($"SqsQueueOwner: {sqsQueueOwner}");

                var request = new GetQueueUrlRequest
                {
                    QueueName = sqsQueueName,
                    QueueOwnerAWSAccountId = sqsQueueOwner
                }; //NOTE: Throws error if does not exist

                var response = _sqsClient.GetQueueUrlAsync(request).Result;
                _sqsQueueUrl = response.QueueUrl;
            }
            _logger.Debug($"SqsQueueUrl: {_sqsQueueUrl}");

            _sqsQueueMessageGroupId = Environment.GetEnvironmentVariable(SQS_QUEUE_MESSAGE_GROUP_ID_ENVIRONMENT_VARIABLE_LOOKUP);
            if (string.IsNullOrEmpty(_sqsQueueMessageGroupId))
            {
                throw new Exception($"Missing Lambda Variable {SQS_QUEUE_MESSAGE_GROUP_ID_ENVIRONMENT_VARIABLE_LOOKUP}");
            }
            _sqsQueueMessageGroupId = _sqsQueueMessageGroupId.Trim();
            _logger.Debug($"SqsQueueMessagGroupId: {_sqsQueueMessageGroupId}");
                       
        }

        public async Task SendMessageToQueue(string recordData)
        {
            _logger.Trace("Beginning SendMessageToQueue");

            SendMessageRequest sendMessageRequest = new SendMessageRequest
            {
                QueueUrl = _sqsQueueUrl,
                MessageBody = recordData,
                MessageGroupId = _sqsQueueMessageGroupId   
            };//NOTE:  Enable ContentBasedDeduplication for particular SQSSqueue since not sending MessageDeduplicationId which could use recordData's Event Id

            var sendMessageResponse  = await _sqsClient.SendMessageAsync(sendMessageRequest);

            _logger.Debug($"Message sent to queue {sendMessageResponse.MessageId}");

            _logger.Trace("Ending SendMessageToQueue");
        }
    }
}