using System;
using System.Collections;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using AspectAwsLambdaLogger;

namespace AspectKinesisLamda
{
    public class AwsSqsFacade : IAwsSqsFacade
    {
        private const string NUM_SQS_QUEUES_ENVIRONMENT_VARIABLE_LOOKUP = "NumSqsQueues";
        private const string SQS_QUEUE_URL_ENVIRONMENT_VARIABLE_LOOKUP = "SqsQueueUrl";
        private const string SQS_QUEUE_MESSAGE_GROUP_ID_ENVIRONMENT_VARIABLE_LOOKUP = "SqsQueueMessageGroupId";

        private readonly AmazonSQSClient _sqsClient;
        private readonly string _sqsQueueMessageGroupId;
        private readonly int _numSqsQueues;
        private readonly string[] _sqsQueueUrls;
        private readonly IAspectLogger _logger;

        private string ParseQueue(int queueNum)
        {
            var queueEnvVar = $"{SQS_QUEUE_URL_ENVIRONMENT_VARIABLE_LOOKUP}{queueNum}";
            var queueUrl = Environment.GetEnvironmentVariable(queueEnvVar);
            if (string.IsNullOrEmpty(queueUrl))
            {
                throw new Exception($"Missing Lambda Variable {queueEnvVar}");
            }
            return queueUrl.Trim();
        }

        public AwsSqsFacade(IAspectLogger logger)
        {
            _logger = logger;
            _sqsClient = new AmazonSQSClient();
            var numQueuesStr = Environment.GetEnvironmentVariable(NUM_SQS_QUEUES_ENVIRONMENT_VARIABLE_LOOKUP);
            if (string.IsNullOrEmpty(numQueuesStr))
            {
                throw new Exception($"Missing Lambda Variable {NUM_SQS_QUEUES_ENVIRONMENT_VARIABLE_LOOKUP}");
            }
            if (!int.TryParse(numQueuesStr, out _numSqsQueues) || _numSqsQueues < 1)
            {
                throw new Exception($"Invalid Lambda Variable {NUM_SQS_QUEUES_ENVIRONMENT_VARIABLE_LOOKUP}: value must be a positive integer");
            }

            _sqsQueueUrls = new string[_numSqsQueues];

            for (int i = 0; i < _numSqsQueues; ++i)
            {
                _sqsQueueUrls[i] = ParseQueue(i+1);
                _logger.Debug($"SqsQueueUrl{i+1}: {_sqsQueueUrls[i]}");
            }

            _sqsQueueMessageGroupId = Environment.GetEnvironmentVariable(SQS_QUEUE_MESSAGE_GROUP_ID_ENVIRONMENT_VARIABLE_LOOKUP);
            if (string.IsNullOrEmpty(_sqsQueueMessageGroupId))
            {
                throw new Exception($"Missing Lambda Variable {SQS_QUEUE_MESSAGE_GROUP_ID_ENVIRONMENT_VARIABLE_LOOKUP}");
            }
            _sqsQueueMessageGroupId = _sqsQueueMessageGroupId.Trim();
            _logger.Debug($"SqsQueueMessagGroupId: {_sqsQueueMessageGroupId}");
                       
        }

        private string AgentArnToSqsQueueUrl(string agentArn)
        {
            _logger.Trace("Beginning AgentArnToSqsQueueUrl");
            try
            {
                if (_numSqsQueues == 1)
                    return _sqsQueueUrls[0];

                using (var algorithm = SHA256.Create())
                {
                    var hash = algorithm.ComputeHash(Encoding.UTF8.GetBytes(agentArn));
                    var i = BitConverter.ToInt64(hash) % _numSqsQueues;
                    if (i < 0)
                        i += _numSqsQueues;
                    _logger.Debug($"AgentARN: {agentArn} SQS Queue index: {i}");
                    return _sqsQueueUrls[i];
                }
            }
            finally
            {
                _logger.Trace("Ending AgentArnToSqsQueueUrl");
            }
        }

        public async Task SendMessageToQueue(string recordData, string agentArn)
        {
            _logger.Trace("Beginning SendMessageToQueue");

            SendMessageRequest sendMessageRequest = new SendMessageRequest
            {
                QueueUrl = AgentArnToSqsQueueUrl(agentArn),
                MessageBody = recordData,
                MessageGroupId = _sqsQueueMessageGroupId   
            };//NOTE:  Enable ContentBasedDeduplication for particular SQSSqueue since not sending MessageDeduplicationId which could use recordData's Event Id

            var sendMessageResponse  = await _sqsClient.SendMessageAsync(sendMessageRequest);

            _logger.Debug($"Message sent to queue {sendMessageResponse.MessageId}");

            _logger.Trace("Ending SendMessageToQueue");
        }
    }
}