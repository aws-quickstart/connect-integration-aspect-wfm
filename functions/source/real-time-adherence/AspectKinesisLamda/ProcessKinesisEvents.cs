using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.KinesisEvents;
using AspectAwsLambdaLogger;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace AspectKinesisLamda
{
    public class ProcessKinesisEvents
    {
        private const string HEARTBEAT_EVENTTYPE = "HEART_BEAT";
        private const string DYNAMODB_TABLE_NAME_ENVIRONMENT_VARIABLE_LOOKUP = "DynamoDbTableName";
        private const string WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP = "WriteEventsToQueue"; 
        private readonly IDynamoDBContext _ddbContext;
        private IAwsSqsFacade _sqsFacade;
        private IAspectLogger _logger;
        private readonly string _dynamoDbTableName;

        //NOTE: Used by Unit Tests
        public ProcessKinesisEvents(IDynamoDBContext ddbContext, IAwsSqsFacade sqsFacade)
        {
            _ddbContext = ddbContext;
            _sqsFacade = sqsFacade;
        }

        public ProcessKinesisEvents()
        {
            _dynamoDbTableName = Environment.GetEnvironmentVariable(DYNAMODB_TABLE_NAME_ENVIRONMENT_VARIABLE_LOOKUP);
            if (string.IsNullOrEmpty(_dynamoDbTableName))
            {
                throw new Exception($"Missing Lambda Variable {DYNAMODB_TABLE_NAME_ENVIRONMENT_VARIABLE_LOOKUP}");
            }
            _dynamoDbTableName = _dynamoDbTableName.Trim();
            AWSConfigsDynamoDB.Context.TypeMappings[typeof(ConnectKinesisEventRecord)] = new Amazon.Util.TypeMapping(typeof(ConnectKinesisEventRecord), _dynamoDbTableName);
            var config = new DynamoDBContextConfig { Conversion = DynamoDBEntryConversion.V2 };
            _ddbContext = new DynamoDBContext(new AmazonDynamoDBClient(), config);

        }

        public async Task AspectKinesisHandler(KinesisEvent kinesisEvent, ILambdaContext context)
        {
            if (_logger == null)
            {
                _logger = new AspectAwsLambdaLogger.AspectAwsLambdaLogger(context.Logger);  //NOTE: Needed for Test or if using CloudWatch and not using NLog->CloudWatch setup in constructor
            }
            if (_sqsFacade == null)
            {
                _sqsFacade = new AwsSqsFacade(_logger); //NOTE: Needed for Test or if using CloudWatch and not using NLog->CloudWatch setup in constructor
            }
            _logger.Debug($"DynamoDbTableName: {_dynamoDbTableName}");
            _logger.Trace("Beginning AspectKinesisHandler");

            _logger.Info($"Record Count: {kinesisEvent.Records.Count}");

            bool writeEventsToQueue = Convert.ToBoolean(Environment.GetEnvironmentVariable(WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP));
            _logger.Debug($"WriteEventsToQueue: {writeEventsToQueue}");

            foreach (var record in kinesisEvent.Records)
            {
                await ProcessEventRecord(record, writeEventsToQueue);
            }

            _logger.Trace("Ending AspectKinesisHandler");
        }

        private async Task ProcessEventRecord(KinesisEvent.KinesisEventRecord record, bool writeEventsToQueue)
        {
            _logger.Trace("Beginning ProcessEventRecord");

            _logger.Debug($"Kinesis EventId:{record.EventId} EventName: {record.EventName} EventSource: {record.EventSource} EventVersion: {record.EventVersion} EventSourceARN {record.EventSourceARN} InvokeIdentityARN: {record.InvokeIdentityArn} Kinesis Time: {record.Kinesis.ApproximateArrivalTimestamp} Kinesis ParitionKey: {record.Kinesis.PartitionKey} Kinesis SeqNum: {record.Kinesis.SequenceNumber}");

            string recordData = GetRecordContents(record.Kinesis);

            _logger.Debug($"RecordData:{recordData}");

            //FOR CREATING TEST DATA
            //var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(recordData);
            //string convertedRecordData = System.Convert.ToBase64String(plainTextBytes);
            //Logger.Info(convertedRecordData);

            if (!String.IsNullOrEmpty(recordData))
            {
                ConnectKinesisEventRecord streamRecord = ConvertRecordData(recordData);
                // if we were able to parse this event, send to DynamoDB and SQS.
                if (streamRecord != null)
                {
                    if (streamRecord.LastEventType != HEARTBEAT_EVENTTYPE)
                    {
                        await ProcessEventToTable(streamRecord);
                    }
                    if (writeEventsToQueue)
                    {
                        await ProcessEventToQueue(recordData);
                    }
                }
            }

            _logger.Trace("Ending ProcessEventRecord");
        }

        private string GetRecordContents(KinesisEvent.Record streamRecord)
        {
            _logger.Trace("Beginning GetRecordContents");

            string contents;
            using (var reader = new StreamReader(streamRecord.Data, Encoding.UTF8))
            {
                contents =  reader.ReadToEnd();
            }

            _logger.Trace("Ending GetRecordContents");

            return contents;         
        }

        private ConnectKinesisEventRecord ConvertRecordData(string recordData)
        {
            _logger.Trace("Beginning ConvertRecordData");

            EventRecordData recordDataObject;
            try
            {
                recordDataObject = JsonConvert.DeserializeObject<EventRecordData>(recordData);
            }
            catch (Exception e)
            {
                _logger.Error($"Exception deserializing recordData: {e}. Discarding recordData.");
                // This record doesn't match the EventRecordData format, return a value that
                // indicates that this record should be discarded.
                return null;
            }

            // if this record doesn't include an agent ARN, assume it doesn't match the
            // required format. Discard it.
            if (String.IsNullOrEmpty(recordDataObject.AgentARN))
            {
                _logger.Info("recordData does not include AgentARN. Discarding recordData.");
                return null;
            }
            
            ConnectKinesisEventRecord streamRecord = new ConnectKinesisEventRecord()
            {
                AgentARN = recordDataObject.AgentARN,
                AgentUsername = recordDataObject.CurrentAgentSnapshot?.Configuration?.Username,
                LastEventType = recordDataObject.EventType,
                LastEventTimeStamp = recordDataObject.EventTimestamp, 
                LastStateChangeTimeStamp = recordDataObject.CurrentAgentSnapshot?.AgentStatus.StartTimestamp,
                CurrentState = recordDataObject.CurrentAgentSnapshot?.AgentStatus?.Name,
                RawAgentEventJSon = recordData
            };

            _logger.Debug($"Event Type: {recordDataObject.EventType} Agent Username: {streamRecord.AgentUsername} Current State: {streamRecord.CurrentState} Event Time: {streamRecord.LastEventTimeStamp} State Change Time: {streamRecord.LastStateChangeTimeStamp}");

            _logger.Trace("Ending ConvertRecordData");

            return streamRecord;
        }

        private async Task ProcessEventToTable(ConnectKinesisEventRecord streamRecord)
        {
            _logger.Trace("Beginning ProcessEventsToDynamoTable");

            var dynamoRecord = await _ddbContext.LoadAsync<ConnectKinesisEventRecord>(streamRecord.AgentARN);
            if (dynamoRecord == null || streamRecord.LastEventTimeStamp >= dynamoRecord.LastEventTimeStamp)
            {
                await _ddbContext.SaveAsync(streamRecord); 
            }
            else 
            {
                _logger.Warn($"Stream record event timestamp {streamRecord.LastEventTimeStamp} is prior to dynamo record event timestamp {dynamoRecord.LastEventTimeStamp}.");
                _logger.Warn($"Current Stream Record Json {streamRecord.RawAgentEventJSon}");
                _logger.Warn($"Current Dynamo Record Json {dynamoRecord.RawAgentEventJSon}");              
            }
            //NOTE: ignores if record old/out-of-order since all info out-of-date; not excepted to be out of order - very rare possiblity

            _logger.Trace("Ending ProcessEventsToDynamoTable");
        }

        private async Task ProcessEventToQueue(string recordData)
        {
            _logger.Trace("Beginning ProcessEventToQueue");

            await _sqsFacade.SendMessageToQueue(recordData);

            _logger.Trace("Ending ProcessEventToQueue");
        }
    }
}