using System;
using System.Collections.Generic;
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
using Newtonsoft.Json.Linq;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace AspectKinesisLamda
{
    public class ProcessKinesisEvents
    {
        private const string HEARTBEAT_EVENTTYPE = "HEART_BEAT";
        private const string DYNAMODB_TABLE_NAME_ENVIRONMENT_VARIABLE_LOOKUP = "DynamoDbTableName";
        private const string CONFIG_TABLE_NAME_ENVIRONMENT_VARIABLE_LOOKUP = "ConfigTableName";
        private const string WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP = "WriteEventsToQueue";
        private const string AHG_FILTER_LEVEL_ENVIRONMENT_VARIABLE_LOOKUP = "AhgFilterLevel";
        private const string AHG_FILTER_SEPARATOR_ENVIRONMENT_VARIABLE_LOOKUP = "AhgFilterSeparator";
        private const string AHG_FILTER_VALUES_ENVIRONMENT_VARIABLE_LOOKUP = "AhgFilterValues";
        private readonly IDynamoDBContext _aeDbContext;
        private readonly IDynamoDBContext _cfgDbContext;
        private IAwsSqsFacade _sqsFacade;
        private IAspectLogger _logger;
        private readonly string _dynamoDbTableName;
        private readonly string _configTableName;
        private readonly int _ahgFilterLevel;
        private readonly char _ahgFilterSeparator;
        private readonly SortedSet<string> _ahgFilterValues;

        private const int MIN_AHG_FILTER_LEVEL = 0;
        private const int MAX_AHG_FILTER_LEVEL = 5;
        private const int AHG_FILTER_LEVEL_DISABLED = 0;

        //NOTE: Used by Unit Tests
        public ProcessKinesisEvents(IDynamoDBContext aeDbContext, IDynamoDBContext cfgDbContext, IAwsSqsFacade sqsFacade)
        {
            _aeDbContext = aeDbContext;
            _cfgDbContext = cfgDbContext;
            _sqsFacade = sqsFacade;
            _ahgFilterLevel = 0;
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
            var aeConfig = new DynamoDBContextConfig { Conversion = DynamoDBEntryConversion.V2 };
            _aeDbContext = new DynamoDBContext(new AmazonDynamoDBClient(), aeConfig);
            _configTableName = Environment.GetEnvironmentVariable(CONFIG_TABLE_NAME_ENVIRONMENT_VARIABLE_LOOKUP);
            if (!string.IsNullOrEmpty(_configTableName))
            {
                _configTableName = _configTableName.Trim();
                AWSConfigsDynamoDB.Context.TypeMappings[typeof(ConfigRecord)] = new Amazon.Util.TypeMapping(typeof(ConfigRecord), _configTableName);
                var cfgConfig = new DynamoDBContextConfig { Conversion = DynamoDBEntryConversion.V2 };
                _cfgDbContext = new DynamoDBContext(new AmazonDynamoDBClient(), cfgConfig);
            }
            else
                _cfgDbContext = null;
            var level = Environment.GetEnvironmentVariable(AHG_FILTER_LEVEL_ENVIRONMENT_VARIABLE_LOOKUP);
            if (string.IsNullOrEmpty(level))
            {
                throw new Exception($"Missing Lambda Variable {AHG_FILTER_LEVEL_ENVIRONMENT_VARIABLE_LOOKUP}");
            }
            if (!int.TryParse(level, out _ahgFilterLevel) || _ahgFilterLevel < MIN_AHG_FILTER_LEVEL || _ahgFilterLevel > MAX_AHG_FILTER_LEVEL)
            {
                throw new Exception($"Invalid Lambda Variable {AHG_FILTER_LEVEL_ENVIRONMENT_VARIABLE_LOOKUP}: value must be an integer between {MIN_AHG_FILTER_LEVEL} and {MAX_AHG_FILTER_LEVEL}");
            }
            if (_ahgFilterLevel != AHG_FILTER_LEVEL_DISABLED)
            {
                var sep = Environment.GetEnvironmentVariable(AHG_FILTER_SEPARATOR_ENVIRONMENT_VARIABLE_LOOKUP);
                if (string.IsNullOrEmpty(sep))
                {
                    throw new Exception($"Missing Lambda Variable {AHG_FILTER_SEPARATOR_ENVIRONMENT_VARIABLE_LOOKUP}");
                }
                if (sep.Length != 1)
                {
                    throw new Exception($"Invalid Lambda Variable {AHG_FILTER_SEPARATOR_ENVIRONMENT_VARIABLE_LOOKUP}: value must be a single character");
                }
                _ahgFilterSeparator = sep[0];
                var values = Environment.GetEnvironmentVariable(AHG_FILTER_VALUES_ENVIRONMENT_VARIABLE_LOOKUP);
                _ahgFilterValues = new SortedSet<string>(values.Split(_ahgFilterSeparator), StringComparer.InvariantCultureIgnoreCase);
            }
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

            var writeEventsToQueue = await ReadWriteEventsToQueueFlag();
            _logger.Debug($"WriteEventsToQueue: {writeEventsToQueue}");

            foreach (var record in kinesisEvent.Records)
            {
                await ProcessEventRecord(record, writeEventsToQueue);
            }

            _logger.Trace("Ending AspectKinesisHandler");
        }

        private async Task<bool> ReadWriteEventsToQueueFlag()
        {
            _logger.Trace("Beginning ReadWriteEventsToQueueFlag");
            string flagStr = null;
            if (_cfgDbContext == null)
            {
                flagStr = Environment.GetEnvironmentVariable(WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP);
                _logger.Debug($"Read environment variable: {flagStr}");
            }
            else
            {
                var dynamoRecord = await _cfgDbContext.LoadAsync<ConfigRecord>(WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP);
                flagStr = dynamoRecord?.Value;
                _logger.Debug($"Read config table: {flagStr}");
                if (string.IsNullOrEmpty(flagStr))
                {
                    _logger.Debug("Config table row is missing, defaulting flag to false.");
                    flagStr = false.ToString();
                }
            }

            var flagValue = Convert.ToBoolean(flagStr);

            _logger.Trace("Ending ReadWriteEventsToQueueFlag");
            return flagValue;
        }

        private bool MatchLevel(string name)
        {
            return _ahgFilterValues.Contains(name);
        }

        private bool MatchFilter(EventRecordData eventRecord)
        {
            switch (_ahgFilterLevel)
            {
                case AHG_FILTER_LEVEL_DISABLED:
                    return true;

                case 1:
                    return MatchLevel(eventRecord?.CurrentAgentSnapshot?.Configuration?.AgentHierarchyGroups?.Level1?.Name);

                case 2:
                    return MatchLevel(eventRecord?.CurrentAgentSnapshot?.Configuration?.AgentHierarchyGroups?.Level2?.Name);

                case 3:
                    return MatchLevel(eventRecord?.CurrentAgentSnapshot?.Configuration?.AgentHierarchyGroups?.Level3?.Name);

                case 4:
                    return MatchLevel(eventRecord?.CurrentAgentSnapshot?.Configuration?.AgentHierarchyGroups?.Level4?.Name);

                case 5:
                    return MatchLevel(eventRecord?.CurrentAgentSnapshot?.Configuration?.AgentHierarchyGroups?.Level5?.Name);

                default:
                    throw new Exception($"Invalid AHG Filter Level {_ahgFilterLevel}");
            }
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
                var minimizedJSON = ShrinkEvent(recordData);
                var eventRecord = ParseEvent(minimizedJSON);
                if (eventRecord != null && MatchFilter(eventRecord))
                {
                    _logger.Debug("Event matches filter");
                    ConnectKinesisEventRecord streamRecord = ConvertRecordData(minimizedJSON, eventRecord);
                    // if we were able to parse this event, send to DynamoDB and SQS.
                    if (streamRecord != null)
                    {
                        if (streamRecord.LastEventType != HEARTBEAT_EVENTTYPE)
                        {
                            await ProcessEventToTable(streamRecord);
                        }
                        if (writeEventsToQueue)
                        {
                            await ProcessEventToQueue(minimizedJSON, streamRecord.AgentARN);
                        }
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

        private EventRecordData ParseEvent(string recordData)
        {
            _logger.Trace("Beginning ParseEvent");

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

            _logger.Trace("Ending ParseEvent");
            return recordDataObject;
        }

        public static string ShrinkEvent(string json)
        {
            var o = JObject.Parse(json);
            o.Remove("PreviousAgentSnapshot");
            var casToken = o["CurrentAgentSnapshot"];
            if (casToken != null && casToken.HasValues)
            {
                var cas = (JObject)casToken;
                if (cas != null)
                {
                    var cnfgToken = cas["Configuration"];
                    if (cnfgToken != null && cnfgToken.HasValues)
                    {
                        var cnfg = (JObject)cnfgToken;
                        cnfg?.Remove("RoutingProfile");
                    }
                }
            }
            return o.ToString(0);
        }

        private ConnectKinesisEventRecord ConvertRecordData(string json, EventRecordData eventRecord)
        {
            _logger.Trace("Beginning ConvertRecordData");

            // if this record doesn't include an agent ARN, assume it doesn't match the
            // required format. Discard it.
            if (String.IsNullOrEmpty(eventRecord?.AgentARN))
            {
                _logger.Info("recordData does not include AgentARN. Discarding recordData.");
                return null;
            }
            
            ConnectKinesisEventRecord streamRecord = new ConnectKinesisEventRecord()
            {
                AgentARN = eventRecord.AgentARN,
                AgentUsername = eventRecord.CurrentAgentSnapshot?.Configuration?.Username,
                LastEventType = eventRecord.EventType,
                LastEventTimeStamp = eventRecord.EventTimestamp, 
                LastStateChangeTimeStamp = eventRecord.CurrentAgentSnapshot?.AgentStatus.StartTimestamp,
                CurrentState = eventRecord.CurrentAgentSnapshot?.AgentStatus?.Name,
                RawAgentEventJSon = json
            };

            _logger.Debug($"Event Type: {eventRecord.EventType} Agent Username: {streamRecord.AgentUsername} Current State: {streamRecord.CurrentState} Event Time: {streamRecord.LastEventTimeStamp} State Change Time: {streamRecord.LastStateChangeTimeStamp}");

            _logger.Trace("Ending ConvertRecordData");

            return streamRecord;
        }

        private async Task ProcessEventToTable(ConnectKinesisEventRecord streamRecord)
        {
            _logger.Trace("Beginning ProcessEventsToDynamoTable");

            var dynamoRecord = await _aeDbContext.LoadAsync<ConnectKinesisEventRecord>(streamRecord.AgentARN);
            if (dynamoRecord == null || streamRecord.LastEventTimeStamp >= dynamoRecord.LastEventTimeStamp)
            {
                await _aeDbContext.SaveAsync(streamRecord); 
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

        private async Task ProcessEventToQueue(string recordData, string agentArn)
        {
            _logger.Trace("Beginning ProcessEventToQueue");

            await _sqsFacade.SendMessageToQueue(recordData, agentArn);

            _logger.Trace("Ending ProcessEventToQueue");
        }
    }
}