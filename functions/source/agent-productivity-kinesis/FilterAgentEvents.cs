using System;
using System.Collections.Generic;
using System.Text;
using Amazon.Kinesis.Model;
using Amazon.KinesisFirehose;
using Amazon.Lambda.Core;
using Amazon.Lambda.KinesisEvents;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace APKinesisLambda
{
    /// <summary>
    /// FilterAgentEvents is the class for the Agent Productivity Kinesis Lambda. It reads agent events
    /// from a Kinesis stream, parses the required information, and outputs a CSV line for each agent
    /// event to a Kinesis Firehose stream. The format matches the output format of the Kinesis Data
    /// Analytics application originally created for the Agent Productivity integration.
    /// </summary>
    public class FilterAgentEvents
    {
        private const string VERBOSE_LOGGING_ENVIRONMENT_VARIABLE = "VerboseLogging";
        private const string FIREHOSE_STREAM_NAME_ENVIRONMENT_VARIABLE = "FirehoseStreamName";
        private const string AGENT_STATUS_SEPARATOR_ENVIRONMENT_VARIABLE = "AgentStatusSeparator";
        private const string SIGNED_OFF_AGENT_STATUSES_ENVIRONMENT_VARIABLE = "SignedOffAgentStatuses";
        private readonly bool _verboseLogging;
        private readonly string _firehoseStreamName;
        private readonly SortedSet<string> _signedOffAgentStatusNames;
        private AmazonKinesisFirehoseClient _kinesisFirehoseClient;

        public FilterAgentEvents()
        {
            var verbose = Environment.GetEnvironmentVariable(VERBOSE_LOGGING_ENVIRONMENT_VARIABLE);
            _verboseLogging = verbose == "true";

            var streamName = Environment.GetEnvironmentVariable(FIREHOSE_STREAM_NAME_ENVIRONMENT_VARIABLE);
            if (string.IsNullOrEmpty(streamName))
                throw new Exception($"Missing environment variable: {FIREHOSE_STREAM_NAME_ENVIRONMENT_VARIABLE}");
            _firehoseStreamName = streamName;
            LogMessage(LogLevel.Debug, $"Firehose stream name = {_firehoseStreamName}");

            var separator = Environment.GetEnvironmentVariable(AGENT_STATUS_SEPARATOR_ENVIRONMENT_VARIABLE);
            if (string.IsNullOrEmpty(separator))
                throw new Exception($"Missing environment variable: {AGENT_STATUS_SEPARATOR_ENVIRONMENT_VARIABLE}");
            LogMessage(LogLevel.Debug, $"Status separator = {separator}");

            // The list of signed-off statuses can be empty. In that case, the OFFLINE state is the only
            // signed-off state. Trim whitespace around statuses listed, so we don't run into issues if there are
            // spaces between statuses listed, e.g., "Break, Lunch". Agent status names are case-insensitive.
            var signedOffStatuses = Environment.GetEnvironmentVariable(SIGNED_OFF_AGENT_STATUSES_ENVIRONMENT_VARIABLE);
            _signedOffAgentStatusNames = (!string.IsNullOrEmpty(signedOffStatuses)) ? 
                                            new SortedSet<string>(signedOffStatuses.Split(separator).Select(s => s.Trim()), StringComparer.InvariantCultureIgnoreCase) : 
                                            new SortedSet<string>();
            LogMessage(LogLevel.Debug, $"Signed-off statuses = {signedOffStatuses}");

            _kinesisFirehoseClient = new AmazonKinesisFirehoseClient();
        }

        public async Task APKinesisHandler(KinesisEvent kinesisEvent, ILambdaContext context)
        {
            LogMessage(LogLevel.Information, $"Record count: {kinesisEvent.Records.Count}");

            foreach (var record in kinesisEvent.Records) 
            {
                var csvLine = ParseRecord(record);
                if (!string.IsNullOrEmpty(csvLine))
                {
                    LogMessage(LogLevel.Debug, $"CSV line = {csvLine}");
                    var mem = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(csvLine + "\n"));

                    var request = new PutRecordRequest
                    {
                        StreamName = _firehoseStreamName,
                        Data = mem 
                    };
                    try
                    {
                        await _kinesisFirehoseClient.PutRecordAsync(_firehoseStreamName, new Amazon.KinesisFirehose.Model.Record() { Data = mem });
                    }
                    catch (Exception ex) 
                    {
                        LogMessage(LogLevel.Error, $"Error writing CSV line to Firehose: {ex}, discarding line");
                    }
                }
            }
        }

        private void LogMessage(LogLevel level, string message)
        {
            switch (level) 
            { 
                case LogLevel.Error:
                    Console.Error.WriteLine(message);
                    break;

                case LogLevel.Warning:
                case LogLevel.Information:
                    Console.WriteLine(message);
                    break;

                default:
                    if (_verboseLogging) 
                        Console.WriteLine(message);
                    break;
            }
        }

        private string? ParseRecord(KinesisEvent.KinesisEventRecord record)
        {
            LogMessage(LogLevel.Information, $"Kinesis EventId: {record.EventId} EventName: {record.EventName} EventSource: {record.EventSource} EventVersion: {record.EventVersion} EventSourceARN {record.EventSourceARN} InvokeIdentityARN: {record.InvokeIdentityArn} Kinesis Time: {record.Kinesis.ApproximateArrivalTimestamp} Kinesis ParitionKey: {record.Kinesis.PartitionKey} Kinesis SeqNum: {record.Kinesis.SequenceNumber}");

            string recordData = GetRecordContents(record.Kinesis);

            LogMessage(LogLevel.Information, $"RecordData: {recordData}");

            AgentEvent? agentEvent;
            try
            {
                agentEvent = JsonConvert.DeserializeObject<AgentEvent>(recordData);
            }
            catch (Exception ex) 
            {
                LogMessage(LogLevel.Error, $"Error parsing RecordData: {ex}. Discarding RecordData");
                agentEvent = null;
            }

            return BuildCSVLine(agentEvent);
        }

        private string GetRecordContents(KinesisEvent.Record streamRecord)
        {
            using (var reader = new StreamReader(streamRecord.Data, Encoding.UTF8))
            {
                return reader.ReadToEnd();
            }
        }

        private string? BuildCSVLine(AgentEvent? agentEvent) 
        {
            if (agentEvent != null)
            {
                var csvLine = new StringBuilder();
                AddCSVField(csvLine, agentEvent?.AWSAccountID);
                AddCSVField(csvLine, agentEvent?.CurrentAgentSnapshot?.Configuration?.Username);
                AddCSVField(csvLine, agentEvent?.CurrentAgentSnapshot?.Configuration?.AgentHierarchyGroups?.Level1?.Name);
                AddCSVField(csvLine, agentEvent?.CurrentAgentSnapshot?.Configuration?.AgentHierarchyGroups?.Level2?.Name);
                AddCSVField(csvLine, agentEvent?.CurrentAgentSnapshot?.Configuration?.AgentHierarchyGroups?.Level3?.Name);
                AddCSVField(csvLine, agentEvent?.CurrentAgentSnapshot?.Configuration?.AgentHierarchyGroups?.Level4?.Name);
                AddCSVField(csvLine, agentEvent?.CurrentAgentSnapshot?.Configuration?.AgentHierarchyGroups?.Level5?.Name);
                AddCSVField(csvLine, agentEvent?.EventTimestamp.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                AddCSVField(csvLine, agentEvent?.CurrentAgentSnapshot?.AgentStatus?.Name);
                AddCSVField(csvLine, ContactState(agentEvent));
                AddCSVField(csvLine, StateType(agentEvent));
                AddCSVField(csvLine, agentEvent?.CurrentAgentSnapshot?.Configuration?.RoutingProfile?.Name);
                AddCSVField(csvLine, agentEvent?.CurrentAgentSnapshot?.AgentStatus?.Type);
                return csvLine.ToString();
            }
            else
                return null;
        }

        /// <summary>
        /// Add a field value to a CSV line.
        /// </summary>
        /// <param name="sb">The contents of the CSV line so far</param>
        /// <param name="fieldValue">The field value to add</param>
        private void AddCSVField(StringBuilder sb, string? fieldValue)
        {
            // if this CSV line already has fields, separate fields with a comma
            if (sb.Length > 0)
                sb.Append(',');
            // if the field value is blank, represent with the string "null"
            if (string.IsNullOrEmpty(fieldValue))
                sb.Append("null");
            // else, we have a field value.
            else
            {
                string value = fieldValue ?? string.Empty;
                var hasComma = value.Contains(',');
                var hasQuote = value.Contains('"');
                // if the field value contains an embedded quote character or comma, need to quote the string
                if (hasComma || hasQuote)
                {
                    sb.Append('"');
                    // if the field value contains an embedded quote character, double up embedded quotes
                    if (hasQuote)
                        sb.Append(value.Replace("\"", "\"\""));
                    sb.Append('"');
                }
                // else, append value as is
                else
                    sb.Append(value);
            }
        }

        /// <summary>
        /// Extract the state of the first contact, if any.
        /// </summary>
        /// <param name="agentEvent">The agent event record to parse</param>
        /// <returns>The state of the agent's first contact or null (if the agent has no associated contacts)</returns>
        private string? ContactState(AgentEvent? agentEvent)
        {
            var contacts = agentEvent?.CurrentAgentSnapshot?.Contacts;
            // if this agent is working on at least one contact, return first contact state
            if (contacts != null && contacts.Count > 0)
                return contacts[0].State;
            // else, no contacts
            else
                return null;
        }

        /// <summary>
        /// Classify this agent event into one of the following state types:
        /// - A - this state represents available time
        /// - 0 - this state should be treated as signed-off
        /// - 1 - this state should be treated as signed-on
        /// </summary>
        /// <param name="agentEvent">The agent event record to classify</param>
        /// <returns>The corresponding state type</returns>
        private string StateType(AgentEvent? agentEvent)
        {
            var statusType = agentEvent?.CurrentAgentSnapshot?.AgentStatus?.Type;
            var statusName = agentEvent?.CurrentAgentSnapshot?.AgentStatus?.Name;
            // if this is the ROUTABLE agent status, mark as availale
            if (statusType == "ROUTABLE")
                return "A";
            // else, if this is the OFFLINE agent status, mark as signed-off
            else if (statusType == "OFFLINE")
                return "0";
            // else, if this status is in the list of signed-off statuses, mark as signed-off
            else if (statusName != null && _signedOffAgentStatusNames.Contains(statusName))
                return "0";
            // else, assum signed-on
            else
                return "1";
        }
    }
}