using System;
using Amazon.DynamoDBv2.DataModel;

namespace AspectKinesisLamda
{
    public class ConnectKinesisEventRecord
    {
        [DynamoDBHashKey]
        public string AgentARN { get; set; }
        public string AgentUsername { get; set; }
        public string LastEventType { get; set; }
        public DateTime LastEventTimeStamp { get; set; }
        public DateTime? LastStateChangeTimeStamp { get; set; }
        public string CurrentState { get; set; }
        public string RawAgentEventJSon { get; set; } 
    }

    public class ConfigRecord
    {
        [DynamoDBHashKey]
        public string Setting { get; set; }
        public string Value { get; set; }
    }

    public class EventRecordData
    {
        public string AgentARN { get; set; }
        public Currentagentsnapshot CurrentAgentSnapshot { get; set; }
        public string EventId { get; set; }
        public DateTime EventTimestamp { get; set; }
        public string EventType { get; set; }
    }

    public class Currentagentsnapshot
    {
        public Agentstatus AgentStatus { get; set; }
        public Configuration Configuration { get; set; }
    }

    public class Agentstatus
    {
        public string ARN { get; set; }
        public string Name { get; set; }
        public DateTime StartTimestamp { get; set; }
    }

    public class HierarchyGroup
    {
        public string Name { get; set; }
    }

    public class AgentHierarchyGroups
    {
        public HierarchyGroup Level1 { get; set; }
        public HierarchyGroup Level2 { get; set; }
        public HierarchyGroup Level3 { get; set; }
        public HierarchyGroup Level4 { get; set; }
        public HierarchyGroup Level5 { get; set; }
    }

    public class Configuration
    {
        public AgentHierarchyGroups AgentHierarchyGroups { get; set; }
        public string Username { get; set; }
    }
}
