using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace APKinesisLambda
{
    internal class AgentEvent
    {
        public string? AWSAccountID { get; set; }
        public AgentSnapshot? CurrentAgentSnapshot { get; set; }
        public DateTime EventTimestamp { get; set; }
        public string? EventType { get; set; }
        public string? Version { get; set; }
    }

    internal class AgentSnapshot
    {
        public AgentStatus? AgentStatus { get; set; }
        public Configuration? Configuration { get; set; }
        public List<Contact>? Contacts { get; set; }
    }

    internal class NamedEntity
    {
        public string? Name { get; set; }
    }

    internal class AgentStatus : NamedEntity
    {
        public string? Type { get; set; }
    }

    internal class Configuration
    {
        public AgentHierarchyGroups? AgentHierarchyGroups { get; set; }
        public NamedEntity? RoutingProfile { get; set; }
        public string? Username { get; set; }
    }

    internal class AgentHierarchyGroups
    {
        public NamedEntity? Level1 { get; set; }
        public NamedEntity? Level2 { get; set; }
        public NamedEntity? Level3 { get; set; }
        public NamedEntity? Level4 { get; set; }
        public NamedEntity? Level5 { get; set; }
    }

    internal class Contact
    {
        public string? State { get; set; }
    }

}
