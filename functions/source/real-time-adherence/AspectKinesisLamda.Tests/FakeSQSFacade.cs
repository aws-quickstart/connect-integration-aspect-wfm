using System.Collections.Generic;
using System.Threading.Tasks;

namespace AspectKinesisLamda.Tests
{
    public class FakeSQSFacade : IAwsSqsFacade
    {
        public List<string> SqsQueue { get; }

        public FakeSQSFacade()
        {
           SqsQueue = new List<string>();
        }

        public Task SendMessageToQueue(string recordData, string agentArn)
        {
            SqsQueue.Add(recordData);
            return Task.CompletedTask;
        }
    }
}