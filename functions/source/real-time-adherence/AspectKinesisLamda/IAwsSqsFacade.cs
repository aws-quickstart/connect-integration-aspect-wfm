using System.Threading.Tasks;

namespace AspectKinesisLamda
{
    public interface IAwsSqsFacade
    {
        Task SendMessageToQueue(string recordData, string agentArn);
    }
}