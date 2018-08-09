using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

namespace AspectKinesisLamda.Tests
{
    public class FakeDynamoDBContext : IDynamoDBContext
    {
        public Dictionary<string, ConnectKinesisEventRecord> DynamoTable { get; }

        public FakeDynamoDBContext()
        {
            DynamoTable = new Dictionary<string, ConnectKinesisEventRecord>();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public Document ToDocument<T>(T value)
        {
            throw new NotImplementedException();
        }

        public Document ToDocument<T>(T value, DynamoDBOperationConfig operationConfig)
        {
            throw new NotImplementedException();
        }

        public T FromDocument<T>(Document document)
        {
            throw new NotImplementedException();
        }

        public T FromDocument<T>(Document document, DynamoDBOperationConfig operationConfig)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<T> FromDocuments<T>(IEnumerable<Document> documents)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<T> FromDocuments<T>(IEnumerable<Document> documents, DynamoDBOperationConfig operationConfig)
        {
            throw new NotImplementedException();
        }

        public BatchGet<T> CreateBatchGet<T>(DynamoDBOperationConfig operationConfig = null)
        {
            throw new NotImplementedException();
        }

        public MultiTableBatchGet CreateMultiTableBatchGet(params BatchGet[] batches)
        {
            throw new NotImplementedException();
        }

        public BatchWrite<T> CreateBatchWrite<T>(DynamoDBOperationConfig operationConfig = null)
        {
            throw new NotImplementedException();
        }

        public MultiTableBatchWrite CreateMultiTableBatchWrite(params BatchWrite[] batches)
        {
            throw new NotImplementedException();
        }

        public Task SaveAsync<T>(T value, CancellationToken cancellationToken = new CancellationToken())
        {
            ConnectKinesisEventRecord record = (ConnectKinesisEventRecord)(object)value;
            string key = record.AgentARN;
            if (DynamoTable.ContainsKey(key))
            {
                DynamoTable[key] = record;
            }
            else
            {
                DynamoTable.Add(key, record);
            }

            return Task.CompletedTask;
        }

        public Task SaveAsync<T>(T value, DynamoDBOperationConfig operationConfig,
            CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<T> LoadAsync<T>(object hashKey, CancellationToken cancellationToken = new CancellationToken())
        {
            string key = hashKey.ToString();
            T record = default(T); 
            if (DynamoTable.ContainsKey(key))
            {
               record  = (T)(object)DynamoTable[key];
            }
            return Task.FromResult<T>(record);
        }

        public Task<T> LoadAsync<T>(object hashKey, DynamoDBOperationConfig operationConfig,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return null;
        }

        public Task<T> LoadAsync<T>(object hashKey, object rangeKey, CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<T> LoadAsync<T>(object hashKey, object rangeKey, DynamoDBOperationConfig operationConfig,
            CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<T> LoadAsync<T>(T keyObject, CancellationToken cancellationToken = new CancellationToken())
        {
            return null;
        }

        public Task<T> LoadAsync<T>(T keyObject, DynamoDBOperationConfig operationConfig,
            CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync<T>(T value, CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync<T>(T value, DynamoDBOperationConfig operationConfig,
            CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync<T>(object hashKey, CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync<T>(object hashKey, DynamoDBOperationConfig operationConfig,
            CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync<T>(object hashKey, object rangeKey, CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync<T>(object hashKey, object rangeKey, DynamoDBOperationConfig operationConfig,
            CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task ExecuteBatchGetAsync(BatchGet[] batches, CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task ExecuteBatchWriteAsync(BatchWrite[] batches, CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public AsyncSearch<T> ScanAsync<T>(IEnumerable<ScanCondition> conditions, DynamoDBOperationConfig operationConfig = null)
        {
            throw new NotImplementedException();
        }

        public AsyncSearch<T> FromScanAsync<T>(ScanOperationConfig scanConfig, DynamoDBOperationConfig operationConfig = null)
        {
            throw new NotImplementedException();
        }

        public AsyncSearch<T> QueryAsync<T>(object hashKeyValue, DynamoDBOperationConfig operationConfig = null)
        {
            throw new NotImplementedException();
        }

        public AsyncSearch<T> QueryAsync<T>(object hashKeyValue, QueryOperator op, IEnumerable<object> values,
            DynamoDBOperationConfig operationConfig = null)
        {
            throw new NotImplementedException();
        }

        public AsyncSearch<T> FromQueryAsync<T>(QueryOperationConfig queryConfig, DynamoDBOperationConfig operationConfig = null)
        {
            throw new NotImplementedException();
        }

        public Table GetTargetTable<T>(DynamoDBOperationConfig operationConfig = null)
        {
            throw new NotImplementedException();
        }
    }
}