using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;
using Amazon.Lambda.KinesisEvents;
using Amazon.Lambda.TestUtilities;

namespace AspectKinesisLamda.Tests
{
    public class FunctionTest
    {
        private const string WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP = "WriteEventsToQueue";

        [Fact]
        public async void TestNewAgentLogIn()
        {
            Environment.SetEnvironmentVariable(WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP, true.ToString());

            string recordData = @" {
            'AWSAccountId': '236630710668',
            'AgentARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0',
            'CurrentAgentSnapshot': null,
            'EventId': '8d2daa4f-a6b4-4c3c-8599-08c355a20f80',
            'EventTimestamp': '2018-05-16T16:12:59.059Z',
            'EventType': 'LOGIN',
            'InstanceARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef',
            'PreviousAgentSnapshot': null,
            'Version': '2017-10-01'
            }";

            ConnectKinesisEventRecord loginRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = null,
                CurrentState = null,
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T16:12:59.059"),
                LastStateChangeTimeStamp = null,
                RawAgentEventJSon = recordData
            };

            var evnt = GenerateKinesisEvent(recordData);

            var context = new TestLambdaContext()
            {
                Logger = new FakeLambdaLogger()
            };
            FakeDynamoDBContext fakeDbContext = new FakeDynamoDBContext();
            FakeSQSFacade fakeSqsFacade = new FakeSQSFacade();
           
            var function = new ProcessKinesisEvents(fakeDbContext, fakeSqsFacade);
            await function.AspectKinesisHandler(evnt, context);

            ConnectKinesisEventRecord dynamoRecord = fakeDbContext.DynamoTable[loginRecord.AgentARN];
            AssertExpectedRecordAgainstDynamoRecord(loginRecord, dynamoRecord);
        }

        [Fact]
        public async void TestUpdatingAgentStateChange()
        {
            Environment.SetEnvironmentVariable(WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP, true.ToString());

            string recordData = @" {
    'AWSAccountId': '236630710668',
            'AgentARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0',
            'CurrentAgentSnapshot': {
                'AgentStatus': {
                    'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent-state/24728943-1643-4181-9844-6aec6e981563',
                    'Name': 'Available',
                    'StartTimestamp': '2018-05-16T16:23:19.019Z'
                },
                'Configuration': {
                    'AgentHierarchyGroups': null,
                    'FirstName': 'Christy',
                    'LastName': 'Burkholder',
                    'RoutingProfile': {
                        'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/routing-profile/12d022b9-6480-4fd4-b5b9-ca72bc66e8a1',
                        'DefaultOutboundQueue': {
                            'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/queue/c5407123-79de-4df1-8afd-fb74a5df5162',
                            'Name': 'BasicQueue'
                        },
                        'InboundQueues': [
                        {
                            'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/queue/c5407123-79de-4df1-8afd-fb74a5df5162',
                            'Name': 'BasicQueue'
                        }
                        ],
                        'Name': 'Basic Routing Profile'
                    },
                    'Username': 'christy.burkholder'
                },
                'Contacts': []
            },
            'EventId': '7315b213-f9c3-4362-b2d6-e26c79dbd3ea',
            'EventTimestamp': '2018-05-16T16:23:19.019Z',
            'EventType': 'STATE_CHANGE',
            'InstanceARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef',
            'PreviousAgentSnapshot': null,
            'Version': '2017-10-01'
        }";

            ConnectKinesisEventRecord loginRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = null,
                CurrentState = null,
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T16:12:59.059"),
                LastStateChangeTimeStamp = null,
                RawAgentEventJSon = recordData
            };

            ConnectKinesisEventRecord stateChangeAvailableRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = "christy.burkholder",
                CurrentState = "Available",
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T16:23:19.019"),
                LastStateChangeTimeStamp = Convert.ToDateTime("2018-05-16T16:23:19.019"),
                RawAgentEventJSon = recordData
            };

            var evnt = GenerateKinesisEvent(recordData);

            var context = new TestLambdaContext()
            {
                Logger = new FakeLambdaLogger()
            };
            FakeDynamoDBContext fakeDbContext = new FakeDynamoDBContext();
            FakeSQSFacade fakeSqsFacade = new FakeSQSFacade();

            await fakeDbContext.SaveAsync(loginRecord);

            var function = new ProcessKinesisEvents(fakeDbContext, fakeSqsFacade);
            await function.AspectKinesisHandler(evnt, context);

            ConnectKinesisEventRecord dynamoRecord = fakeDbContext.DynamoTable[stateChangeAvailableRecord.AgentARN];
            AssertExpectedRecordAgainstDynamoRecord(stateChangeAvailableRecord, dynamoRecord);
        }

        [Fact]
        public async void TestUpdatingAgentHeartbeat()
        {
            Environment.SetEnvironmentVariable(WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP, true.ToString());

            string recordData = @" {
    'AWSAccountId': '236630710668',
    'AgentARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0',
    'CurrentAgentSnapshot': {
                'AgentStatus': {
                    'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent-state/24728943-1643-4181-9844-6aec6e981563',
        'Name': 'Available',
        'StartTimestamp': '2018-05-16T16:23:19.019Z'
                },
      'Configuration': {
                    'AgentHierarchyGroups': null,
        'FirstName': 'Christy',
        'LastName': 'Burkholder',
        'RoutingProfile': {
                        'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/routing-profile/12d022b9-6480-4fd4-b5b9-ca72bc66e8a1',
          'DefaultOutboundQueue': {
                            'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/queue/c5407123-79de-4df1-8afd-fb74a5df5162',
            'Name': 'BasicQueue'
          },
          'InboundQueues': [
            {
              'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/queue/c5407123-79de-4df1-8afd-fb74a5df5162',
              'Name': 'BasicQueue'
            }
          ],
          'Name': 'Basic Routing Profile'
        },
        'Username': 'christy.burkholder'
      },
      'Contacts': []
    },
    'EventId': '75daa20d-0bea-4c34-b469-60f3c7166dbe',
    'EventTimestamp': '2018-05-16T16:26:43.043Z',
    'EventType': 'HEART_BEAT',
    'InstanceARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef',
    'PreviousAgentSnapshot': {
      'AgentStatus': {
        'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent-state/24728943-1643-4181-9844-6aec6e981563',
        'Name': 'Available',
        'StartTimestamp': '2018-05-16T16:23:19.019Z'
      },
      'Configuration': {
        'AgentHierarchyGroups': null,
        'FirstName': 'Christy',
        'LastName': 'Burkholder',
        'RoutingProfile': {
          'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/routing-profile/12d022b9-6480-4fd4-b5b9-ca72bc66e8a1',
          'DefaultOutboundQueue': {
            'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/queue/c5407123-79de-4df1-8afd-fb74a5df5162',
            'Name': 'BasicQueue'
          },
          'InboundQueues': [
            {
              'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/queue/c5407123-79de-4df1-8afd-fb74a5df5162',
              'Name': 'BasicQueue'
            }
          ],
          'Name': 'Basic Routing Profile'
        },
        'Username': 'christy.burkholder'
      },
      'Contacts': []
    },
    'Version': '2017-10-01'
  }";

            ConnectKinesisEventRecord loginRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = null,
                CurrentState = null,
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T16:12:59.059"),
                LastStateChangeTimeStamp = null,
                RawAgentEventJSon = recordData
            };

            ConnectKinesisEventRecord statechangeAvailableRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = "christy.burkholder",
                CurrentState = "Available",
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T16:23:19.019"),
                LastStateChangeTimeStamp = Convert.ToDateTime("2018-05-16T16:23:19.019"),
                RawAgentEventJSon = recordData
            };

            ConnectKinesisEventRecord heartbeatAvailableRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = "christy.burkholder",
                CurrentState = "Available",
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T16:26:43.043"),
                LastStateChangeTimeStamp = Convert.ToDateTime("2018-05-16T16:23:19.019"),
                RawAgentEventJSon = recordData
            };

            var evnt = GenerateKinesisEvent(recordData);

            var context = new TestLambdaContext()
            {
                Logger = new FakeLambdaLogger()
            };
            FakeDynamoDBContext fakeDbContext = new FakeDynamoDBContext();
            FakeSQSFacade fakeSqsFacade = new FakeSQSFacade();

            await fakeDbContext.SaveAsync(loginRecord);
            await fakeDbContext.SaveAsync(statechangeAvailableRecord);

            var function = new ProcessKinesisEvents(fakeDbContext, fakeSqsFacade);
            await function.AspectKinesisHandler(evnt, context);

            ConnectKinesisEventRecord dynamoRecord = fakeDbContext.DynamoTable[heartbeatAvailableRecord.AgentARN];
            AssertExpectedRecordAgainstDynamoRecord(statechangeAvailableRecord, dynamoRecord);
        }

        [Fact]
        public async void TestUpdatingAgentOlderStateChange()  
        {
            Environment.SetEnvironmentVariable(WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP, true.ToString());

            string recordData = @" {
    'AWSAccountId': '236630710668',
            'AgentARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0',
            'CurrentAgentSnapshot': {
                'AgentStatus': {
                    'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent-state/24728943-1643-4181-9844-6aec6e981563',
                    'Name': 'Available',
                    'StartTimestamp': '2018-05-16T15:23:19.019Z'
                },
                'Configuration': {
                    'AgentHierarchyGroups': null,
                    'FirstName': 'Christy',
                    'LastName': 'Burkholder',
                    'RoutingProfile': {
                        'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/routing-profile/12d022b9-6480-4fd4-b5b9-ca72bc66e8a1',
                        'DefaultOutboundQueue': {
                            'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/queue/c5407123-79de-4df1-8afd-fb74a5df5162',
                            'Name': 'BasicQueue'
                        },
                        'InboundQueues': [
                        {
                            'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/queue/c5407123-79de-4df1-8afd-fb74a5df5162',
                            'Name': 'BasicQueue'
                        }
                        ],
                        'Name': 'Basic Routing Profile'
                    },
                    'Username': 'christy.burkholder'
                },
                'Contacts': []
            },
            'EventId': '7315b213-f9c3-4362-b2d6-e26c79dbd3ea',
            'EventTimestamp': '2018-05-16T15:23:19.019Z',
            'EventType': 'STATE_CHANGE',
            'InstanceARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef',
            'PreviousAgentSnapshot': null,
            'Version': '2017-10-01'
        }";

            ConnectKinesisEventRecord loginRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = null,
                CurrentState = null,
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T16:12:59.059"),
                LastStateChangeTimeStamp = null,
                RawAgentEventJSon = recordData
            };

            ConnectKinesisEventRecord stateChangeAvailableRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = "christy.burkholder",
                CurrentState = "Available",
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T15:23:19.019"),
                LastStateChangeTimeStamp = Convert.ToDateTime("2018-05-16T15:23:19.019"),
                RawAgentEventJSon = recordData
            };

            var evnt = GenerateKinesisEvent(recordData);

            var context = new TestLambdaContext()
            {
                Logger = new FakeLambdaLogger()
            };
            FakeDynamoDBContext fakeDbContext = new FakeDynamoDBContext();
            FakeSQSFacade fakeSqsFacade = new FakeSQSFacade();

            await fakeDbContext.SaveAsync(loginRecord);

            var function = new ProcessKinesisEvents(fakeDbContext, fakeSqsFacade);
            await function.AspectKinesisHandler(evnt, context);

            ConnectKinesisEventRecord dynamoRecord = fakeDbContext.DynamoTable[stateChangeAvailableRecord.AgentARN];
            AssertExpectedRecordAgainstDynamoRecord(loginRecord, dynamoRecord);
        }

        [Fact]
        public async void TestFirstWriteToQueueTrue()
        {
            Environment.SetEnvironmentVariable(WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP, true.ToString());

            string recordData = @" {
            'AWSAccountId': '236630710668',
            'AgentARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0',
            'CurrentAgentSnapshot': null,
            'EventId': '8d2daa4f-a6b4-4c3c-8599-08c355a20f80',
            'EventTimestamp': '2018-05-16T16:12:59.059Z',
            'EventType': 'LOGIN',
            'InstanceARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef',
            'PreviousAgentSnapshot': null,
            'Version': '2017-10-01'
            }";

            var evnt = GenerateKinesisEvent(recordData);

            var context = new TestLambdaContext()
            {
                Logger = new FakeLambdaLogger()
            };
            FakeDynamoDBContext fakeDbContext = new FakeDynamoDBContext();
            FakeSQSFacade fakeSqsFacade = new FakeSQSFacade();

            var function = new ProcessKinesisEvents(fakeDbContext, fakeSqsFacade);
            await function.AspectKinesisHandler(evnt, context);
           
            Assert.True(fakeSqsFacade.SqsQueue.Count ==1);
            string lastQueueMessage = fakeSqsFacade.SqsQueue[fakeSqsFacade.SqsQueue.Count - 1];
            Assert.Equal(recordData, lastQueueMessage);
        }

        [Fact]
        public async void TestFirstWriteToQueueFalse()
        {
            Environment.SetEnvironmentVariable(WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP,false.ToString());

            string recordData = @" {
            'AWSAccountId': '236630710668',
            'AgentARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0',
            'CurrentAgentSnapshot': null,
            'EventId': '8d2daa4f-a6b4-4c3c-8599-08c355a20f80',
            'EventTimestamp': '2018-05-16T16:12:59.059Z',
            'EventType': 'LOGIN',
            'InstanceARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef',
            'PreviousAgentSnapshot': null,
            'Version': '2017-10-01'
            }";

            var evnt = GenerateKinesisEvent(recordData);

            var context = new TestLambdaContext()
            {
                Logger = new FakeLambdaLogger()
            };
            FakeDynamoDBContext fakeDbContext = new FakeDynamoDBContext();
            FakeSQSFacade fakeSqsFacade = new FakeSQSFacade();

            var function = new ProcessKinesisEvents(fakeDbContext, fakeSqsFacade);
            await function.AspectKinesisHandler(evnt, context);
            
            Assert.True(fakeSqsFacade.SqsQueue.Count == 0);
        }

        [Fact]
        public async void TestNonFirstWriteToQueueTrue()
        {
            Environment.SetEnvironmentVariable(WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP, true.ToString());

            string recordData = @" {
    'AWSAccountId': '236630710668',
            'AgentARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0',
            'CurrentAgentSnapshot': {
                'AgentStatus': {
                    'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent-state/24728943-1643-4181-9844-6aec6e981563',
                    'Name': 'Available',
                    'StartTimestamp': '2018-05-16T16:23:19.019Z'
                },
                'Configuration': {
                    'AgentHierarchyGroups': null,
                    'FirstName': 'Christy',
                    'LastName': 'Burkholder',
                    'RoutingProfile': {
                        'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/routing-profile/12d022b9-6480-4fd4-b5b9-ca72bc66e8a1',
                        'DefaultOutboundQueue': {
                            'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/queue/c5407123-79de-4df1-8afd-fb74a5df5162',
                            'Name': 'BasicQueue'
                        },
                        'InboundQueues': [
                        {
                            'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/queue/c5407123-79de-4df1-8afd-fb74a5df5162',
                            'Name': 'BasicQueue'
                        }
                        ],
                        'Name': 'Basic Routing Profile'
                    },
                    'Username': 'christy.burkholder'
                },
                'Contacts': []
            },
            'EventId': '7315b213-f9c3-4362-b2d6-e26c79dbd3ea',
            'EventTimestamp': '2018-05-16T16:23:19.019Z',
            'EventType': 'STATE_CHANGE',
            'InstanceARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef',
            'PreviousAgentSnapshot': null,
            'Version': '2017-10-01'
        }";

            ConnectKinesisEventRecord loginRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = null,
                CurrentState = null,
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T16:12:59.059"),
                LastStateChangeTimeStamp = null,
                RawAgentEventJSon = recordData
            };

            var evnt = GenerateKinesisEvent(recordData);

            var context = new TestLambdaContext()
            {
                Logger = new FakeLambdaLogger()
            };
            FakeDynamoDBContext fakeDbContext = new FakeDynamoDBContext();
            FakeSQSFacade fakeSqsFacade = new FakeSQSFacade();

            await fakeDbContext.SaveAsync(loginRecord);
            fakeSqsFacade.SqsQueue.Add(("First message"));

            var function = new ProcessKinesisEvents(fakeDbContext, fakeSqsFacade);
            await function.AspectKinesisHandler(evnt, context);

            Assert.True(fakeSqsFacade.SqsQueue.Count == 2);
            string lastQueueMessage = fakeSqsFacade.SqsQueue[fakeSqsFacade.SqsQueue.Count - 1];
            Assert.Equal(recordData, lastQueueMessage);
        }

        [Fact]
        public async void TestNonFirstWriteToQueueFalse()
        {
            Environment.SetEnvironmentVariable(WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP, false.ToString());

            string recordData = @" {
    'AWSAccountId': '236630710668',
            'AgentARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0',
            'CurrentAgentSnapshot': {
                'AgentStatus': {
                    'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent-state/24728943-1643-4181-9844-6aec6e981563',
                    'Name': 'Available',
                    'StartTimestamp': '2018-05-16T16:23:19.019Z'
                },
                'Configuration': {
                    'AgentHierarchyGroups': null,
                    'FirstName': 'Christy',
                    'LastName': 'Burkholder',
                    'RoutingProfile': {
                        'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/routing-profile/12d022b9-6480-4fd4-b5b9-ca72bc66e8a1',
                        'DefaultOutboundQueue': {
                            'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/queue/c5407123-79de-4df1-8afd-fb74a5df5162',
                            'Name': 'BasicQueue'
                        },
                        'InboundQueues': [
                        {
                            'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/queue/c5407123-79de-4df1-8afd-fb74a5df5162',
                            'Name': 'BasicQueue'
                        }
                        ],
                        'Name': 'Basic Routing Profile'
                    },
                    'Username': 'christy.burkholder'
                },
                'Contacts': []
            },
            'EventId': '7315b213-f9c3-4362-b2d6-e26c79dbd3ea',
            'EventTimestamp': '2018-05-16T16:23:19.019Z',
            'EventType': 'STATE_CHANGE',
            'InstanceARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef',
            'PreviousAgentSnapshot': null,
            'Version': '2017-10-01'
        }";

            ConnectKinesisEventRecord loginRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = null,
                CurrentState = null,
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T16:12:59.059"),
                LastStateChangeTimeStamp = null,
                RawAgentEventJSon = recordData
            };

            var evnt = GenerateKinesisEvent(recordData);

            var context = new TestLambdaContext();
            FakeDynamoDBContext fakeDbContext = new FakeDynamoDBContext();
            FakeSQSFacade fakeSqsFacade = new FakeSQSFacade();

            await fakeDbContext.SaveAsync(loginRecord);
            fakeSqsFacade.SqsQueue.Add(("First message"));

            var function = new ProcessKinesisEvents(fakeDbContext, fakeSqsFacade);
            await function.AspectKinesisHandler(evnt, context);
          
            Assert.True(fakeSqsFacade.SqsQueue.Count ==1);
        }

        [Fact]
        public async void TestWriteToQueueOlderStateChange() 
        {
            Environment.SetEnvironmentVariable(WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP, true.ToString());

            string recordData = @" {
    'AWSAccountId': '236630710668',
            'AgentARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0',
            'CurrentAgentSnapshot': {
                'AgentStatus': {
                    'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent-state/24728943-1643-4181-9844-6aec6e981563',
                    'Name': 'Available',
                    'StartTimestamp': '2018-05-16T15:23:19.019Z'
                },
                'Configuration': {
                    'AgentHierarchyGroups': null,
                    'FirstName': 'Christy',
                    'LastName': 'Burkholder',
                    'RoutingProfile': {
                        'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/routing-profile/12d022b9-6480-4fd4-b5b9-ca72bc66e8a1',
                        'DefaultOutboundQueue': {
                            'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/queue/c5407123-79de-4df1-8afd-fb74a5df5162',
                            'Name': 'BasicQueue'
                        },
                        'InboundQueues': [
                        {
                            'ARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/queue/c5407123-79de-4df1-8afd-fb74a5df5162',
                            'Name': 'BasicQueue'
                        }
                        ],
                        'Name': 'Basic Routing Profile'
                    },
                    'Username': 'christy.burkholder'
                },
                'Contacts': []
            },
            'EventId': '7315b213-f9c3-4362-b2d6-e26c79dbd3ea',
            'EventTimestamp': '2018-05-16T15:23:19.019Z',
            'EventType': 'STATE_CHANGE',
            'InstanceARN': 'arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef',
            'PreviousAgentSnapshot': null,
            'Version': '2017-10-01'
        }";

            ConnectKinesisEventRecord loginRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = null,
                CurrentState = null,
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T16:12:59.059"),
                LastStateChangeTimeStamp = null,
                RawAgentEventJSon = recordData
            };

            ConnectKinesisEventRecord stateChangeAvailableRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = "christy.burkholder",
                CurrentState = "Available",
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T15:23:19.019"),
                LastStateChangeTimeStamp = Convert.ToDateTime("2018-05-16T15:23:19.019"),
                RawAgentEventJSon = recordData
            };

            var evnt = GenerateKinesisEvent(recordData);

            var context = new TestLambdaContext();
            FakeDynamoDBContext fakeDbContext = new FakeDynamoDBContext();
            FakeSQSFacade fakeSqsFacade = new FakeSQSFacade();

            await fakeDbContext.SaveAsync(loginRecord);
            fakeSqsFacade.SqsQueue.Add(("First message"));

            var function = new ProcessKinesisEvents(fakeDbContext, fakeSqsFacade);
            await function.AspectKinesisHandler(evnt, context);
           
            Assert.True(fakeSqsFacade.SqsQueue.Count == 2);
            string lastQueueMessage = fakeSqsFacade.SqsQueue[fakeSqsFacade.SqsQueue.Count - 1];
            Assert.Equal(recordData, lastQueueMessage);
        }

        private static KinesisEvent GenerateKinesisEvent(string recordData)
        {
            KinesisEvent evnt = new KinesisEvent
            {
                Records = new List<KinesisEvent.KinesisEventRecord>
                {
                    new KinesisEvent.KinesisEventRecord
                    {
                        AwsRegion = "us-east-1",
                        Kinesis = new KinesisEvent.Record
                        {
                            ApproximateArrivalTimestamp = DateTime.Now,
                            Data = new MemoryStream(Encoding.UTF8.GetBytes(recordData))
                        }
                    }
                }
            };
            return evnt;
        }

        private static void AssertExpectedRecordAgainstDynamoRecord(ConnectKinesisEventRecord expectedRecord, ConnectKinesisEventRecord dynamoRecord)
        {
            Assert.Equal(expectedRecord.AgentUsername, dynamoRecord.AgentUsername);
            Assert.Equal(expectedRecord.CurrentState, dynamoRecord.CurrentState);
            Assert.Equal(expectedRecord.LastEventTimeStamp, dynamoRecord.LastEventTimeStamp); 
            Assert.Equal(expectedRecord.LastStateChangeTimeStamp, dynamoRecord.LastStateChangeTimeStamp);
            Assert.Equal(expectedRecord.RawAgentEventJSon, dynamoRecord.RawAgentEventJSon);

        }
    }
}
