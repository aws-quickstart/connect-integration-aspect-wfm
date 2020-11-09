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
                RawAgentEventJSon = ProcessKinesisEvents.ShrinkEvent(recordData)
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
                RawAgentEventJSon = ProcessKinesisEvents.ShrinkEvent(recordData)
            };

            ConnectKinesisEventRecord stateChangeAvailableRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = "christy.burkholder",
                CurrentState = "Available",
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T16:23:19.019"),
                LastStateChangeTimeStamp = Convert.ToDateTime("2018-05-16T16:23:19.019"),
                RawAgentEventJSon = ProcessKinesisEvents.ShrinkEvent(recordData)
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
                RawAgentEventJSon = ProcessKinesisEvents.ShrinkEvent(recordData)
            };

            ConnectKinesisEventRecord statechangeAvailableRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = "christy.burkholder",
                CurrentState = "Available",
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T16:23:19.019"),
                LastStateChangeTimeStamp = Convert.ToDateTime("2018-05-16T16:23:19.019"),
                RawAgentEventJSon = ProcessKinesisEvents.ShrinkEvent(recordData)
            };

            ConnectKinesisEventRecord heartbeatAvailableRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = "christy.burkholder",
                CurrentState = "Available",
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T16:26:43.043"),
                LastStateChangeTimeStamp = Convert.ToDateTime("2018-05-16T16:23:19.019"),
                RawAgentEventJSon = ProcessKinesisEvents.ShrinkEvent(recordData)
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
                RawAgentEventJSon = ProcessKinesisEvents.ShrinkEvent(recordData)
            };

            ConnectKinesisEventRecord stateChangeAvailableRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = "christy.burkholder",
                CurrentState = "Available",
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T15:23:19.019"),
                LastStateChangeTimeStamp = Convert.ToDateTime("2018-05-16T15:23:19.019"),
                RawAgentEventJSon = ProcessKinesisEvents.ShrinkEvent(recordData)
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
            var shrunkRecord = ProcessKinesisEvents.ShrinkEvent(recordData);
            Assert.Equal(shrunkRecord, lastQueueMessage);
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
                RawAgentEventJSon = ProcessKinesisEvents.ShrinkEvent(recordData)
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
            Assert.Equal(loginRecord.RawAgentEventJSon, lastQueueMessage);
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
                RawAgentEventJSon = ProcessKinesisEvents.ShrinkEvent(recordData)
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
                RawAgentEventJSon = ProcessKinesisEvents.ShrinkEvent(recordData)
            };

            ConnectKinesisEventRecord stateChangeAvailableRecord = new ConnectKinesisEventRecord()
            {
                AgentARN =
                    "arn:aws:connect:us-east-1:236630710668:instance/6e3230e5-2dd2-4464-ade8-b032cdd0abef/agent/dfe8970a-ee78-4fa2-b79b-b6b3e92d35c0",
                AgentUsername = "christy.burkholder",
                CurrentState = "Available",
                LastEventTimeStamp = Convert.ToDateTime("2018-05-16T15:23:19.019"),
                LastStateChangeTimeStamp = Convert.ToDateTime("2018-05-16T15:23:19.019"),
                RawAgentEventJSon = ProcessKinesisEvents.ShrinkEvent(recordData)
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
            Assert.Equal(stateChangeAvailableRecord.RawAgentEventJSon, lastQueueMessage);
        }

        [Fact]
        public async void TestLoadTesterStateChange()
        {
            Environment.SetEnvironmentVariable(WRITE_EVENTS_TO_QUEUE_ENVIRONMENT_VARIABLE_LOOKUP, true.ToString());

            string recordData = @"{'AgentARN':'arn:aws:connect:us-east-1:271773505756:instance/cbc96518-6f6f-4088-b088-69dd5b4e48e4/agent/c2caec05-237b-41ea-8609-a1e22c53b36c','AWSAccoutId':'271773505756','CurrentAgentSnapshot':{'AgentStatus':{'ARN':'arn:aws:connect:us-east-1:271773505756:instance/2FEFAF4F-77D7-41EA-BF0A-30F78C559263/agentStatus/71098961-A697-4C52-B7E5-044B19911E2A','Name':'Available','StartTimestamp':'2020-10-08T11:53:27.107Z'},'Configuration':{'FirstName':'agent1','AgentHierarchyGroups':{'Level1':{'ARN':'arn:aws:connect:us-east-1:271773505756:instance/0f8fad5b-d9cb-469f-a165-70867728950e/agenthierarchy/1EA46F31-34AD-447E-BE87-083960EE720A','Name':'Team1'},'Level2':{'ARN':null,'Name':null},'Level3':{'ARN':null,'Name':null},'Level4':{'ARN':null,'Name':null},'Level5':{'ARN':null,'Name':null}},'LastName':'ConnAgt','RoutingProfile':{'ARN':'arn:aws:connect:us-east-1:271773505756:instance/7c9e6679-7425-40de-944b-e07fc1f90ae7/routing-profile/6815DA5C-D294-4044-A7DB-2E5ADADAB765','Name':'RoutingProfile0','InboundQueues':[{'ARN':'arn:aws:connect:us-east-1:271773505756:instance/3335BEC7-C7DE-4730-9FE9-E6F81DCE9BBA/routing-profile/B2BBD2E4-CDE9-42CB-B84B-AD4C4A7EF6DC','Name':'Queue0'},{'ARN':'arn:aws:connect:us-east-1:271773505756:instance/459B085A-70C5-4D31-AC69-32050095C063/routing-profile/E96ED4FF-28A4-456E-888F-6CD2E9A50CD7','Name':'Queue1'}],'DefaultOutboundQueue':{'ARN':'arn:aws:connect:us-east-1:271773505756:instance//818157BF-7110-4160-82D3-68DEBCCF09E4/routing-profile/37B27072-72FC-4BE5-A815-E80EFD2FA98B','Name':'OutQueue0'}},'Username':'CAagent1@aspect.com'},'Contacts':[{'ContactId':null,'InitialContactId':'0','InitiationMethod':'INBOUND','State':'CONNECTING','StartStateTimestamp':'2020-10-08T11:53:27.107Z','ConnectedToAgentTimestamp':null,'QueueTimestamp':'2020-10-08T11:53:27.107Z','Queue':{'ARN':'arn:aws:connect:us-east-1:271773505756:instance/3335BEC7-C7DE-4730-9FE9-E6F81DCE9BBA/routing-profile/B2BBD2E4-CDE9-42CB-B84B-AD4C4A7EF6DC','Name':'Queue0'}}]},'EventId':'EventId-1','EventTimestamp':'2020-10-08T11:53:27.107Z','EventType':'STATE_CHANGE','InstanceARN':'arn:aws:connect:us-east-1:271773505756:instance/aaaaaaaa-bbbb-ccccdddd-1','PreviousAgentSnapshot':{'AgentStatus':{'ARN':'arn:aws:connect:us-east-1:271773505756:instance/2FEFAF4F-77D7-41EA-BF0A-30F78C559263/agentStatus/71098961-A697-4C52-B7E5-044B19911E2A','Name':'Available','StartTimestamp':'2020-10-08T11:53:27.107Z'},'Configuration':{'FirstName':null,'AgentHierarchyGroups':{'Level1':{'ARN':'arn:aws:connect:us-east-1:271773505756:instance/1EA46F31-34AD-447E-BE87-083960EE720A/agenthierarchy/group1','Name':'Team1'},'Level2':{'ARN':null,'Name':null},'Level3':{'ARN':null,'Name':null},'Level4':{'ARN':null,'Name':null},'Level5':{'ARN':null,'Name':null}},'LastName':null,'RoutingProfile':{'ARN':'arn:aws:connect:us-east-1:271773505756:instance/7c9e6679-7425-40de-944b-e07fc1f90ae7/routing-profile/6815DA5C-D294-4044-A7DB-2E5ADADAB765','Name':'RoutingProfile0','InboundQueues':[{'ARN':'arn:aws:connect:us-east-1:271773505756:instance/3335BEC7-C7DE-4730-9FE9-E6F81DCE9BBA/routing-profile/B2BBD2E4-CDE9-42CB-B84B-AD4C4A7EF6DC','Name':'Queue0'},{'ARN':'arn:aws:connect:us-east-1:271773505756:instance/459B085A-70C5-4D31-AC69-32050095C063/routing-profile/E96ED4FF-28A4-456E-888F-6CD2E9A50CD7','Name':'Queue1'}],'DefaultOutboundQueue':{'ARN':'arn:aws:connect:us-east-1:271773505756:instance/818157BF-7110-4160-82D3-68DEBCCF09E4/routing-profile/37B27072-72FC-4BE5-A815-E80EFD2FA98B','Name':'OutQueue0'}},'Username':null},'Contacts':[{'ContactId':null,'InitialContactId':'0','InitiationMethod':'INBOUND','State':'CONNECTING','StartStateTimestamp':'2020-10-08T11:53:27.107Z','ConnectedToAgentTimestamp':null,'QueueTimestamp':'2020-10-08T11:53:27.107Z','Queue':{'ARN':'arn:aws:connect:us-east-1:271773505756:instance/3335BEC7-C7DE-4730-9FE9-E6F81DCE9BBA/routing-profile/B2BBD2E4-CDE9-42CB-B84B-AD4C4A7EF6DC','Name':'Queue0'}}]},'Version':'2019-05-25'}";

            ConnectKinesisEventRecord stateChangeRecord = new ConnectKinesisEventRecord()
            {
                AgentARN = "arn:aws:connect:us-east-1:271773505756:instance/cbc96518-6f6f-4088-b088-69dd5b4e48e4/agent/c2caec05-237b-41ea-8609-a1e22c53b36c",
                AgentUsername = "CAagent1@aspect.com",
                CurrentState = "Available",
                LastEventTimeStamp = Convert.ToDateTime("2020-10-08T11:53:27.107"),
                LastStateChangeTimeStamp = Convert.ToDateTime("2020-10-08T11:53:27.107"),
                RawAgentEventJSon = ProcessKinesisEvents.ShrinkEvent(recordData)
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

            ConnectKinesisEventRecord dynamoRecord = fakeDbContext.DynamoTable[stateChangeRecord.AgentARN];
            AssertExpectedRecordAgainstDynamoRecord(stateChangeRecord, dynamoRecord);
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
