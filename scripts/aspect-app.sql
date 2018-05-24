CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM"
  ("AWSAccountId" char(12),
  "Username" varchar(256),
  "AHGLevel1" varchar(256),
  "AHGLevel2" varchar(256),
  "AHGLevel3" varchar(256),
  "AHGLevel4" varchar(256),
  "AHGLevel5" varchar(256),
  "EventTimestamp" timestamp,
  "StateName" varchar(256),
  "ContactState" varchar(256),
  "StateType" char(1));

CREATE OR REPLACE STREAM "TEMP_SQL_STREAM"
  ("AWSAccountId" char(12),
  "Username" varchar(256),
  "AHGLevel1" varchar(256),
  "AHGLevel2" varchar(256),
  "AHGLevel3" varchar(256),
  "AHGLevel4" varchar(256),
  "AHGLevel5" varchar(256),
  "EventTimestamp" timestamp,
  "StateName" varchar(256),
  "ContactState" varchar(256),
  "StateType" char(1));

CREATE OR REPLACE PUMP "TEMP_PUMP" AS
   INSERT INTO TEMP_SQL_STREAM
       SELECT STREAM a."AWSAccountId", a."casUsername", a."casAhgL1Name", a."casAhgL2Name", a."casAhgL3Name", a."casAhgL4Name", a."casAhgL5Name", a."EventTimestamp", a."casStateName", a."casContactState", b."StateType"
       FROM SOURCE_SQL_STREAM_001 AS a
       LEFT OUTER JOIN AGENT_STATUS_INFO AS b
       ON a."casStateName" = b."StateName"
       WHERE a."casUsername" IS NOT NULL;

CREATE OR REPLACE PUMP "STREAM_PUMP" AS
   INSERT INTO DESTINATION_SQL_STREAM
      SELECT STREAM "AWSAccountId", "Username", "AHGLevel1", "AHGLevel2", "AHGLevel3", "AHGLevel4", "AHGLevel5", "EventTimestamp", "StateName", "ContactState", COALESCE("StateType", '1')
      FROM TEMP_SQL_STREAM
      WINDOW W1 AS (PARTITION BY "AWSAccountId" RANGE INTERVAL '15' MINUTE PRECEDING);
