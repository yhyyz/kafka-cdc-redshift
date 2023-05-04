[toc]

### CDC多库多表实时入仓(Redshift)方案

#### 一、背景说明

##### 1.1 Redshift简要说明

Redshfit作为当今时代性价比最优的企业级云上数仓，不仅采用`code generation,vectorized execution`等技术优化其执行引擎，提供卓越的性能表现。同时作为云原生的企业级数仓，Redshfit在稳定性，安全性，弹性扩展，容灾能力等方面也不断创新，为企业客户的智能数据分析保驾护航。这里列举几个Redshift的功能特性。1. 存算分离的架构，底层数据以专用格式存储在S3，计算资源不受存储限制可以灵活弹性扩展。2. DataSharing可以做到跨账号，跨Region的数据秒级共享，无需移动数据，底层存储是同一份数据。3. Redshift Serverless无需预置管理维护集群,随查询并发自动扩展RPU计算资源，按RPU计算时长付费，不查询不收费。4.Redshift ML可以SQL的方式创建机器学习模型，自动选择最优的算法，自动生成推理的UDF函数，在仓中直接完成对数据的深度探索。5. Streaming Ingestion可以直接将MSK和KDS的数据摄入到Redshift，无需部署任何其它组件，秒级别延迟。到2023年，Redshift历经11年的战火洗礼和技术创新，始终追求卓越，为数万企业保驾护航。

##### 1.2 CDC工具和Redshiftd结合

关于CDC的基本介绍及相关的工具对比在[该篇数据入湖的文章](https://aws.amazon.com/cn/blogs/china/best-practice-of-using-amazon-emr-cdc-to-enter-the-lake-in-real-time-in-a-multi-database-multi-table-scenario/)中已经详细说明，这里不再赘述,唯一需要说明的是对于MSK Connector和Flink CDC其内核都是Debezium。总体而言当前CDC实时写入Redshift方式有两种模式。

第一种模式，CDC工具解析Binlog(比如MySQL)数据直接将数据Sink到Redshift。能以这种方式实现的工具，AWS的服务目前只有一个`DMS(Database Migartion Service)`,其它商业的公司的付费工具比如`Fivetran`也可以实现。开源的工具Flink CDC通过DataStream API做深度的定制也可以做到。大家可能会有疑问可不可以直接使用Flink JDBC的Connector Sink数据，这里多做一些解释，Flink JDBC Connector Sink的原理是封装Upsert(CDC变更所以必须设置为Upsert模式)的SQL语法，对于不同的Database Flink对于幂等的实现有不同方式。比如MySql是`INSERT .. ON DUPLICATE KEY UPDATE ..` ,PostgreSQL是`INSERT .. ON CONFLICT .. DO UPDATE SET ..`，我们知道Redshift是不支持PostgreSQL这个Upsert的语法的(Redshift有Merge语法)。当Flink检查到语法不兼容时，幂等的逻辑会变为执行SQL检查每条数据是否存在，如果不存在就Insert，存在执行Update，一旦退化到这个逻辑执行是比较低效的，此外Redshift最高效的更新逻辑是Merge的方式,实现Merge可以使用Merge语法，也可以Staging表做，核心都是将Update转换为Delete+Insert。所以对于Flink而言将CDC这种有更新的数据直接通过JDBC Sink到Redshift是不可行的。如果通过DataStream API自己封装逻辑，将数据Sink到S3再Merge的方式是最高效的，但是代码的开发是有挑战的，一些客户本身CDC的数据量不大，也会使用DataStream API自己JDBC封装Batch的Insert到Staging表然后Merge，此方法当CDC数据量不大时可行。但如果CDC数据量较大JDBC的Insert将会成为性能瓶颈。 

第二种模式，CDC工具解析Binlog将数据发送到MSK(Kafka),再用工具消费MSK中的数据写入到Redshift。此种方式下写入到Redshift的工具，目前最好的选择Spark, 因为我们有Spark Redshift Connector.这个Connector是开源的，AWS对Spark Redshift Connector在Gule和EMR环境里也有深度的优化。该Connector原理是将数据Sink到S3,然后Copy数据到Redshift，对于更新可以在写入Staging表后执行postactions在事务里封装Delete+Insert逻辑,此种方式的更新是非常高效的。Spark Sturctured Streaming设置Checkpoint Interval控制Batch延迟，可以做到端到端小于30s延迟(和Spark资源，同时并发同步表数据，Redshift资源相关)。

##### 1.3 CDC同步到Redshift要解决的问题

CDC数据同步到Redshif，我们会面临如下的挑战，以上两种模式技术上要解决的问题如下：

1. Full Load阶段的性能，在全量数据阶段，是向MySQL发送的Select查询，对于单个大表的加载应该做合理的分片，并行加载数据。对于多张表要并行执行全量加载，而不是串行执行。
2. 源端的Schema变更的支持程度，添加列，删除列是常规的变更，列类型的变更对于列存的引擎而言都是需要比较大的代价的，一般都不会支持，字符串类型定义的长度变更也是有些场景会遇到的。
3. CDC阶段的并行，我们知道Binlog的解析是单线程做的，但是对于解析的数据Sink到多张Redshift表是可以并行的，比如有30表同时同步到Redshift，并行可以降低多表的同步时间。
4. 数据顺序的保证，如果我们CDC数据Sink到的Kafka，Kafka可以保证单分区有序，但是多分区是无序的，对于CDC数据而言，我们要保证同一张表相同主键的变更数据发送到同一分区，如果单分区对于大表会成为性能瓶颈，所以我们要自定义分区Key来保证相同主键Sink到同一分区，这样的做的还可以有两个优势，第一，单线程的Binlog解析的下游算子可以重新设定并行度，当我们想在Binlog解析阶段增加逻辑，可以多并行度处理提升性能。第二，多个表的数据Sink到同一个Kafka的Topic时，可以多分区保证性能，这对于一些客户有上千张表同步时，非常关键。
5. 数据Sink到Redshift前合并优化，如果相同主键的数据在一个时间段内有大量的并更，比如同一条数据在10s内做了20次的Update。我们没有必要将这20条变更应用到Redshift，Sink之前我们只需要按照时间做开窗函数取最后一次的Update就应用到Redshif就可以。这种方式的处理在变更频繁的场景下可以很好的提升同步性能。

| 同步模式                      | 架构 | 灵活性             | 开放性               | 监控             | Full  Load性能 | Schema变更                     | CDC并行          | 合并优化       | 数据回溯                    | 延迟   | 成本 |
| ----------------------------- | ---- | ------------------ | -------------------- | ---------------- | -------------- | ------------------------------ | ---------------- | -------------- | --------------------------- | ------ | ---- |
| DMS->Redshift                 | 简单 | 稍差               | 内部封装，用户不可见 | 完善的监控体系   | 大表稍差       | 增加，删除列                   | 可以(1~32并行度) | BatchApply支持 | 创建新Task指定Binlog位点    | 秒级别 | 低   |
| CDC工具->MSK->Spark->Redshift | 复杂 | 上下游解构，较灵活 | 开放代码，自主可控   | 无，需要自己构建 | 自己控制并行度 | 增加，删除列，字符串列长度变更 | 自定义           | 开窗函数支持   | 指定Kafka消费位置或时间即可 | 秒级别 | 稍高 |

对于以上两种模式，当需要更高的自主可控性，以及CDC数据要下游多端复用时，建议选择MSK解耦上下游的模型。

#### 二、架构方案

我们会重点说明MSK解耦模式的架构方案及部署实施。对于DMS直接Sink到Redshift在AWS Console配置即可，会在下面小结中说明使用时的注意事项。

##### 2.1 架构图

![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202305041258640.png)

##### 2.2 架构说明

* 三种方式支持MySQL CDC数据实时解析发送到Kafka。DMS和MSK Connector不用写代码，AWS Console上配置一下就可以。KDA Flink CDC需要写代码实现，[已经写好点击链接](https://github.com/yhyyz/flink-cdc-msk)和MSK Connector可以配置单表单topic，也可以多表单topic. KDA Flink CDC
  目前支持所有表单topic,按库划分topic. 由于都实现了主键作为partition key, topic都可以设置多分区
* Spark Streaming消费Kafka CDC数据多线程Sink多张表到Redshift，Kafka解耦上下游，链路更稳定，数据回溯更方便。[代码已写好点击链接](https://github.com/yhyyz/kafka-cdc-redshift)
* 多线程并行写多表, 支持insert, update，delete，秒级别延迟，更新删除逻辑比较轻，使用Staging转换UPDATE = DELETE+INSERT
* Schema自动变更支持，增加列，删除列，修改字符串列的长度,使用from_json动态生成每批次的DataFrame的Schema,以DataFrame的Schema为标准更新Redshift的Schema。Full Load阶段可以根据数据量调整资源快速加载
* 三种CDC方式均支持按照MySQL主键值作为Kafka Partition Key,保证CDC顺序。DMS和MSK Connector本身提供支持。KDA Flink CDC代码里通过自定义Kafka Partition实现，由于做主键分区，所以可以在CDC阶段Rebalance，下游算子不在需要设置单并行度保序，保证高性能
* Spark Structured Streaming消费数据对单批次数据做了开窗处理，相同记录的多次更新在同一批次过来，可以做到数据合并为单条，这对于更新频繁的场景有很好性能提升，减小了Sink到下游的Redshift的压力。
* 延迟可以做到秒级别，压测50线程50表并行写(包含CIUD操作)，端到端数据延迟40~60秒，50线程100张表90~120秒. 延迟和数据量，Streaming作业的资源及Redshift的集群资源相关，可在自己环境测试看延迟，可以调整Checkpoint Interval调整Streaming作业每批次间隔时间，一般30秒之内延迟没有问题。
* 对于Mongodb,Postgres,Sqlserver,Oracle,TiDB,Db2,Oceanbase Flink CDC都是支持的，但是当前Sink到Redshift的代码中只适配了MySQL的CDC Format,对于其它数据库，如果有需要可以参看代码，自己加入逻辑，比较简单。
* 支持忽略DDL模式,用户自己控制建表和控制Schema变更,如果设置之后，不再自动创建表，用户自己预先创建表，同样不会自动添加删除列，源端变更需要用户控制
* 支持delete数据单独写到一张表，表名自动以_delete结尾, 支持只同步delete数据，不同步原表数据，表名字自动以_delete结尾
* Spark根据消费的CDC DataFrame在Redshfit自动创建表结构，维护Schema变更，需要注意的是自动创建的是根据CDC的Json种的数据类型映射的，而非MySQL表中的元数据类型，表中的列的顺序是按照字母顺序排序的。如果想自己创建表，指定类型和字段顺序也是支持的，开启忽略DDL模式即可

##### 2.3 DMS使用时的注意事项

如果使用DMS直接CDC Sink到Redshift时，我们在配置时需要注意如下事项

* DMS实例的大小影响同步的性能，对于生产环境中要同步的表比较多，表的数据量较大，变更较多时，非常建议直接选择大类型的实例。大类型实例在Full Load阶段能够提升性能。

* DMS当遇到不支持的源端的Schema变更，比如修改类类型，或者修改字符类型长度时，DMS任务会失败,你需要手动调整Redshift表的列，让其和源端匹配，再重新Resume作业。

* 如果DMS同步过程中某张表处于Suspend状态，你可以开启Debug日志，然后再DMS控制台，可以单独Reload这张表，查看错误原因，再对应解决，主要Reload这张表时，会重新拉取该表的数据。

* DMS任务在创建的时候如果是Full Load+ongoing replication模式，是不能直接从指定的Binlog位点重新启动作业的，需要新建一个ongoing replication的Task，这时可以从指定的MySQL Binlog位点启动作业，也可以从上个作业的Checkpoint启动作业

* 关于DMS如何启动失败的作业，以及如何从Binlog位点及checkpoint恢复作业，下面以API操作举例，也可以在AWS控制台点击配置。当指定位点恢复作业时，只要指定的位点比当前的位点早就可以，不用担心Redshift数据重复，因为在Redshift端数据是会被更新的，会保证最终一致性。有时你可能不方便找到之前消费的Binlog是哪个，DMS提供了From a custom CDC start time通过指定一个timestamp(UTC)来标识从哪个位置消费数据，同样不需要十分准确的时间，只要比当前停止的时间早一些就可以。

  ```shell
  # 对于 Full load, ongoing replication类型作业，使用api可以从上次停止的的位点恢复，console上的resume也可以， 但此类型作业不可以从任意指定的chepoint点或者native LSN 恢复作业。因此对于此类型的作业，如果想要用新的Task启动作业，可以新启动一个cdc类型作业
  # resume-processing 和console上resume等同。 reload-target和console上的restart等同。 此类型作业 start-replication 只能首次启动作业才可以用
  aws dms start-replication-task \
      --replication-task-arn arn:aws:dms:ap-southeast-1:xxx:task:xxx \
      --start-replication-task-type resume-processing \
      
  # 指定位点，from native LSN 如何果获取这个位点，在下方查看mysql binlog位点说明
  aws dms start-replication-task \
      --replication-task-arn arn:aws:dms:ap-southeast-1:xxx:task:xxx \
      --start-replication-task-type start-replication  \
      --cdc-start-position mysql-bin-changelog.038818:568
      
  # 指定checkpoint from checkpoint 
  aws dms start-replication-task \
      --replication-task-arn arn:aws:dms:ap-southeast-1:xxx:task:xxx \
      --start-replication-task-type start-replication  \
      --cdc-start-position checkpoint:V1#264#mysql-bin-changelog.039022:2167:-1:2198:167598213826489:mysql-bin-changelog.039022:2052#0#0#*#0#825
  
  # 从上一次失败恢复
  aws dms start-replication-task \
      --replication-task-arn arn:aws:dms:ap-southeast-1:xxx:task:xxx \
     --start-replication-task-type resume-processing
  
  # 根据情况，binlog保留时间，防止binlog删除，cdc恢复数据可能找不到binlog位点
  call mysql.rds_show_configuration;
  call mysql.rds_set_configuration('binlog retention hours', 72);
  # cdc 需要mysql 设置binlog_format为ROW
  
  # 查看mysql binlog 位点
  show master status;
  show binary logs;
  show binlog events in 'mysql-bin-changelog.xxxx';
  ```
  
* DMS作业一定要加上监控报警，比较重要的磁盘使用率的指标，同步延迟的指标，表同步状态的指标，一旦出问题我们可以及时发现并修复。建议创建DMS TASK时开启DMS自身日志表（**awsdms_status**，**awsdms_suspended_tables**,**dmslogs.awsdms_history**）信息的同步到Redshift，以便我们定位异常问题

* 如果表中有大量的Lob字段，DMS对于Lob类字段的处理性能不是特别高，它的逻辑是先将非Lob类型的列，同步到Redshift，Lob列此时为空，然后单独执行Update语句更新Lob列。同时在同步Lob和Text字段时，不建议开启ParallelApplyThreads参数。

* DMS写入到Redshif时，常用的参数调整,一般保持默认即可，如果遇到性能问题，再对应调整参数

  ```markdown
  BatchApplyEnabled=true 这个默认就是True
  BatchSplitSize=0 0表示没有限制, 设为0即可
  BatchApplyTimeoutMax=30  等待的最小值，默认是1s
  BatchApplyTimeoutMin=60  等待的最大值，默认是30s
  ParallelApplyThreads=32  默认值是0，调整为32,表中有Lob和Text列，不建议调整，保持默认值
  ParallelApplyBufferSize=1000 每个buffer queue中最大记录数，默认是100，调整为1000
  BatchApplyMemoryLimit=1500 默认值500MB,MaxFileSize(默认32MB)*ParallelApplyThreads=32*32=1024MB,调成1500
  MemoryLimitTotal=6144 默认值1024MB,事务在内存中超过这个值开始写磁盘
  MemoryKeepTime=180   默认值60s, 事务在内存中超过这个时间没有写到target开始写磁盘
  ```
  

#### 三、部署实施

对于CDC Sink到MSK这里使用两种方式给大家做个实例，第一个方式是Flink CDC的方式，Flink作业部署到KDA(Kinesis Data Analytics)中.第二个是MSK Connector配置Sink到MSK. 对于DMS发送到MSK，AWS Console上配置即可，只会说明注意事项。对于消费CDC数据Sink到Redshift，会将Spark Structured Streaming程序部署到Glue中。

##### 3.1 Flink CDC Sink MSK

* 代码支持说明

  ```markdown
  1. KDA Flink(Flink版本 1.15)
  2. Flink CDC DataStream API解析MySQL Binlog发送到Kafka，支持按库发送到不同Topic, 也可以发送到同一个Topic
  3. 自定义FlinkKafkaPartitioner, 数据库名，表名，主键值三个拼接作为partition key, 保证相同主键的记录发送到Kafka的同一个分区，保证消息顺序。
  4. Flink CDC支持增量快照算法，全局无锁，Batch阶段的checkpoint, 但需要表有主键，如果没有主键列增量快照算法就不可用，无法同步数据，需要设置scan.incremental.snapshot.enabled=false禁用增量快照
  5. 当前只加入了MySQL的支持，如需其它数据库，可以自行修改
  ```

* 准备JAR包

  ```shell
  # 代码地址： https://github.com/yhyyz/flink-cdc-msk 
  # 使用编译好的JAR
  wget https://dxs9dnjebzm6y.cloudfront.net/tmp/flink-cdc-msk-1.0-SNAPSHOT-202304101313.jar
  # 上传到S3
  aws s3 cp flink-cdc-msk-1.0-SNAPSHOT-202304101313.jar s3://xxxx/jars/
  # 自己编译
  git clone https://github.com/yhyyz/flink-cdc-msk
  mvn clean package -Dscope.type=provided
  ```

* AWS Console或者AWS cli创建KDA应用，填写JAR包地址和相关参数说明

  ```properties
  # local表示本地运行，prod表示KDA运行
  project_env local or prod
  # 是否禁用flink operator chaining 
  disable_chaining false or true 
  # kafka的投递语义at_least_once or exactly_once,建议at_least_once，不用担心Redshift写入重复因为已经做了幂等
  delivery_guarantee at_least_once
  # mysql 地址
  host localhost:3306
  # mysql 用户名
  username xxx
  # mysql 密码
  password xxx
  # 需要同步的数据库，支持正则，多个可以逗号分隔
  db_list test_db,cdc_db_02
  # 需要同步的表 支持正则，多个可以逗号分隔
  tb_list test_db.product.*,test_db.user.*,cdc_db_02.sbt.*
  # 在快照读取之前，Source 不需要数据库锁权限。每个并行Readers都应该具有唯一的 Server id，所以 Server id 必须是类似 `5400-6400` 的范围，并且该范围必须大于并行度。
  server_id 10000-10010
  # mysql 时区
  server_time_zone Etc/GMT
  # latest从当前CDC开始同步，initial先快照再CD
  position latest or initial
  # kafka 地址
  kafka_broker localhost:9092
  # topic 名称, 如果所有的数据都发送到同一个topic,设定要发送的topic名称
  topic test-cdc-1
   # 如果按照数据库划分topic,不同的数据库中表发送到不同topic,可以设定topic前缀，topic名称会被设定为 前缀+数据库名。 设定了-topic_prefix参数后，topic参数不再生效
  topic_prefix flink_cdc_
  # 需要同步的表的主键,这次表的正则
  table_pk [{"db":"test_db","table":"product","primary_key":"pid"},{"db":"test_db","table":"product_01","primary_key":"pid"},{"db":"cdc_db_02","table":"sbt.*","primary_key":"id"}]
  ```

  ![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202304281515823.png)

* 查看运行状态

  ![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202304281532089.png)

##### 3.2 MSK Connector CDC发送到MSK

* 创建Debezium Mysql Plugin

  ```shell
  # debezium mysql plugin
  wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/debezium/debezium-connector-mysql/versions/1.9.7/debezium-debezium-connector-mysql-1.9.7.zip
  # 上传到S3
  aws s3 cp debezium-debezium-connector-mysql-1.9.7.zip s3://xxxx/msk-connector-plugin/
  ```
  
  ![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202304281544573.png)
  
* 创建Worker配置

  ```properties
  key.converter=org.apache.kafka.connect.storage.StringConverter
  key.converter.schemas.enable=false
  value.converter=org.apache.kafka.connect.json.JsonConverter
  value.converter.schemas.enable=false
  ```
  
  ![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202304281547981.png)

* 创建Connector并启动

  ```properties
  connector.class=io.debezium.connector.mysql.MySqlConnector
  database.user=admin
  database.server.id=11000
  tasks.max=1
  database.server.name=test_server
  database.hostname=xxxxxx
  database.port=3306
  database.password=xxxx
  database.include.list=test_db
  topic.prefix=msk_cdc_
  database.history.kafka.topic=test_db
  database.history.kafka.bootstrap.servers=xxxx:9092,xxxx:9092
  include.schema.changes=true
  decimal.handling.mode=string
  time.precision.mode=adaptive_time_microseconds
  database.connectionTimeZone=UTC
  
  
  # 注意Debezium2.x 和1.x版本，参数是有差异的，这里使用的是1.9.x
  # 默认主键作为partition key,可以通过 message.key.columns 设置
  # 格式：https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-property-message-key-columns
  # inventory.customers:pk1,pk2;
  # (.*).purchaseorders:pk3,pk4
  ```
  

##### 3.3 DMS CDC 发送到MSK

DMA控制台直接配置Endpoint和Task即可，比较简单，不在赘述。需要注意的是创建Endpoint是指定参数

```properties
MessageFormat: json-unformatted
IncludePartitionValue: true
PartitionIncludeSchemaTable: true
NoHexPrefix: NoHexPrefix
IncludeNullAndEmpty: true
# endpoint 参数说明看这里 https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-dms-endpoint-kafkasettings.html
```

##### 3.4 验证MSK CDC数据

```shell
# 根据自己kafka版本选择
wget https://archive.apache.org/dist/kafka/2.8.2/kafka_2.12-2.8.2.tgz
tar -xvzf /kafka_2.12-2.8.2.tgz && cd kafka_2.12-2.8.2
export bs="xxxxx.kafka.us-east-1.amazonaws.com:9092"
export topic="xxx"
# 从头消费数据
./bin/kafka-console-consumer.sh --bootstrap-server $bs --topic $topic --from-beginning | jq .
# 从头消费数据，key也输出
./bin/kafka-console-consumer.sh --bootstrap-server $bs --topic $topic --from-beginning --property print.key=true
# cdc 格式样例
https://github.com/yhyyz/kafka-cdc-redshift#mysql-cdc%E6%A0%BC%E5%BC%8F%E6%A0%B7%E4%BE%8B
```

##### 3.5 消费MSK数据写入到Redshift

该程序使用Spark Structred Streaming实现。在AWS上部署有三个选择`Glue , EMR Serverless，EMR on EC2，EMR on EKS`，代码是兼容的, 这里以Glue部署作为样例.

* 下载依赖

  ```shell
  # 下载依赖的JAR, 上传到S3
  # spark redshift connector，对开源做了点修改，开源默认将String类型映射为了Text默认长度是256，调整为了varchar(65535). 代码地址，https://github.com/yhyyz/emr-spark-redshift。 可以使用如下编译好的
  wget https://dxs9dnjebzm6y.cloudfront.net/tmp/emr-spark-redshift-1.0-SNAPSHOT.jar
  # 可以将offset通过listener提交到Kafka方便Kafka测监控消费
  wget https://dxs9dnjebzm6y.cloudfront.net/tmp/spark-sql-kafka-offset-committer-1.0.jar
  # clone代码，cdc_util build成whl,方便再在多个环境中使用,直接执行如下命令build 或者下载build好的
  # 编译好的
  https://dxs9dnjebzm6y.cloudfront.net/tmp/cdc_util_202304251252-1.1-py3-none-any.whl
  # 自己编译
  git clone https://github.com/yhyyz/kafka-cdc-redshift
  python3 -m venv cdc_venv
  source ./cdc_venv/bin/activate
  python3 setup.py bdist_wheel
  ```

* 创建job配置参数

  因为需要传递给JOB参数较多，将其定义到了一个文件中，启动作业会解析该配置文件，可以使用该https://github.com/yhyyz/kafka-cdc-redshift/blob/main/config/job-4x.properties文件作为参数配置。GitHub上README注意看，里面会有各种配置的更新,以及支持的特性。

  ```properties
  # for glue job，请仔细看参数说明
  aws_region = us-east-1
  s3_endpoint = s3.us-east-1.amazonaws.com
  # spark checkpoint 目前，作业重启是会自动从checkpoint位置消费
  checkpoint_location = s3://xxxx/checkpoint/
  # 每批次的消费间隔，控制者最小的延迟，建议值10s~60s之间
  checkpoint_interval = 30 seconds
  # kafka 地址
  kafka_broker = xxxx1.us-east-1.amazonaws.com:9092,xxxx2.us-east-1.amazonaws.com:9092
  # 消费CDC数据的Kafka的topic，多个以逗号分隔
  topic = flink_mysql_cdc
  # 启始的消费位置，支持latest earliest和指定时间戳例如: 1682682682000. 需要注意的是此参数只在第一此启动作业时生效，当作业重启时会从checkpoint启动，如果重启时生效，需要删除checkpoint目录再启动。Spark作业如果有checkpoint会优先从checkpoint启动，如果没有才会从该参数指定的位置启动。
  startingOffsets = latest
  # 并发写的线程数，如果要50张表同时写就配置成50，更多并发所有表的同步延迟就会更短，消耗的资源就会更多，不建议超过50
  thread_max_workers = 30
  # 是否启动Debug日志，启动Debug会影响性能，主要用来排查错误
  disable_msg = true
  # CDC的格式，目前支持三个值，FLINK-CDC or DMS-CDC or MSK-DEBEZIUM-CDC
  cdc_format = FLINK-CDC
  # 每个批次最大从Kafka拉取多少条数据，当回溯数据时，防止一个batch消费太多数据，资源开的不够，Spark内存溢出
  max_offsets_per_trigger = 1000000
  # 消费者组
  consumer_group = cdc-redshift-glue-g1
  # Redshift 链接信息配置
  # 如果用secret manager管理链接，填secret_id一个就可以
  redshift_secret_id =
  redshift_host = xxxx.us-east-1.redshift.amazonaws.com
  redshift_port = 5439
  redshift_username = xxx
  redshift_password = xxx
  redshift_database = dev
  redshift_schema = public
  # 数据写S3的临时路径
  redshift_tmpdir = s3://xxxx/glue-cdc/tmpdir/
  # redshift关联的iam role，需要有S3权限
  redshift_iam_role = arn:aws:iam::xxxx:role/admin-role-panchao
  
  # 同步表的配置
  sync_table_list = [\
  {"db": "test_db", "table": "product", "primary_key": "pid"},\
  {"db": "test_db", "table": "user", "primary_key": "id"},\
  {"db": "test_db", "table": "product_02", "primary_key": "pid"},\
  {"db": "test_db", "table": "product_multiple_key_01", "primary_key": "pid,pname"},\
  {"db": "cdc_db_02", "table": "sbtest1", "primary_key": "id"},\
  {"db": "cdc_db_02", "table": "sbtest2", "primary_key": "id"},\
  {"db": "cdc_db_02", "table": "sbtest3", "primary_key": "id"},\
  ....
  {"db": "cdc_db_02", "table": "sbtest100", "primary_key": "id"}
  ]
  
  # 其它同步方式的配置例子
  # save_delete设置为true，表示同步原表同时，将delete数据单独写一张表
  # only_save_delete设置为true,表示只同步delete数据，不同步原表数据
  sync_table_list = [\
  {"db": "test_db", "table": "product", "primary_key": "pid","ignore_ddl":"true","save_delete":"true"},\
  {"db": "test_db", "table": "user", "primary_key": "id","only_save_delete":"true"}\
  ]
  
  # ignore_ddl，忽略ddl变更，表需要用户自己创建，创建表名如果不配置，请用源端的表名字创建redshift表
  # target_table 配置在Redshift创建表名称
  sync_table_list = [\
  {"db": "test_db", "table": "product", "primary_key": "pid","ignore_ddl":"true"},\
  {"db": "test_db", "table": "product", "primary_key": "pid","ignore_ddl":"true","target_table":"t_product"},\
  {"db": "test_db", "table": "user", "primary_key": "id"}\
  ]
  ```

  

* 创建Glue Job配置如下

  ```properties
  --extra-jars s3://xxxx/jars/emr-spark-redshift-1.0-SNAPSHOT.jar,s3://xxxxx/tmp/spark-sql-kafka-offset-committer-1.0.jar
  --additional-python-modules  redshift_connector,jproperties,s3://xxxx/tmp/cdc_util-1.1-py3-none-any.whl
  --aws_region us-east-1
  # 注意这个参数 --conf 直接写后边内容，spark.executor.cores 调成了8，表示一个worker可以同时运行的task是8
  # --conf spark.sql.shuffle.partitions=1  --conf spark.default.parallelism=1 设置为1，这是为了降低并行度，保证当多个线程同时写多张表时，都尽可能有资源执行，设置为1时，最终生产的数据文件也是1个，如果数据量很大，生产的一个文件可能会比较大，比如500MB，这样redshift copy花费的时间就会长一些，如果想要加速，就把这两个值调大一些，比如4，这样就会生产4个125M的文件，Redshift并行copy就会快一些，但Glue作业的资源对应就要设置多一些，可以观察执行速度评估
  --conf spark.sql.streaming.streamingQueryListeners=net.heartsavior.spark.KafkaOffsetCommitterListener  --conf spark.executor.cores=8 --conf spark.sql.shuffle.partitions=1  --conf spark.default.parallelism=1 --conf spark.speculation=false --conf spark.dynamicAllocation.enabled=false
  --config_s3_path  s3://xxxx/kafka-cdc-redshift/job-4x.properties
  # Glue 选择3.x,作业类型选择Spark Streaming作业，worker个数根据同步表的数量和大小选择，Number of retries 在Streaming作业下可以设置大些，比如100。 失败自动重启，且会从checkpoint自动重启。 
  ```

  ![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202304281630244.png)

* 启动作业运行即可

  ![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202304281635599.png)

##### 3.6 Redshift中查看数据

上述配置我并行同步了MySQL中50张表到Redshift,使用Sysbench压测MySQL保护(insert,update,delete)操作。

![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202304281648452.png)

##### 3.5 监控

监控是非常重要的，Kafka的Offset延迟，Glue作业的运行状态，同步到Redshift表的数据和元表的数据的一致性检查。这些重要的指标都需要监控报警出来，当前需要将这些监控纳入到自己的监控体系中。最简单的方式是CloudWatch拿到Kafka，Glue的监控指标定义报警规则。对于数据一致性的校验，可以写一个脚本，执行双端SQL对比检查，这是相对简单高效的方式。

#### 四、总结

本篇文章介绍了多库多表实时同步到Redshift的方案选择及各自方案的适用场景。最简单的方式是使用DMS同步到Redshift，当需要更高的自主可控性，以及CDC数据要下游多端复用时，可以选择CDC到MSK的上下游解耦的方案。这两种方式同样可以结合使用，当遇到DMS在某些场景不能满足需求时，比如包含Lob列的表同步遇到性能瓶颈时，或者某些过亿的表Flull Load阶段时间较长时，可以考虑两者结合，这部分表可以使用Spark Structured Streaming的方式做同步。
