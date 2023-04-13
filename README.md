## MSK(kafka) cdc redshift
Spark Streaming从Kafka中消费Flink CDC数据，多库多表实时同步到Redshift.当前支持
* Glue Streaming Job
* EMR Serverless Streaming Job

#### fix history
* 20230419 修复指定Redshift非public之外的schema是，执行SQL时没有set search_path造成的异常

#### Glue Streaming
* 下载依赖
```shell
# 下载依赖的JAR, 上传到S3
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/emr-spark-redshift-1.0-SNAPSHOT.jar
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/spark-sql-kafka-offset-committer-1.0.jar
# cdc_util build成whl,方便再在多个环境中使用,直接执行如下命令build 或者下载build好的
python3 setup.py bdist_wheel
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/cdc_util-1.1-py3-202304140009.whl
# 作业运行需要的配置文件放到了在项目的config下，可以参考job-4x.properties，将文件上传到S3,后边配置Glue作业用


```
* Glue job配置
```shell
--extra-jars s3://panchao-data/jars/emr-spark-redshift-1.0-SNAPSHOT.jar,s3://panchao-data/tmp/spark-sql-kafka-offset-committer-1.0.jar
--additional-python-modules  redshift_connector,jproperties,s3://panchao-data/tmp/cdc_util-1.1-py3-none-any.whl
--aws_region us-east-1
# 注意这个参数 --conf 直接写后边内容，spark.executor.cores 调成了8，表示一个worker可以同时运行的task是8
--conf  spark.sql.streaming.streamingQueryListeners=net.heartsavior.spark.KafkaOffsetCommitterListener  --conf spark.executor.cores=8 --conf spark.sql.shuffle.partitions=1  --conf spark.default.parallelism=1 --conf spark.speculation=false
--config_s3_path  s3://panchao-data/kafka-cdc-redshift/job-4x.properties

# Glue 选择3.x,作业类型选择Spark Streaming作业，worker个数根据同步表的数量和大小选择，Number of retries 可以设置大些。 失败自动重启，且会从checkpoint自动重启

```


#### EMR Serverless
* python lib venv
```shell
# python lib
python3 -m venv cdc_venv

source cdc_venv/bin/activate
pip3 install --upgrade pip
pip3 install redshift_connector jproperties
# cdc_util是封装好的Spark CDC Redshift 的包，源代码在cdc_util中
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/cdc_util-1.1-py3-none-any.whl
pip3 install cdc_util-1.1-py3-none-any.whl

pip3 install venv-pack
venv-pack -f -o cdc_venv.tar.gz

# 上传到S3
aws s3 cp cdc_venv.tar.gz s3://panchao-data/cdc/
```
* submit job
```shell
# https://dxs9dnjebzm6y.cloudfront.net/tmp/spark-sql-kafka-offsert-commiter-1.0.jar
app_id=00f8frvjd84ve709
role_arn=
script_path=s3://panchao-data/serverless-script/cdc_redshift.py
config_path=s3://panchao-data/kafka-cdc-redshift/job.properties
aws emr-serverless start-job-run \
    --region  us-east-1 \
    --name cdc_redshift \
    --application-id 00f8nmbb3mgik909 \
    --execution-role-arn arn:aws:iam::946277762357:role/admin-role-panchao \
    --execution-timeout-minutes 0 \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://panchao-data/serverless-script/cdc_redshift.py",
            "entryPointArguments": ["us-east-1","s3://panchao-data/kafka-cdc-redshift/job.properties"],
            "sparkSubmitParameters": "--conf spark.speculation=false  --conf spark.sql.streaming.streamingQueryListeners=net.heartsavior.spark.KafkaOffsetCommitterListener --conf spark.executor.cores=8 --conf spark.executor.memory=16g --conf spark.driver.cores=8 --conf spark.driver.memory=16g --conf spark.executor.instances=10 --conf spark.sql.shuffle.partitions=2  --conf spark.default.parallelism=2 --conf spark.dynamicAllocation.enabled=false --conf spark.emr-serverless.driver.disk=150G --conf spark.emr-serverless.executor.disk=150G --conf spark.jars=s3://panchao-data/tmp/spark-sql-kafka-offsert-commiter-1.0.jar,s3://panchao-data/emr-serverless-cdc/jars/spark320/*.jar,/usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar,/usr/share/aws/redshift/spark-redshift/lib/minimal-json.jar --conf spark.archives=s3://panchao-data/cdc/cdc_venv.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
           "s3MonitoringConfiguration": {
             "logUri": "s3://panchao-data/emr-serverless/logs"
           }
        }
    }'
```
