[toc]

#### EMR Serveless canal cdc redshift

```
EMR Serveless 6.11.0
```

##### 1. 导入环境变量

```shell
export APP_S3_BUCKET='s3://xxxxx'
export AWS_REGION="us-east-1"
export EMR_SERVERLESS_APP_ID='00fcvtfb3pisut09'
export EMR_SERVERLESS_EXECUTION_ROLE_ARN='arn:aws:iam::xxxxx:role/admin-role-panchao'
export APP_NAME='emr-serverless-cdc-redshift'
export APP_LOCAL_HOME="/home/ec2-user/$APP_NAME"

export CDC_UTIL_WHL_NAME="cdc_util_202401161811-1.1-py3-none-any.whl"
export CDC_UTIL_WHL_LOCATION="https://dxs9dnjebzm6y.cloudfront.net/tmp/$CDC_UTIL_WHL_NAME"

export APP_S3_HOME="$APP_S3_BUCKET/$APP_NAME"
export MAIN_SCRIPT_PATH="$APP_S3_HOME/script/cdc_redshift.py"
export JOB_CONFIG_PATH="$APP_S3_HOME/script/job-ec2-canal.properties"
export PYTHON_VENV_PATH="$APP_S3_HOME/venv/cdc_venv.tar.gz"

s3://xxxxx/emr-serverless-cdc-redshift/venv/cdc_venv.tar.gz
```

##### 2. 打包python venv及依赖

```shell
# python 3.7+
mkdir -p $APP_LOCAL_HOME/venv/
cd $APP_LOCAL_HOME/venv/
deactivate
rm -rf ./cdc_venv
rm -rf ./cdc_venv.tar.gz

python3 -m venv cdc_venv
source cdc_venv/bin/activate
pip3 install --upgrade pip
pip3 install redshift_connector jproperties 
# cdc_util是封装好的Spark CDC Redshift的包，源代码在cdc_util中
wget $CDC_UTIL_WHL_LOCATION
pip3 install $CDC_UTIL_WHL_NAME

pip3 install venv-pack
venv-pack -f -o cdc_venv.tar.gz
# 上传到S3
aws s3 cp $APP_LOCAL_HOME/venv/cdc_venv.tar.gz $APP_S3_HOME/venv/
```

```shell
# 可以使用打包好的venv
mkdir -p $APP_LOCAL_HOME/venv/
cd $APP_LOCAL_HOME/venv/
rm -rf cdc_venv.tar.gz
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/cdc_venv.tar.gz
aws s3 cp $APP_LOCAL_HOME/venv/cdc_venv.tar.gz $APP_S3_HOME/venv/
```

##### 3. 依赖包

```shell
mkdir $APP_LOCAL_HOME/jars/
rm -rf $APP_LOCAL_HOME/jars/*
cd $APP_LOCAL_HOME/
aws s3 rm --recursive $APP_S3_HOME/jars/

wget -P ./jars  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.2/spark-sql-kafka-0-10_2.12-3.3.2.jar
wget -P ./jars  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.2/kafka-clients-2.8.2.jar
wget -P ./jars  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.2/spark-token-provider-kafka-0-10_2.12-3.3.2.jar
wget -P ./jars  https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# emr 6.10 之后emr or emr serverless 默认会在spark.executor/driver.extraClassPath中配置/usr/share/aws/redshift/spark-redshift/lib/* 加入自带的redshift connector。所以下面的emr-spark-redshift-1.3-SNAPSHOT.jar再emr serverless中并不生效，可以不使用，如果使用自定义的再emr serverless中要通过自定义image实现，emr on ec2中,需要在/etc/spark/conf/spark-defaults.conf中去掉依赖
#wget -P  ./jars https://dxs9dnjebzm6y.cloudfront.net/tmp/emr-spark-redshift-1.3-SNAPSHOT.jar

wget -P  ./jars https://dxs9dnjebzm6y.cloudfront.net/tmp/spark3.3-sql-kafka-offset-committer-1.0.jar

aws s3 sync $APP_LOCAL_HOME/jars ${APP_S3_HOME}/jars/
```

##### 4. 执行脚本及配置文件

```shell
cd $APP_LOCAL_HOME/
mkdir $APP_LOCAL_HOME/script/
rm -rf $APP_LOCAL_HOME/script/*
wget -P  ./script https://raw.githubusercontent.com/yhyyz/kafka-cdc-redshift/main/emr_ec2/cdc_redshift.py
aws s3 cp ./script/cdc_redshift.py $APP_S3_HOME/script/

wget -P ./script https://raw.githubusercontent.com/yhyyz/kafka-cdc-redshift/main/config/job-ec2-canal.properties
# 注意修改job-ec2-canal.properties配置
aws s3 cp ./script/job-ec2-canal.properties $APP_S3_HOME/script/
```

##### 5. 配置sumbit

```shell
cd $APP_LOCAL_HOME/
cat << EOF > $APP_LOCAL_HOME/start-job-run.json
{
    "name":"$APP_NAME",
    "applicationId":"$EMR_SERVERLESS_APP_ID",
    "executionRoleArn":"$EMR_SERVERLESS_EXECUTION_ROLE_ARN",
    "jobDriver":{
        "sparkSubmit":{
        "entryPoint":"$MAIN_SCRIPT_PATH",
        "entryPointArguments":[
        		"$AWS_REGION",
        		"$JOB_CONFIG_PATH"
        ],
         "sparkSubmitParameters":"--conf spark.sql.streaming.streamingQueryListeners=net.heartsavior.spark.KafkaOffsetCommitterListener --conf spark.executor.cores=16 --conf spark.executor.memory=16g --conf spark.driver.cores=8 --conf spark.driver.memory=16g --conf spark.executor.instances=10 --conf spark.sql.shuffle.partitions=2  --conf spark.default.parallelism=2 --conf spark.dynamicAllocation.enabled=false --conf spark.emr-serverless.driver.disk=150G --conf spark.emr-serverless.executor.disk=150G --conf spark.jars=$(aws s3 ls $APP_S3_HOME/jars/ | grep -o '\S*\.jar$'| awk '{print "'"$APP_S3_HOME/jars/"'"$1","}' | tr -d '\n' | sed 's/,$//'),/usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar --conf spark.archives=${PYTHON_VENV_PATH}#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        }
   },
   "configurationOverrides":{
        "monitoringConfiguration":{
            "s3MonitoringConfiguration":{
                "logUri":"$APP_S3_HOME/logs"
            }
        }
   }
}
EOF
jq . $APP_LOCAL_HOME/start-job-run.json
```

##### 6. 提交作业

```shell
export EMR_SERVERLESS_JOB_RUN_ID=$(aws emr-serverless start-job-run \
    --no-paginate --no-cli-pager --output text \
    --region $AWS_REGION \
    --name $APP_NAME \
    --application-id $EMR_SERVERLESS_APP_ID \
    --execution-role-arn $EMR_SERVERLESS_EXECUTION_ROLE_ARN \
    --execution-timeout-minutes 0 \
    --cli-input-json file://$APP_LOCAL_HOME/start-job-run.json \
    --query jobRunId)
```

##### 7. 监控作业

```shell
now=$(date +%s)sec
while true; do
    jobStatus=$(aws emr-serverless get-job-run \
                    --no-paginate --no-cli-pager --output text \
                    --application-id $EMR_SERVERLESS_APP_ID \
                    --job-run-id $EMR_SERVERLESS_JOB_RUN_ID \
                    --query jobRun.state)
    if [ "$jobStatus" = "PENDING" ] || [ "$jobStatus" = "SCHEDULED" ] || [ "$jobStatus" = "RUNNING" ]; then
        for i in {0..5}; do
            echo -ne "\E[33;5m>>> The job [ $EMR_SERVERLESS_JOB_RUN_ID ] state is [ $jobStatus ], duration [ $(date -u --date now-$now +%H:%M:%S) ] ....\r\E[0m"
            sleep 1
        done
    else
        echo -ne "The job [ $EMR_SERVERLESS_JOB_RUN_ID ] is [ $jobStatus ]\n\n"
        break
    fi
done
```

##### 8. 检查错误

```shell
JOB_LOG_HOME=$APP_LOCAL_HOME/log/$EMR_SERVERLESS_JOB_RUN_ID
rm -rf $JOB_LOG_HOME && mkdir -p $JOB_LOG_HOME
aws s3 cp --recursive $APP_S3_HOME/logs/applications/$EMR_SERVERLESS_APP_ID/jobs/$EMR_SERVERLESS_JOB_RUN_ID/ $JOB_LOG_HOME >& /dev/null
gzip -d -r -f $JOB_LOG_HOME >& /dev/null
grep --color=always -r -i -E 'error|failed|exception' $JOB_LOG_HOME
```

