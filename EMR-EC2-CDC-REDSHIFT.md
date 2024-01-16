[toc]

#### EMR EC2 canal cdc redshift

```
EMR EC2 6.11.0
```

##### 1. 导入环境变量

```shell
export APP_S3_BUCKET='s3://xxxxx'
export AWS_REGION="us-east-1"
export APP_NAME='emr-ec2-cdc-redshift'
export APP_LOCAL_HOME="/home/ec2-user/$APP_NAME"

export CDC_UTIL_WHL_NAME="cdc_util_202401161811-1.1-py3-none-any.whl"
export CDC_UTIL_WHL_LOCATION="https://dxs9dnjebzm6y.cloudfront.net/tmp/$CDC_UTIL_WHL_NAME"

export APP_S3_HOME="$APP_S3_BUCKET/$APP_NAME"
export MAIN_SCRIPT_PATH="$APP_S3_HOME/script/cdc_redshift.py"
export JOB_CONFIG_PATH="$APP_S3_HOME/script/job-ec2-canal.properties"
export PYTHON_VENV_PATH="$APP_S3_HOME/venv/cdc_venv.tar.gz"
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

##### 5. ec2 master spark sumbit

```shell
export APP_S3_BUCKET='s3://xxxxx'
export AWS_REGION="us-east-1"
export APP_NAME='emr-ec2-cdc-redshift'

export APP_S3_HOME="$APP_S3_BUCKET/$APP_NAME"
export MAIN_SCRIPT_PATH="$APP_S3_HOME/script/cdc_redshift.py"
export JOB_CONFIG_PATH="$APP_S3_HOME/script/job-ec2-canal.properties"
export PYTHON_VENV_PATH="$APP_S3_HOME/venv/cdc_venv.tar.gz"

# cluster mode
spark-submit --master yarn --deploy-mode cluster \
--conf spark.sql.streaming.streamingQueryListeners=net.heartsavior.spark.KafkaOffsetCommitterListener \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=4g \
--conf spark.driver.cores=2 \
--conf spark.driver.memory=4g \
--conf spark.executor.instances=2 \
--conf spark.sql.shuffle.partitions=2  \
--conf spark.default.parallelism=2 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.jars=$(aws s3 ls $APP_S3_HOME/jars/ | grep -o '\S*\.jar$'| awk '{print "'"$APP_S3_HOME/jars/"'"$1","}' | tr -d '\n' | sed 's/,$//'),/usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-redshift.jar \
--conf spark.yarn.dist.archives=${PYTHON_VENV_PATH}\#environment \
--conf spark.pyspark.python=./environment/bin/python \
$MAIN_SCRIPT_PATH $AWS_REGION $JOB_CONFIG_PATH 


# client mode 不建议，因为client模式driver在提交机器的节点，那python的venv也必须在这个本地节点上，然后加上--conf spark.pyspark.driver.python=./environment/bin/python  这个参数

spark-submit --master client --deploy-mode cluster \
--conf spark.sql.streaming.streamingQueryListeners=net.heartsavior.spark.KafkaOffsetCommitterListener \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=4g \
--conf spark.driver.cores=2 \
--conf spark.driver.memory=4g \
--conf spark.executor.instances=2 \
--conf spark.sql.shuffle.partitions=2  \
--conf spark.default.parallelism=2 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.jars=$(aws s3 ls $APP_S3_HOME/jars/ | grep -o '\S*\.jar$'| awk '{print "'"$APP_S3_HOME/jars/"'"$1","}' | tr -d '\n' | sed 's/,$//'),/usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-redshift.jar \
--conf spark.yarn.dist.archives=${PYTHON_VENV_PATH}\#environment \
--conf spark.pyspark.driver.python=./environment/bin/python \
--conf spark.pyspark.python=./environment/bin/python \
$MAIN_SCRIPT_PATH $AWS_REGION $JOB_CONFIG_PATH 
```

