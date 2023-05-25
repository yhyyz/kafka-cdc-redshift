from botocore.exceptions import ClientError
import boto3
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import col
import redshift_connector
from cdc_util.redshift_schema_evolution import SchemaEvolution
import json
from typing import Optional
import base64
import re


def gen_filter_udf(db, table):
    def filter_table(str_json, ):
        reg_schema = '"db":\\s*"{0}"'.format(db)
        reg_table = '"coll":\\s*"{0}"'.format(table)
        schema_pattern = re.compile(reg_schema)
        schema_res = schema_pattern.findall(str_json)
        table_pattern = re.compile(reg_table)
        table_res = table_pattern.findall(str_json)
        if schema_res and table_res:
            return True
        else:
            return False
        # return '"schema-name":"{0}"'.format(db) in str_json and '"table-name":"{0}"'.format(table) in str_json

    return udf(filter_table, BooleanType())


def gen_doc_id_udf():
    def gen_doc_id(str_json, ):
        pattern = r'"documentKey".*\\"_id\\":\s*([\d.]+|\{[^}]+\})'
        match = re.search(pattern, str_json)
        if match:
            content = match.group(1)
        else:
            content = "error_doc_key"
        return content

    return udf(gen_doc_id, StringType())


class MongoCDCRedshiftSink:
    def __init__(self, spark, redshift_schema, redshift_iam_role, redshift_tmpdir, logger=None,
                 disable_dataframe_show="false", host: Optional[str] = None, port: Optional[int] = None,
                 database: Optional[str] = None, user: Optional[str] = None,
                 password: Optional[str] = None, redshift_secret_id: Optional[str] = None,
                 region_name: Optional[str] = None, s3_endpoint: Optional[str] = None):
        if logger:
            self.logger = logger
        else:
            self.logger = print
        self.disable_dataframe_show = disable_dataframe_show
        self.data_frame = None
        self.spark = spark
        self.s3_endpoint = s3_endpoint

        self.redshift_tmpdir = redshift_tmpdir
        self.redshift_iam_role = redshift_iam_role

        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

        self.redshift_schema = redshift_schema

        self.redshift_secret_id = redshift_secret_id
        self.region_name = region_name

        if redshift_secret_id:
            secret_dict = json.loads(self._get_secret())
            self.con = redshift_connector.connect(
                host=secret_dict["host"],
                database=secret_dict["database"],
                user=secret_dict["username"],
                password=secret_dict["password"],
                port=int(secret_dict["port"])
            )
            self.host = secret_dict["host"]
            self.database = secret_dict["database"]
            self.user = secret_dict["username"]
            self.port = int(secret_dict["port"])
            self.password = secret_dict["password"]

        else:
            self.con = redshift_connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=int(self.port)
            )

    def _getDFExampleString(self, df):
        if self.disable_dataframe_show == "false":
            data_str = df._jdf.showString(5, 20, False)
            # truncate false
            # data_str = df._jdf.showString(5, int(false), False)
            schema_str = df._jdf.schema().treeString()
            return schema_str + "\n" + data_str
        else:
            return "(disable show dataframe)"

    def _run_sql_with_result(self, sql_str, schema):
        with self.con.cursor() as cursor:
            cursor.execute("set search_path to '$user', public, {0}".format(schema))
            cursor.execute(sql_str)
            res = cursor.fetchall()
            return res

    def _check_table_exists(self, table, schema):
        sql = "select distinct tablename from pg_table_def where tablename = '{0}' and schemaname='{1}'".format(table,
                                                                                                                schema)
        res = self._run_sql_with_result(sql, schema)
        if not res:
            return False
        else:
            return True

    def _get_cdc_sql_from_view(self, view_name):
        ""
        # row_number order by metadata.timestamp get top 1, Merge the same primary key data in a batch, reduce copying to redshift data
        iud_op_sql = "select * from (select doc_id, (CASE WHEN ISNULL(fullDocument)=false THEN fullDocument ELSE  '[]' END  ) AS doc, operationType as operation, to_date(from_unixtime(ts_ms / 1000), 'yyyy-MM-dd HH:mm:ss') as ts_date, cast((ts_ms / 1000) as timestamp) as ts_ms, row_number() over (partition by doc_id order by ts_ms desc) as seqnum  from {view_name} where (operationType='insert' or operationType='replace' or operationType='update' or operationType='delete') ) t1 where seqnum=1".format(
            view_name="global_temp." + view_name)
        return iud_op_sql

    def _get_on_sql(self, stage_table, target_table):
        join_key = "doc_id"
        tmp = "{stage_table}.{join_key} = {target_table}.{join_key}".format(stage_table=stage_table,
                                                                            target_table=target_table,
                                                                            join_key=join_key)
        return tmp

    def _do_write(self, scf, redshift_schema, table_name, target_table):
        if target_table:
            stage_table_name = redshift_schema + "." + "stage_table_" + target_table
            redshift_target_table = redshift_schema + "." + target_table
            redshift_target_table_without_schema = target_table
        else:
            stage_table_name = redshift_schema + "." + "stage_table_" + table_name
            redshift_target_table = redshift_schema + "." + table_name
            redshift_target_table_without_schema = table_name
        view_name = "kafka_source_" + table_name
        scf.createOrReplaceGlobalTempView(view_name)

        iud_op = self._get_cdc_sql_from_view(view_name)

        self.logger("iud operation(load,update,insert,delete) sql:" + iud_op)
        cols_to_drop = ['seqnum']
        iud_df = self.spark.sql(iud_op).drop(*cols_to_drop)
        # add super schema metadata
        super_column_list = ["doc"]
        fields = []
        for field in iud_df.schema.fields:
            if field.name in super_column_list:
                sf = StructField(field.name, field.dataType, field.nullable, metadata={"super": True})
            else:
                sf = StructField(field.name, field.dataType, field.nullable)
            fields.append(sf)
        schema_with_super_metadata = StructType(fields)

        iud_df = self.spark.createDataFrame(iud_df.rdd, schema_with_super_metadata)

        self.logger("stage table dataframe spark write to s3 {0}".format(self._getDFExampleString(iud_df)))

        iud_df_columns = iud_df.columns
        iud_df_columns.remove("operation")
        operation_del_value = "delete"

        on_sql = self._get_on_sql(stage_table_name, redshift_target_table)
        # if redshift target table already exists, do not create table
        create_target_table_sql = "create table  {target_table} sortkey (ts_date) as select {columns} from {stage_table} where 1=3;".format(
            stage_table=stage_table_name, target_table=redshift_target_table, columns=",".join(iud_df_columns))

        transaction_sql = "begin; delete from {target_table} using {stage_table} where {on_sql}; insert into {target_table}({columns}) select {columns} from {stage_table} where operation!='{operation_del_value}'; drop table {stage_table}; end;".format(
            stage_table=stage_table_name, target_table=redshift_target_table, on_sql=on_sql,
            columns=",".join(iud_df_columns), operation_del_value=operation_del_value)
        if self._check_table_exists(redshift_target_table_without_schema, redshift_schema):
            post_query = transaction_sql
        else:
            post_query = transaction_sql.replace("begin;", "begin; {0}".format(create_target_table_sql))

        self.logger("spark redshift jdbc transaction sql after copy stage table : " + post_query)
        iud_df.write \
            .format("io.github.spark_redshift_community.spark.redshift") \
            .option("url", "jdbc:redshift://{0}:{1}/{2}".format(self.host, self.port, self.database)) \
            .option("dbtable", stage_table_name) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("tempdir", self.redshift_tmpdir) \
            .option("postactions", post_query) \
            .option("tempformat", "CSV") \
            .option("s3_endpoint", self.s3_endpoint) \
            .option("extracopyoptions", "TRUNCATECOLUMNS region '{0}'".format(self.region_name)) \
            .option("aws_iam_role", self.redshift_iam_role).mode("append").save()

    def run_task(self, item, data_frame):
        task_status = {}
        try:
            self.logger("sync table info:" + str(item))
            db_name = item["db"]
            table_name = item["table"]
            target_table = ""
            if "target_table" in item:
                target_table = item["target_table"]
            # target_table = redshift_schema + "." + table_name
            task_status["table_name"] = table_name
            fdf = data_frame.filter(gen_filter_udf(db_name, table_name)(col('value')))
            df = fdf.withColumn("doc_id", gen_doc_id_udf()(col('value')))

            # self.logger("the table {0}: record number: {1}".format(table_name, str(fdf.count())))
            if not df.rdd.isEmpty():
                self.logger("the table {0}:  kafka source data: {1}".format(table_name, self._getDFExampleString(df)))
                # auto gen schema
                json_schema = self.spark.read.json(df.rdd.map(lambda p: str(p["value"]))).schema
                self.logger("the table {0}: auto gen json schema: {1}".format(table_name, str(json_schema)))
                scf = df.select(from_json(col("value"), json_schema).alias("kdata"), col("doc_id").alias("doc_id")).select("kdata.*", "doc_id")

                self.logger("the table {0}: kafka source data with auto gen schema: {1}".format(table_name,
                                                                                                self._getDFExampleString(
                                                                                                    scf)))

                self._do_write(scf, self.redshift_schema, table_name, target_table)
                self.logger("sync the table complete: " + table_name)
                task_status["status"] = "finished"
                return task_status
            else:
                task_status["status"] = "the table in the current batch has no data"
        except Exception as e:
            task_status["status"] = "error"
            task_status["exception"] = "{0}".format(e)
            self.logger(e)
            return task_status

    def _get_secret(self):
        secret_name = self.redshift_secret_id
        region_name = self.region_name
        self.logger(
            "get redshift conn from secrets manager,secret_id: {0} region_name: {1}".format(secret_name, region_name))
        # Create a Secrets Manager client

        session = boto3.session.Session(region_name=region_name)
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                return secret
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                return decoded_binary_secret
