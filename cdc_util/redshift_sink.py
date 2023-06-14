from botocore.exceptions import ClientError
import boto3
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import col,to_timestamp,to_date,date_add,expr
import redshift_connector
from cdc_util.redshift_schema_evolution import SchemaEvolution
import json
from typing import Optional
import base64
import re


def gen_filter_udf(db, table, cdc_format):
    def filter_table(str_json, ):
        reg_schema = ""
        reg_table = ""
        if cdc_format == "DMS-CDC":
            reg_schema = '"schema-name":"{0}"'.format(db)
            reg_table = '"table-name":"{0}"'.format(table)
        elif cdc_format == "FLINK-CDC" or cdc_format == "MSK-DEBEZIUM-CDC":
            reg_schema = '"db":"{0}"'.format(db)
            reg_table = '"table":"{0}"'.format(table)
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


def change_cdc_format_udf(cdc_format):
    def change_cdc_format(str_json, ):
        res_str_json = str_json
        if cdc_format == "FLINK-CDC" or cdc_format == "MSK-DEBEZIUM-CDC":
            match_op = re.search(r'"op":"(.*?)"', str_json)
            op_value = match_op.group(1)
            if op_value == "d":
                res_str_json = re.sub(r'"before":(.*?),"after":null',
                                      lambda
                                          match_data: f'"before":{match_data.group(1)},"after":{match_data.group(1)}',
                                      str_json)
        return res_str_json
    return udf(change_cdc_format, StringType())


class CDCRedshiftSink:
    def __init__(self, spark, cdc_format, redshift_schema, redshift_iam_role, redshift_tmpdir, logger=None,
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
        self.cdc_format = cdc_format
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
    def _getDFSchemaJsonString(self, df):
        if self.disable_dataframe_show == "false":
            schema_str = df.schema.json()
            return schema_str
        else:
            return "(disable show dataframe schema)"

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

    def _convert_date_or_timestamp(self, df, time_columns, convert_format):
        column_list = time_columns.split("|")
        if len(column_list) == 2:
            date_format = column_list[1]
        else:
            if convert_format == "date":
                date_format = "since_1970"
            else:
                date_format = "yyyy-MM-dd\'T\'HH:mm:ss\'Z\'"
        if len(column_list) == 0:
            return df
        columns = column_list[0].split(",")
        for column in columns:
            if convert_format == "date":
                if date_format == "since_1970":
                    df = df.withColumn(column, expr("date_add('1970-01-01',cast({0} as int))".format(column)))
                else:
                    df = df.withColumn(column, expr('to_date({0}, "{1}")'.format(column, date_format)))
            else:
                df = df.withColumn(column, expr('to_timestamp({0}, "{1}")'.format(column, date_format)))
        return df


    #	{"before":null,"after":{"pid":6,"pname":"pp6-name","pprice":"12.14","create_time":"2023-03-12T16:15:06Z","modify_time":"2023-03-12T16:15:06Z"},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"test_db","sequence":null,"table":"product_03","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null,"kafka_partition_key":"test_db.product_03.no_pk"},"op":"r","ts_ms":1678759544359,"transaction":null}
    def _get_cdc_sql_from_view(self, view_name, primary_key):
        # row_number order by metadata.timestamp get top 1, Merge the same primary key data in a batch, reduce copying to redshift data
        iud_op_sql = ""
        if self.cdc_format == "DMS-CDC":
            partition_key = ",".join(["data." + pk for pk in primary_key.split(",")])
            iud_op_sql = "select * from (select data.*, metadata.operation as operation, row_number() over (partition by {primary_key} order by metadata.timestamp desc) as seqnum  from {view_name} where (metadata.operation='load' or metadata.operation='delete' or metadata.operation='insert' or metadata.operation='update') and  metadata.`record-type`!='control' and metadata.`record-type`='data') t1 where seqnum=1".format(
                primary_key=partition_key, view_name="global_temp." + view_name)
        elif self.cdc_format == "FLINK-CDC" or self.cdc_format == "MSK-DEBEZIUM-CDC":
            partition_key = ",".join(["after." + pk for pk in primary_key.split(",")])
            iud_op_sql = "select * from (select after.*, op as operation, row_number() over (partition by {primary_key} order by ts_ms desc) as seqnum  from {view_name} where (op='u' or op='d' or op='c' or op='r') ) t1 where seqnum=1".format(
                primary_key=partition_key, view_name="global_temp." + view_name)

        return iud_op_sql

    def _get_cdc_sql_delete_from_view(self, view_name, primary_key):
        d_op_sql = ""
        if self.cdc_format == "DMS-CDC":
            partition_key = ",".join(["data." + pk for pk in primary_key.split(",")])
            d_op_sql = "select * from (select data.*, metadata.operation as operation, row_number() over (partition by {primary_key} order by metadata.timestamp desc) as seqnum  from {view_name} where (metadata.operation='delete') and  metadata.`record-type`!='control' and metadata.`record-type`='data') t1 where seqnum=1".format(
                primary_key=partition_key, view_name="global_temp." + view_name)
        elif self.cdc_format == "FLINK-CDC" or self.cdc_format == "MSK-DEBEZIUM-CDC":
            partition_key = ",".join(["after." + pk for pk in primary_key.split(",")])
            d_op_sql = "select * from (select after.*, op as operation, row_number() over (partition by {primary_key} order by ts_ms desc) as seqnum  from {view_name} where (op='d') ) t1 where seqnum=1".format(
                primary_key=partition_key, view_name="global_temp." + view_name)

        return d_op_sql
    def _get_on_sql(self, stage_table, target_table, primary_key):
        on_sql = []
        for pk in primary_key.split(","):
            tmp = "{stage_table}.{join_key} = {target_table}.{join_key}".format(stage_table=stage_table,
                                                                                target_table=target_table, join_key=pk)
            on_sql.append(tmp)
        return " and ".join(on_sql)

    def _do_write_delete(self, scf, redshift_schema, table_name, primary_key, target_table, ignore_ddl,super_columns, timestamp_columns, date_columns):
        if target_table:
            target_table = target_table+"_delete"
            stage_table_name = redshift_schema + "." + "stage_table_" + target_table
            redshift_target_table = redshift_schema + "." + target_table
            redshift_target_table_without_schema = target_table
        else:
            table_name = table_name+"_delete"
            stage_table_name = redshift_schema + "." + "stage_table_" + table_name
            redshift_target_table = redshift_schema + "." + table_name
            redshift_target_table_without_schema = table_name

        view_name = "kafka_source_" + table_name
        scf.createOrReplaceGlobalTempView(view_name)

        d_op = self._get_cdc_sql_delete_from_view(view_name, primary_key=primary_key)
        self.logger("d operation(delete) sql:" + d_op)
        cols_to_drop = ['seqnum']
        d_df = self.spark.sql(d_op).drop(*cols_to_drop)
        if super_columns:
            # add super schema metadata
            super_column_list = super_columns.split(",")
            if len(super_columns)>0:
                d_df = d_df.na.fill("").replace(to_replace='', value="{}", subset=super_column_list)
            fields = []
            for field in d_df.schema.fields:
                if field.name in super_column_list:
                    sf = StructField(field.name, field.dataType, field.nullable, metadata={"super": True})
                else:
                    sf = StructField(field.name, field.dataType, field.nullable)
                fields.append(sf)
            schema_with_super_metadata = StructType(fields)
            d_df = self.spark.createDataFrame(d_df.rdd, schema_with_super_metadata)
            self.logger(
                "stage table dataframe with super metadata {0}".format(self._getDFSchemaJsonString(d_df)))

        if timestamp_columns:
            d_df = self._convert_date_or_timestamp(d_df, time_columns=timestamp_columns, convert_format="timestamp")
            self.logger(
                "stage table dataframe with timestamp convert {0}".format(self._getDFSchemaJsonString(iud_df)))
        if date_columns:
            d_df = self._convert_date_or_timestamp(d_df, time_columns=date_columns, convert_format="date")
            self.logger(
                "stage table dataframe with date convert {0}".format(self._getDFSchemaJsonString(iud_df)))

        self.logger("stage table delete operate dataframe spark write to s3 {0}".format(self._getDFExampleString(d_df)))
        d_df_columns = d_df.columns
        d_df_columns.remove("operation")
        on_sql = self._get_on_sql(stage_table_name, redshift_target_table, primary_key)
        se = SchemaEvolution(d_df_columns, d_df.schema, redshift_schema, redshift_target_table_without_schema, self.logger, host=self.host,
                             port=self.port, database=self.database, user=self.user, password=self.password)
        if ignore_ddl and ignore_ddl == "true":
            insert_sql_columns, select_sql_columns_with_cast_type = se.get_columns_with_cast_type_from_redshift()
            transaction_sql = "begin; delete from {target_table} using {stage_table} where {on_sql}; insert into {target_table}({insert_columns}) select {select_columns} from {stage_table}; drop table {stage_table}; end;".format(
                stage_table=stage_table_name, target_table=redshift_target_table, on_sql=on_sql,
                insert_columns=",".join(insert_sql_columns), select_columns=",".join(select_sql_columns_with_cast_type))
            if self._check_table_exists(redshift_target_table_without_schema, redshift_schema):
                post_query = transaction_sql
            else:
                raise Exception(
                    "you set ignore_ddl=true but the redshift table not exists: " + str(redshift_target_table))
        else:
            css = se.get_change_schema_sql()
            se.close_conn()
            create_target_table_sql = "create table  {target_table} sortkey ({sortkey}) as select {columns} from {stage_table} where 1=3;".format(
                stage_table=stage_table_name, target_table=redshift_target_table, columns=",".join(d_df_columns),
                sortkey=primary_key)
            transaction_sql = "begin;{scheam_change_sql} delete from {target_table} using {stage_table} where {on_sql}; insert into {target_table}({columns}) select {columns} from {stage_table}; drop table {stage_table}; end;".format(
                stage_table=stage_table_name, target_table=redshift_target_table, on_sql=on_sql,
                columns=",".join(d_df_columns), scheam_change_sql=css)
            if self._check_table_exists(redshift_target_table_without_schema, redshift_schema):
                post_query = transaction_sql
            else:
                post_query = transaction_sql.replace("begin;", "begin; {0}".format(create_target_table_sql))

        self.logger("spark redshift jdbc transaction sql(save delete data) after copy stage table : " + post_query)
        d_df.write \
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

    def _do_write(self, scf, redshift_schema, table_name, primary_key, target_table, ignore_ddl,super_columns,timestamp_columns, date_columns):
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

        iud_op = self._get_cdc_sql_from_view(view_name, primary_key=primary_key)

        self.logger("iud operation(load,update,insert,delete) sql:" + iud_op)
        cols_to_drop = ['seqnum']
        iud_df = self.spark.sql(iud_op).drop(*cols_to_drop)
        # add super schema metadata
        if super_columns:
            super_column_list = super_columns.split(",")
            if len(super_columns)>0:
                iud_df = iud_df.na.fill("").replace(to_replace='', value="{}", subset=super_column_list)
            fields = []
            for field in iud_df.schema.fields:
                if field.name in super_column_list:
                    sf = StructField(field.name, field.dataType, field.nullable, metadata={"super": True})
                else:
                    sf = StructField(field.name, field.dataType, field.nullable)
                fields.append(sf)
            schema_with_super_metadata = StructType(fields)
            iud_df = self.spark.createDataFrame(iud_df.rdd, schema_with_super_metadata)
            self.logger(
                "stage table dataframe schema with super metadata {0}".format(self._getDFSchemaJsonString(iud_df)))

        if timestamp_columns:
            iud_df = self._convert_date_or_timestamp(iud_df,time_columns=timestamp_columns,convert_format="timestamp")
            self.logger(
                "stage table dataframe with timestamp convert {0}".format(self._getDFSchemaJsonString(iud_df)))
        if date_columns:
            iud_df = self._convert_date_or_timestamp(iud_df,time_columns=date_columns, convert_format="date")
            self.logger(
                "stage table dataframe with date convert {0}".format(self._getDFSchemaJsonString(iud_df)))

        self.logger("stage table dataframe spark write to s3 {0}".format(self._getDFExampleString(iud_df)))

        iud_df_columns = iud_df.columns
        iud_df_columns.remove("operation")

        operation_del_value = ""
        if self.cdc_format == "DMS-CDC":
            operation_del_value = "delete"
        elif self.cdc_format == "FLINK-CDC" or self.cdc_format == "MSK-DEBEZIUM-CDC":
            operation_del_value = "d"
        on_sql = self._get_on_sql(stage_table_name, redshift_target_table, primary_key)

        se = SchemaEvolution(iud_df_columns, iud_df.schema, redshift_schema, redshift_target_table_without_schema, self.logger, host=self.host,
                             port=self.port, database=self.database, user=self.user, password=self.password)
        if ignore_ddl and ignore_ddl == "true":
            insert_sql_columns,select_sql_columns_with_cast_type = se.get_columns_with_cast_type_from_redshift()
            transaction_sql = "begin; delete from {target_table} using {stage_table} where {on_sql}; insert into {target_table}({insert_columns}) select {select_columns} from {stage_table} where operation!='{operation_del_value}'; drop table {stage_table}; end;".format(
                stage_table=stage_table_name, target_table=redshift_target_table, on_sql=on_sql,
                insert_columns=",".join(insert_sql_columns), select_columns=",".join(select_sql_columns_with_cast_type), operation_del_value=operation_del_value)
            if self._check_table_exists(redshift_target_table_without_schema, redshift_schema):
                post_query = transaction_sql
            else:
                raise Exception("you set ignore_ddl=true but the redshift table not exists: " + str(redshift_target_table))
        else:
            css = se.get_change_schema_sql()
            se.close_conn()
            # if redshift target table already exists, do not create table
            create_target_table_sql = "create table  {target_table} sortkey ({sortkey}) as select {columns} from {stage_table} where 1=3;".format(
                stage_table=stage_table_name, target_table=redshift_target_table, columns=",".join(iud_df_columns),
                sortkey=primary_key)
            transaction_sql = "begin;{scheam_change_sql} delete from {target_table} using {stage_table} where {on_sql}; insert into {target_table}({columns}) select {columns} from {stage_table} where operation!='{operation_del_value}'; drop table {stage_table}; end;".format(
                stage_table=stage_table_name, target_table=redshift_target_table, on_sql=on_sql,
                columns=",".join(iud_df_columns), scheam_change_sql=css, operation_del_value=operation_del_value)
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
            primary_key = item["primary_key"]
            target_table = ""
            ignore_ddl = ""
            save_delete = ""
            only_save_delete = ""
            super_columns = ""
            timestamp_columns = ""
            date_columns = ""
            if "target_table" in item:
                target_table = item["target_table"]
            if "ignore_ddl" in item:
                ignore_ddl = item["ignore_ddl"]
            if "save_delete" in item:
                save_delete = item["save_delete"]
            if "only_save_delete" in item:
                only_save_delete = item["only_save_delete"]
            if "super_columns" in item:
                super_columns = item["super_columns"]
            if "timestamp_columns" in item:
                timestamp_columns = item["timestamp_columns"]
            if "date_columns" in item:
                date_columns = item["date_columns"]

            # target_table = redshift_schema + "." + table_name

            task_status["table_name"] = table_name

            df = data_frame.filter(gen_filter_udf(db_name, table_name, self.cdc_format)(col('value')))
            fdf = df.select(change_cdc_format_udf(self.cdc_format)(col('value')).alias("value"))
            # self.logger("the table {0}: record number: {1}".format(table_name, str(fdf.count())))
            if not fdf.rdd.isEmpty():
                self.logger("the table {0}:  kafka source data: {1}".format(table_name, self._getDFExampleString(fdf)))
                # auto gen schema
                json_schema = self.spark.read.json(fdf.rdd.map(lambda p: str(p["value"]))).schema
                self.logger("the table {0}: auto gen json schema: {1}".format(table_name, str(json_schema)))
                scf = fdf.select(from_json(col("value"), json_schema).alias("kdata")).select("kdata.*")

                self.logger("the table {0}: kafka source data with auto gen schema: {1}".format(table_name,
                                                                                                self._getDFExampleString(
                                                                                                    scf)))
                if only_save_delete == "true":
                    self._do_write_delete(scf, self.redshift_schema, table_name, primary_key, target_table, ignore_ddl,super_columns,timestamp_columns,date_columns)
                else:
                    self._do_write(scf, self.redshift_schema, table_name, primary_key, target_table,ignore_ddl, super_columns,timestamp_columns,date_columns)
                    if save_delete == "true":
                        self._do_write_delete(scf, self.redshift_schema, table_name, primary_key, target_table, ignore_ddl,super_columns,timestamp_columns,date_columns)
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
