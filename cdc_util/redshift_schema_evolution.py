import redshift_connector
import boto3
import base64
from botocore.exceptions import ClientError
import json
from typing import Optional
from pyspark.sql.types import *


class SchemaEvolution:
    def __init__(self,  data_frame_columns, data_frame_schema, redshift_schema, redshift_table, logger=None,
                 host: Optional[str] = None, port: Optional[int] = None, database: Optional[str] = None, user: Optional[str] = None,
                 password: Optional[str] = None, redshift_secret_id: Optional[str] = None, region_name: Optional[str] = None):
        # redshift_connector.paramstyle = 'named'
        if logger:
            self.logger = logger
        else:
            self.logger = print
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.data_frame_columns = data_frame_columns
        self.data_frame_schema = data_frame_schema
        self.redshift_schema = redshift_schema
        self.table = redshift_table
        self.add_columns = None
        self.drop_columns = None
        self.has_columns = False
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
            self.password = secret_dict["password"]
            self.port = int(secret_dict["port"])
        else:
            self.con = redshift_connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=int(self.port)
            )

    def _get_change_columns(self):
        rtc_columns = self._get_redshift_table_columns()
        if rtc_columns:
            dfc_columns = self.data_frame_columns

            add_columns = list(set(dfc_columns).difference(set(rtc_columns)))
            drop_columns = list(set(rtc_columns).difference(set(dfc_columns)))
            self.add_columns = add_columns
            self.drop_columns = drop_columns
            self.has_columns = True

        else:
            self.has_columns = False

    def _run_sql(self, sql_str,schema):

        with self.con.cursor() as cursor:
            cursor.execute("set search_path to '$user', public, {0}".format(schema))
            for sql in sql_str.split(";"):
                if sql != '':
                    cursor.execute(sql.strip())
            self.con.commit()

    def _run_sql_with_result(self, sql_str,schema):

        with self.con.cursor() as cursor:
            cursor.execute("set search_path to '$user', public, {0}".format(schema))
            cursor.execute(sql_str)
            res = cursor.fetchall()
            return res

    def _get_redshift_table_columns(self):
        sql = "select \"column\" from pg_table_def where tablename = '{0}' and schemaname='{1}'".format(self.table, self.redshift_schema)
        res = self._run_sql_with_result(sql, self.redshift_schema)
        merge_list = sum(list(res), [])
        if not merge_list:
            return None
        else:
            return merge_list

    def _get_redshift_table_columns_with_type(self):
        sql = "select \"column\", \"type\" from pg_table_def where tablename = '{0}' and schemaname='{1}'".format(
            self.table, self.redshift_schema)
        res = self._run_sql_with_result(sql, self.redshift_schema)
        columns_with_type_list = []
        for item in res:
            columns_with_type_dict = {}
            name = item[0]
            data_type = item[1]
            cast_type = True
            if "timestamp" in data_type:
                data_type = "timestamp"
            elif "character" in data_type and "varying" in data_type:
                data_type = "varchar{0}".format(data_type.split("varying")[1])
            elif "character(" in data_type:
                data_type = "varchar{0}".format(data_type.split("character")[1])
            elif "boolean" in data_type:
                data_type = "boolean"
            elif "date" in data_type:
                data_type = "date"
            elif "double precision" in data_type:
                data_type = "float8"
            elif "numeric" in data_type:
                data_type = "numeric{0}".format(data_type.split("numeric")[1])
            elif "real" in data_type:
                data_type = "float4"
            elif "integer" in data_type:
                data_type = "int4"
            elif "bigint" in data_type:
                data_type = "int8"
            elif "smallint" in data_type:
                data_type = "smallint"
            else:
                cast_type = False
            columns_with_type_dict["col_name"] = name
            columns_with_type_dict["data_type"] = data_type
            columns_with_type_dict["cast"] = cast_type
            columns_with_type_list.append(columns_with_type_dict)
        return columns_with_type_list

    def get_columns_with_cast_type_from_redshift(self):
        col_list = self._get_redshift_table_columns_with_type()
        insert_sql_columns = []
        select_sql_columns_with_cast_type = []
        for item in col_list:
            col_name = item["col_name"]
            data_type = item["data_type"]
            cast = item["cast"]
            insert_sql_columns.append(col_name)
            from_column = col_name
            if cast:
                if data_type == "smallint":
                    from_column = "case when trim({col_name}) ~ '^[0-9]+$' then trim({col_name}) else null " \
                                  "end::smallint as {col_name}".format(col_name=col_name)
                else:
                    from_column = col_name+"::"+data_type
            select_sql_columns_with_cast_type.append(from_column)
        return insert_sql_columns, select_sql_columns_with_cast_type

    def _field_string(self, field):
        type_mapping = {
            IntegerType(): "INTEGER",
            LongType(): "BIGINT",
            DoubleType(): "DOUBLE PRECISION",
            FloatType(): "REAL",
            ShortType(): "INTEGER",
            ByteType(): "SMALLINT",
            BooleanType(): "BOOLEAN",
            TimestampType(): "TIMESTAMP",
            DateType(): "DATE",
        }
        string_type = type_mapping.get(field.dataType)
        if field.dataType == StringType():
            if "maxlength" in field.metadata:
                string_type = "VARCHAR({0})".format(int(field.metadata.get("maxlength")))
            elif "super" in field.metadata:
                string_type = "super"
            else:
                string_type = "VARCHAR(65535)"
        if field.dataType == DecimalType():
            string_type = "DECIMAL({0},{1})".format(field.dataType.precision, field.dataType.scale)
        if string_type:
            nullable = "" if field.nullable else "NOT NULL"
            res = field.name + " " + string_type + " " + nullable
            return res.strip()
        else:
            raise Exception("not support data type: " + field.simpleString())

    def _gen_add_col_sql(self):
        sql_list = []
        for col in self.add_columns:
            col_field = None
            for filed in self.data_frame_schema.fields:
                if filed.name == col:
                    col_field = filed
                    break
            if col_field:
                col_string = self._field_string(col_field)
                add_columns_sql = "alter table {0}.{1} add column {2};".format(self.redshift_schema, self.table, col_string)
                sql_list.append(add_columns_sql)
        return sql_list

    def close_conn(self):
        if self.con:
            self.con.close()

    def _gen_drop_col_sql(self):
        sql_list = []
        for col in self.drop_columns:
            drop_columns_sql = "alter table {0}.{1} drop column {2};".format(self.redshift_schema, self.table, col)
            sql_list.append(drop_columns_sql)
        return sql_list

    def change_schema(self):
        self._get_change_columns()
        if not self.has_columns:
            self.logger("schema evolution not get columns from redshift table,maybe table not exists {0}".format(self.table))
            return ""
        add_list = self._gen_add_col_sql()
        self.logger("schema evolution add columns {0}".format(add_list))
        if add_list:
            self._run_sql("".join(add_list),self.redshift_schema)
        drop_list = self._gen_drop_col_sql()
        self.logger("schema evolution drop columns {0}".format(drop_list))
        if drop_list:
            self._run_sql("".join(drop_list),self.redshift_schema)

    # only get the schema sql that needs to be changed without sending it to redshift server for execution
    def get_change_schema_sql(self):
        self._get_change_columns()
        if not self.has_columns:
            self.logger("schema evolution not get columns from redshift table,maybe table not exists {0}".format(self.table))
            return ""
        schema_sql = []
        add_list = self._gen_add_col_sql()
        drop_list = self._gen_drop_col_sql()
        if add_list:
            schema_sql.extend(add_list)
        if drop_list:
            schema_sql.extend(drop_list)
        self.logger("schema evolution get change schema sql {0}".format(schema_sql))
        return "".join(schema_sql)

    def _get_secret(self):
        secret_name = self.redshift_secret_id
        region_name = self.region_name
        self.logger("get redshift conn from secrets manager,secret_id: {0} region_name: {1}".format(secret_name,region_name))
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

