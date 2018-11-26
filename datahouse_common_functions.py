__description__ = '''
==== OVERVIEW ====

ORIGINAL AUTHOR Dan Gooden
PURPOSE         Contains common functions re-used by multiple tasks.
NOTES           None at this time.

==================
'''

import datetime
import re
import luigi
from psycopg2 import ProgrammingError
import os

DEFAULT_DATATYPE = 'VARCHAR'
CURRENCY_DATATYPE = "NUMBER(18, 2)"
SAP_CURRENCY_DATATYPE = "NUMBER(18, 2)"
DECIMAL_DATATYPE = "NUMBER(18,2)"
HIGH_PRECISION_NUMERIC_DATATYPE = "NUMBER(38,12)"


def write_lines_to_file(filepath, lines):
    """
    Opens a file at filepath, and writes the lines to the file, then closes the file.

    :param filepath: The full file path.
    :param lines: The lines to be written to the file.
    :return: None
    """
    target = open(filepath, 'w')
    target.write(lines)
    target.close()


def create_table_statement(table_name, column_definitions, diststyle=None, distkey=None, sortkey=None, column_type=None,
                           add_metadata_columns=True, default_datatype=DEFAULT_DATATYPE, delete_if_exists=False):
    """
    Generate a SQL CREATE TABLE STATEMENT using the parameters specified.

    :param table_name: The name of the table including the schema, e.g. clean.sf_account
    :param column_definitions:
    :param diststyle: The distribution style to use
    :param distkey: The distribution key to use
    :param sortkey: The sort keys to use
    :param column_type: The type of table being created. This is used to determine how to use the column definitions.
                        Possible values are target (use the target column definitions),
                        name_only (the column definitions are a list of column names),
                        use_spec_file & use_spec_file_but_ignoreheader (use the source column definitions), or a
                        custom type that is used as the dictionary key for the column definition.
    :param add_metadata_columns: Whether the standard ETL metadata columns should be added automatically.
    :param default_datatype: The default datatype to use if none is specified.
    :param delete_if_exists: Drop the table before recreating it, if it exists, performing a cascade delete.
    :return: Return the SQL script to (drop &) create the table.
    """

    header = '''---------------------------------------------------------------------------------------------------------------------------------------
-- CREATE TABLE DDL
-- Generated ''' + str(datetime.datetime.utcnow()) + '''
---------------------------------------------------------------------------------------------------------------------------------------

'''
    if delete_if_exists:
        delete_if_exists_statement = "DROP TABLE IF EXISTS " + table_name + " CASCADE;\n\n"
    else:
        delete_if_exists_statement = "\n"

    create_statement = build_line("CREATE TABLE " + table_name + "(")

    last = len(column_definitions) - 1
    for i, column in enumerate(column_definitions):
        if column_type == ('target'):
            column = column['target_column']
        elif column_type == 'name_only':
            column = {'name': column}
        elif column_type in ('use_spec_file', 'use_spec_file_but_ignoreheader'):
            column = column['source_column']
        elif column_type:
            column = column[column_type]

        if 'datatype' in column and column_type not in ('use_spec_file', 'use_spec_file_but_ignoreheader'):
            if column['datatype'] == 'currency':
                column['datatype'] = CURRENCY_DATATYPE
            if column['datatype'] == 'sap_currency':
                column['datatype'] = SAP_CURRENCY_DATATYPE
            if column['datatype'] in ('epochsec', 'epochms', 'ticks', 'ticks2aest'):
                column['datatype'] = 'timestamp'
            if column['datatype'] == 'highprecisionnumeric':
                column['datatype'] = HIGH_PRECISION_NUMERIC_DATATYPE
            datatype = " " + column['datatype']
        else:
            if column_type == 'name_only' and ('_ok' in column['name'] or '_valid' in column['name']):
                 datatype = " boolean"
            else:
                datatype = " " + default_datatype

        if 'mandatory' in column and column['mandatory']:
            not_null = " not null "
        else:
            not_null = " "

        default = " default false " if "boolean" in datatype or "bool" in datatype else ""

        column_ddl = column['name'] + datatype + default + not_null
        if i != last:
            column_ddl += ','

        create_statement += build_line(column_ddl)

    if add_metadata_columns:
        create_statement += build_line(",etl_updated_timestamp timestamp default CURRENT_TIMESTAMP")
        create_statement += build_line(",etl_deleted_flag boolean default false ")

    create_statement += build_line(")")

    create_statement += build_line(";")
    get_logger().debug("Create table statement:\n " + header + delete_if_exists_statement + create_statement)
    return header + delete_if_exists_statement + create_statement


def create_join_using_keys(keys_definition, left_table, right_table, delete_condition=None, use_left_key_only=False):
    """
    Create a SQL join statement between two tables.

    Warning: Ensure columns used in join do not contain null values, as the equality operator will not return
    results on null = null. When run in a non-production environment the code will attempt to check for the existence of
    null values in join columns, if the table does not exist (i.e. temp table) the test will be skipped.

    :param keys_definition: A list of tuples of column joins, e.g. [("right_table_key_column", "left_table_key_column")]
    :param left_table: The table name (or alias) on the left of the join (no schema prefix).
    :param right_table: The table name (or alias) on the right of the join (no schema prefix).
    :param use_left_key_only: Use the left key for both right and left key. Useful when working with spec file
            ssource/target and you want to create a join based on only target columns.
    :return: Return the SQL join statement.
    """
    from config_manager import ConfigManager

    etl = ""
    last = len(keys_definition) - 1
    from database_connection import DatabaseQuery
    for i, key in enumerate(keys_definition):
        datatype_cast = ""
        if len(key) == 3:
            datatype_cast = "::" + key[2]
        left_key = key[1]
        if use_left_key_only:
            right_key = key[1]
        else:
            right_key = key[0]

        if not ConfigManager().is_prod():
            try:
                if not delete_condition:
                    check_for_nulls = DatabaseQuery("select count(*) from " + left_table + " where " + left_key + " is null")
                else:
                    check_for_nulls = DatabaseQuery("select count(*) from " + left_table + " where " + left_key + " is null" + " and " + delete_condition)
                if check_for_nulls.result()[0][0] != 0:
                    print "select count(*) from " + left_table + " where " + left_key + " is null"
                    raise Exception("Null values found during join in " + left_table + "." + left_key)
            except ProgrammingError:
                pass  # Table may not exist, so skip test
            try:
                if not delete_condition:
                    check_for_nulls = DatabaseQuery("select count(*) from " + right_table + " where " + right_key + " is null")
                else:
                    check_for_nulls = DatabaseQuery("select count(*) from " + right_table + " where " + right_key + " is null"+ " and " + delete_condition)

                if check_for_nulls.result()[0][0] != 0:
                    raise Exception("Null values found during join in " + right_table + "." + right_key)
            except ProgrammingError:
                pass  # Table may not exist, so skip test

        etl += left_table + "." + left_key + datatype_cast + " = " + right_table + "." + right_key + datatype_cast + " "
        if i != last:
            etl += "and "
    if delete_condition:
        etl += " and "+left_table + "." +delete_condition
    return etl


def build_line(text):
    """
    Add a new line character to a string.

    :param text: String to add new line character to.
    :return: Return the string with a new line character appended.
    """
    return unicode(str(text) + "\n", "utf-8")


def process_column_name(column_name):
    """
    Clean up column names by replacing invalid characters with an '_', and wrap reserved keywords in quotes so they can
    be used as column names.

    :param column_name: Column name to process
    :return: Cleaned column name
    """
    # Handle columns that have the same name as reserved keys

    column_name = re.sub(r'[^a-zA-Z0-9_\"]','_',column_name.strip())
    if column_name.lower() in ('time', 'datetime', 'timestamp', 'user'):
        return "\"" + column_name + "\""
    else:
        return column_name


def drop_view_statement(view_name):
    """
    Return statement to drop a view with a cascade"

    :param view_name: View name to drop, including schema.
    :return: SQL statement to drop view.
    """

    return "DROP VIEW IF EXISTS " + view_name + " CASCADE;\n\n"


def drop_table_statement(table_name):
    """
    Return statement to drop a table with a cascade"

    :param view_name: Table name to drop, including schema.
    :return: SQL statement to drop table.
    """
    return "DROP TABLE IF EXISTS " + table_name + " CASCADE;\n\n"


def is_compressed(filepath):
    """
    This method determines whether the file is compressed by searching the file name for .gz

    :param filepath: The filepath to check.
    :return: True if the file is compressed using gz, False otherwise.
    """
    m = re.search('.*(.gz)\d*$', str(filepath))
    return True if m else False


def split_cloud_storage_path(key, directory_prefix_only=True):
    """
    Split a cloud storage key into it's 'path' and 'filename'.

    :param key: The cloud storage key to examine
    :param directory_prefix_only: Return the directory/path only
    :return: Returns either the path, or the path and filename as a tuple.
    """
    if '/' in key:
        m = re.search('^(.*/)(.*$)', key)
        if m:
            if directory_prefix_only:
                return m.group(1)
            else:
                return m.group(1), m.group(2)
        else:
            raise Exception("Directory prefix not found in key", key)
    else:
        return "", key


def extract_table_name_from_schema_and_table_name(name):
    """
    Extract the table name from a schema.tablename string.

    :param name: The schema.tablename string to parse. Note, this method will return this param if no '.' is found.
    :return: The tablename component of the string.
    """
    if '.' in name:
        schemaname, tablename = name.split('.')
        return tablename
    else:
        return name


def get_columns_for_table(domain_env, tablename, ignore_metadata_columns=True, return_datatypes=False):
    """
    Return the column names (& data types) of a table. This method is used to determine if a table contains the
    necessary/relevant columns, e.g. if a new column is added to a spec file.

    :param rs_cluster_section: The Redshift configuration section to use. Required to connect to the Db.
    :param tablename: The name of the table required.
    :param ignore_metadata_columns: Whether the etl metadata columns should be included in the column list.
    :param return_datatypes: Whether the data types should be returned as well as the names.
    :return: Return either a list of column names, or a list of tuples containing name and
             datatype, i.e. [(name, datatype)].
    """
    from database_connection import DatabaseQuery

    tablename = tablename.lower()
    if '.' in tablename:
        schemaname, tablename = tablename.split('.')
    else:
        raise Exception("Missing schema in get_columns_for_table() " + tablename)
    select_qry = "select column_name,data_type from INFORMATION_SCHEMA.COLUMNS where lower(table_schema) = lower('" + schemaname + \
                 "') and lower(table_name)= lower('" + tablename + "')"
    if ignore_metadata_columns:
        select_qry += " and lower(column_name) not in ('etl_updated_timestamp', 'etl_deleted_flag')"

    query = DatabaseQuery(select_qry, False, domain_env)
    rows = query.result(None, schemaname)
    get_logger().debug("Rows in get_columns_for_table({}):\n".format(tablename) + str(rows))

    if return_datatypes:
        columns = [(process_column_name(r[0]), r[1]) for r in rows]
    else:
        columns = [process_column_name(r[0]) for r in rows]

    return columns


def extract_bucket_and_path(s3_address):
    """
    Split an S3 path into it's 'bucket' and 'path'.

    :param s3_address: The S3 key to examine
    :return: Returns the bucket and the path as a tuple
    """

    m = re.search("s3://(.+?)/(.*)", str(s3_address), re.IGNORECASE)  # '^.*/(.*\.[csv|txt])$', str(key))
    if m and m.group and m.group(1):
        path = "" if not m.group(2) else m.group(2)
        return m.group(1), path
    else:
        raise Exception("Bucket not found in " + s3_address)


def download_s3_file_to_local_target(s3_source_path, local_file_target, bucket=None):
    """
    Download an S3 object to a local path.

    :param s3_source_path: The S3 object to download.
    :param local_file_target: The full file path on the local machine.
    :param bucket: The S3 bucket to use (default is the Domain S3 bucket as per the environment configuration).
    :return: Return the local file path.
    """
    from datahouse_core import DomainEnvironment

    env = DomainEnvironment()
    if not bucket:
        bucket = env.env_config.s3_bucket

    s3_file = env.s3().Object(bucket, s3_source_path)
    s3_file.download_file(local_file_target.path)
    return local_file_target


def get_os():
    """
    Determine whether the operating system is Windows or Mac.

    The production server is a Windows machine, whereas some of the development machines are Mac

    :return: Either Mac, Windows, or an exception if the OS is not recognised.
    """
    import platform

    if platform.system() == 'Darwin':
        return "Mac"
    elif platform.system() == 'Windows':
        return "Windows"
    else:
        raise Exception("platform.system not identified - " + platform.system())


def today():
    """
    Return today's date as a string. The time is not included.

    :return: Return today's date as a string in the format YYYY-MM-DD.
    """
    return datetime.datetime.now().strftime('%Y-%m-%d')


def slice_timestamp_to_date_string(timestamp):
    """Return a date.

    If a date in the format YYYY-MM-DD is passed to this function it will return that same date as the output.

    :param timestamp: A string timestamp to be sliced, in the format YYYY-MM-DD HH:SS...
    :return: A string date in the format YYYY-MM-DD
    """
    return str(timestamp)[:10]


def slice_timestamp_to_date_hour_string(timestamp):
    """Return a date.

    If a date in the format YYYY-MM-DD HH:00:00 is passed to this function it will return that same date as the output.

    :param timestamp: A string timestamp to be sliced, in the format YYYY-MM-DD HH:MM:SS
    :return: A string date in the format YYYY-MM-DD HH:00:00
    """
    return str(timestamp)[:13] + str(":00:00")


def treat_as_prod_environment(context_schema=None):
    """Determines whether the environment should be treated as if it is production.

    This can be used to determine whether tests should be executed. For e.g. if Task is on the datamart layer, and
    both core and clean contexts are forced to prod, then the result should be pass the same tests as if it was prod.

    :param context_schema: The schema context to use to determine if the environment can be expected to behave like prod
    :return: True if the environment should behave like prod, otherwise False
    """
    if luigi.configuration.get_config().get('domain', 'env-type').lower() == 'prod':
        return True
    elif not context_schema:
        return False

    if context_schema == 'datamart':
        return True

    else:
        raise Exception("Unknown schema context, " + str(context_schema) + ". Please implement rules.")


def file_size(file_path):
    import os
    file_info = os.stat(file_path)
    return file_info.st_size


def get_logger():
    import logging
    from ConfigParser import NoOptionError
    try:
        luigi.interface.setup_interface_logging(luigi.configuration.get_config().get('core', 'logging_conf_file'))
    except NoOptionError:
        pass
    return logging.getLogger('luigi-interface')


def check_output(cmd, **kwargs):
    import subprocess
    try:
        get_logger().debug("check_output command: " + str(cmd))
        result = subprocess.check_output(cmd, **kwargs)
        return result
    except subprocess.CalledProcessError as e:
        get_logger().exception("Subprocess Return Code: " + str(e.returncode) + "\n" + str(e.output))
        raise
