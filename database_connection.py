import abc
import os
import re
import uuid
from ast import literal_eval
from datetime import datetime

import luigi
import pandas as pd
from luigi.contrib import s3
from luigi.contrib.redshift import RedshiftManifestTask

from datahouse_common_functions import get_columns_for_table, write_lines_to_file, \
    extract_table_name_from_schema_and_table_name, process_column_name, create_join_using_keys, \
    extract_bucket_and_path, get_logger
from datahouse_core import DomainTask, DomainEnvironment, JobCompletedLogRecord
from config_manager import ConfigManager


class NoDataReturned(Exception):
    """
    An exception raised if no data is returned.
    """
    pass


class DatabaseQuery:
    """
    A class representing a database query.

    The run() and the result() can be accessed separately.
    """
    configuration_section_name = None
    connection = None
    new_connection = True
    cursor_list = None
    query = None
    autocommit = None
    resultset = None
    test_mode_override = None

    def __init__(self, query, autocommit=False, domain=DomainEnvironment()):
        self.query = query
        self.autocommit = autocommit
        self.domain = domain

    def __del__(self):

        if self.connection and self.new_connection:
            self.connection.execute_string("commit;")
            self.connection.close()

    def run(self, connection=None, schema=None):

        get_logger().debug("Query to execute:\n" + self.query)

        if not connection and self.connection is None:
            self.connection = self.domain.connect()
            self.connection.autocommit = self.autocommit
        else:
            self.connection = connection
            self.new_connection = False

        if self.test_mode() == 'off':
            return

        try:
            self.cursor_list = self.connection.execute_string(self.query)

        except Exception as e:
            get_logger().debug("Rolling back query: " + self.query)
            get_logger().debug('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            self.connection.cursor().execute("rollback")
            raise

    def result(self, connection=None, schema=None):
        if self.test_mode() == 'off':
            return []

        if self.cursor_list is None:
            self.run(connection, schema)

        if self.resultset is None:
            row_list =[]
            for cursors in self.cursor_list:
                row_list = map(list, cursors)
        return row_list

    def result_to_data_frame(self, connection=None, schema=None):
        if self.test_mode() == 'off':
            return []

        if self.cursor is None:
            self.run(connection, schema)

        return pd.read_sql(self.query, self.connection)

    def test_mode(self):
        if self.test_mode_override:
            return self.test_mode_override
        return ConfigManager().test_config('run_queries')


class RunDatabaseQuery(DomainTask):
    """
    A Luigi task to run a query on the default database server (as per the environment configuration).

    The run method (when the object is fully configured):
      * If a key generator task is provided, the run method yields to that task first
      * Writes the query to a text file for auditing purposes
      * Runs the query
      * Confirms column uniqueness is maintained
      * Runs the test
      * Confirms records exist
      * Commits the transaction if no exceptions are raised, or rolls back the transaction

    These steps are configured by overwriting the relevant method or property.

    """
    rs_cluster_override = luigi.Parameter(default='', significant=False)

    def __init__(self, *args, **kwargs):
        super(RunDatabaseQuery, self).__init__(*args, **kwargs)
        self._rs_cluster_section = None
        if self.rs_cluster_override != '':
            self._rs_cluster_section = self.rs_cluster_override
        elif self.rs_cluster_section != '':
            self._rs_cluster_section = self.rs_cluster_section

    @property
    def rs_cluster_section(self):
        return ''

    @property
    def domain(self):
        return DomainEnvironment(self._rs_cluster_section)

    @abc.abstractproperty
    def object_name(self):
        return None

    @property
    def stage_name(self):
        # Can overwrite this method to provide a stage for storing the DDL
        return ""

    @abc.abstractproperty
    def query(self):
        return None

    @property
    def task_comments(self):
        return self.object_name

    @property
    def save_query(self):
        return True

    @property
    def autocommit(self):
        return False

    def run(self):
        from framework.core.tests import TestColumnUniqueness, TestRecordsReturned

        if self.pre_run_tasks() and self.execute_pre_run_tasks:
            yield self.pre_run_tasks()

        if self.key_generator:
            yield self.key_generator

        rsquery = DatabaseQuery(self.query, self.autocommit, self.domain)

        if self.save_query:
            self.query_helper.write_query_to_text_file(self.query, self.object_name, self.stage_name)

        rsquery.run()

        if self.post_execution_query:
            self.query_helper.write_query_to_text_file(self.query, self.object_name + "-post_exec_sql", self.stage_name)
            rs_post_execution_query = DatabaseQuery(self.post_execution_query, self.autocommit,
                                                    self.domain)
            rs_post_execution_query.run(rsquery.connection)

        try:
            if self.test():
                self.test().run_test(rsquery.connection)
            if self.unique_columnsets:
                uniqueness_test = TestColumnUniqueness(self.unique_columnsets, self.object_name,
                                                       self.unique_columnsets_exclude_deleted_flag,
                                                       run_timestamp=self.run_timestamp)
                # uniqueness_test.respect_test_config = False
                uniqueness_test.run_test(rsquery.connection)
            if self.verify_records_exist_query:
                TestRecordsReturned(self.verify_records_exist_query, run_timestamp=self.run_timestamp).run_test(
                    rsquery.connection)
        except Exception as err:
            if not self.autocommit and (luigi.configuration.get_config().get('domain', 'env-type') == 'prod' or
                                        not self.config_manager.test_config('commit_on_failure')):
                get_logger().debug("Rolling back query: " + self.query)
                rsquery.connection.rollback()
            raise

        # Flag job complete
        self.log_task()

    def run_for_test(self):
        '''
        This run_for_test() method is for use when unit testing Luigi classes in Pytest without the luigi framework.
        Unfortunately pytest doesn't execute methods that contain yield statements, but Luigi requires yield statements
        to pass control to another task during a run method.

        Hence, this run_for_test method has the yield statements removed, so cannot be used for any unit testing that
        requires yielding a task. Instead this can be addressed by using the 'other_tasks_to_run' data context approach
        as seen in test_listing_snapshot_daily.py

        :return: Does not return anything, instead executes run method of the task without associated yield statements.
        '''
        from framework.core.tests import TestColumnUniqueness, TestRecordsReturned

        rsquery = DatabaseQuery(self.query, self.autocommit, self.domain)

        if self.save_query:
            self.query_helper.write_query_to_text_file(self.query, self.object_name, self.stage_name)
        rsquery.run()

        if self.post_execution_query:
            self.query_helper.write_query_to_text_file(self.query, self.object_name + "-post_exec_sql", self.stage_name)
            rs_post_execution_query = DatabaseQuery(self.post_execution_query, self.autocommit,
                                                    self.domain)
            rs_post_execution_query.run(rsquery.connection)

        try:
            if self.test():
                self.test().run_test(rsquery.connection)
            if self.unique_columnsets:
                uniqueness_test = TestColumnUniqueness(self.unique_columnsets, self.object_name,
                                                       self.unique_columnsets_exclude_deleted_flag,
                                                       run_timestamp=self.run_timestamp)
                uniqueness_test.run_test(rsquery.connection)
            if self.verify_records_exist_query:
                TestRecordsReturned(self.verify_records_exist_query, run_timestamp=self.run_timestamp).run_test(
                    rsquery.connection)
        except Exception as err:
            if not self.autocommit and (luigi.configuration.get_config().get('domain', 'env-type') == 'prod' or
                                        not self.config_manager.test_config('commit_on_failure')):
                get_logger().debug("Rolling back query: " + self.query)
                rsquery.connection.rollback()
            raise

        # Flag job complete
        self.log_task()

    def does_table_exist(self):
        """
        Determine whether the table already exists.
        """
        schemaname, tablename = self.object_name.split('.')
        query = ( "select 1 as table_exists from information_schema.tables where lower(table_name) = lower('{}') and lower(table_schema)= lower('{}') limit 1").format(tablename,schemaname)


        cursor = self.domain.get_cursor()
        get_logger().debug("Running query :" + str(query))
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            return bool(result)
        finally:
            cursor.close()

    @property
    def unique_columnsets(self):
        return None

    @property
    def unique_columnsets_exclude_deleted_flag(self):
        return False

    @property
    def table_operations(self):
        return TableOperations(self.run_timestamp, self.domain)

    # Composition variable for key generator
    @property
    def key_generator(self):
        return None

    @property
    def verify_records_exist_query(self):
        return None

    @property
    def post_execution_query(self):
        """
        This query is run after the main query() property is executed and before the test is run.

        It can be used to run post-processing tasks that are required.
        :return: A SQL statement that performs post processing tasks.
        """
        return None

    def pre_run_tasks(self):
        """
        This method can be used to yield tasks that should run prior to the main run method
        :return: Return tasks that should yield prior to the main run
        """
        return

    @property
    def execute_pre_run_tasks(self):
        """
        Control whether the pre_run_tasks are executed during default run method()
        :return: Return True if pre_run_tasks are required
        """
        return False


class QueryHelper:
    """
    This class is a helper class that writes queries to a log file location, or loads SQL statements from script files.
    """
    domain = DomainEnvironment()
    instance_path = None

    def __init__(self, instance_module_location=None):
        self.instance_path = instance_module_location

    @classmethod
    def write_query_to_text_file(self, query, object_name, stage_name):
        from framework.core.datahouse_common_functions import write_lines_to_file

        if stage_name != "":
            directory = luigi.configuration.get_config().get('domain', 'temp-dir') + "querylog" + os.sep + \
                        stage_name + os.sep
        else:
            directory = luigi.configuration.get_config().get('domain', 'temp-dir') + "querylog" + os.sep

        # create local folder
        if not os.path.exists(directory):
            try:
                os.makedirs(directory)
            except os.error:
                pass

        write_lines_to_file(os.path.join(directory, str(object_name) + ".sql"), query)

    def load_query_from_script(self, stage_name, filename, format_parameter_args=None, format_parameter_kwargs=None):
        """
        This method loads a SQL file, replaces parameterised schemas with the schema appropriate for the environment,
        and replaces format parameters (i.e. {0}) with the correct result before returning the result.

        :param stage_name: The stage the script resides in, e.g. ga, BillingService, CoreData, etc (nb: case sensitive).
        :param filename: The file to load, excluding the .sql extension.
        :param format_parameters: List of format parameters.
        :return: The query (or queries) from the SQL file, ready to be executed on the database.
        """
        import os
        if not self.instance_path:
            raise Exception("Missing instance module location")
        script_dir = self.instance_path
        rel_path = "Scripts/" + stage_name + "/" + filename + ".sql"
        filename_location = os.path.join(script_dir, rel_path)

        with open(filename_location, "r") as sql_file:
            sql_script = sql_file.read()

        if format_parameter_args is not None:

            sql_script = sql_script.format(*format_parameter_args, **format_parameter_kwargs)

        return sql_script


class TableOperations:
    """
    This class is a helper class for performing database table operations.
    """

    def __init__(self, run_timestamp, domain=DomainEnvironment()):
        self.domain = domain
        self.config_manager = ConfigManager(run_timestamp)

    def generate_tasks_to_align_columns(self, run_timestamp, stage_name, source_object, target_table,
                                        default_datatype=False):
        """
        This method compares two tables, and generates a list of tasks to add columns that are on the source table, but
        not on the target table, to the target table.

        :param run_timestamp: The timestamp to use for the tasks that are generated.
        :param stage_name: The stage applicable to the tasks.
        :param source_object: The source object (both schema and name). Can be either a view or table.
        :param target_table: The target table (both schema and name) to check against the source object.
        :param default_datatype: The default data type to use when aligning columns.
        :return: Return a list of tasks to add columns to the target table, or an empty list if not tasks are required.
        """
        tasks = []
        source_columns = get_columns_for_table(self.domain, source_object, True, True)
        target_columns = get_columns_for_table(self.domain, target_table)

        for column in source_columns:
            column_name = column[0]
            column_datatype = column[1].replace('character varying', 'varchar')
            if column_name not in target_columns:
                # The missing column needs to be added to the target table
                tasks.append(AddColumnToTable(tablename=target_table, column=column_name,
                                              relevant_stage_name=stage_name, datatype=column_datatype,
                                              run_timestamp=run_timestamp
                                              )
                             )
        return tasks

    # Generates CTAS sql
    def generate_ctas_sql(self, source_schema, target_schema, source_table, target_table, rs_cluster=''):
        # Get the DISTKEY and SORT KEY definition

        query = DatabaseQuery(None, domain=DomainEnvironment(rs_cluster))
        query.test_mode_override = 'on'

        ctas_sql = "DROP TABLE IF EXISTS {}.{} ".format(target_schema, target_table)
        ctas_sql += ";\nCREATE TABLE {}.{} \n".format(target_schema, target_table) + " \nAS \nSELECT * FROM {}.{} ".format(source_schema, source_table)
        return str(ctas_sql)

    # Get the column names of a table
    def get_all_columns(self, schema, table_name, rs_cluster=''):
        query_column_list = "SELECT column_name from information_schema.columns WHERE lower(table_name) = '{}' and lower(table_schema) = '{}' ".format(
            table_name, schema)
        rs_column_list = DatabaseQuery(query_column_list, domain=DomainEnvironment(rs_cluster))
        rs_column_list.test_mode_override = 'on'
        rs_column_list.run()

        column_names = rs_column_list.result()
        return column_names

    def return_ddl_for_object(self, source_object, target_name, drop_table=False, add_etl_metadata=True,
                              add_dim_metadata=False):

        source_schema = source_object.split('.')[0]
        source_object_name = source_object.split('.')[1]

        if source_object_name[-2:] == '_v':
            create_tmp_table = "create transient table {}.{}_tmp as select * from {};".format(self.config_manager.staging,
                                                                                    source_object_name, source_object)
            query = DatabaseQuery(create_tmp_table)
            query.test_mode_override = 'on'
            query.run()
            source_object_name += '_tmp'

        query_string = "select replace(regexp_replace(regexp_replace(lower(get_ddl('table','" +source_schema +"."+  source_object_name +"')), '(etl_deleted_flag.*\\\n)|(etl_updated_timestamp.*\\\n)|(originating_business_key.*\\\n)|(originating_business_key_name.*\\\n)|(originating_source_system_code.*\\\n)', ''),'[,]?\\\s+\\\);$','\\n'), 'transient','');"
        query =DatabaseQuery(query_string)
        query.test_mode_override = 'on'
        ddl = query.result()[0][0]


        if add_etl_metadata:
            ddl += "	,etl_updated_timestamp timestamp default CURRENT_TIMESTAMP\n"
            ddl += "	,etl_deleted_flag boolean default false\n"

        if add_dim_metadata:
            ddl += "	,row_effective_timestamp timestamp default '1970-01-01'::timestamp\n"
            ddl += "	,row_expiry_timestamp timestamp default '2400-01-01'::timestamp\n"
            ddl += "	,row_update_timestamp timestamp default '2400-01-01'::timestamp\n"
            ddl += "	,row_current_flag boolean default true\n"
            ddl += "	,etl_inserted_timestamp timestamp default CURRENT_TIMESTAMP\n"

        ddl += ");"

        ddl = ddl.replace("{}".format(source_object_name), target_name)
        if drop_table:
            ddl = "DROP TABLE IF EXISTS {} \n ".format(target_name)

        get_logger().debug("return_ddl_for_object: " + ddl)

        if source_object_name != source_object.split('.')[1]:
            drop_tmp_table = "drop table if exists {}.{};".format(self.config_manager.staging, source_object_name)
            query = DatabaseQuery(drop_tmp_table)
            query.test_mode_override = 'on'
            query.run()

        return ddl


class RunScriptDatabaseQuery(RunDatabaseQuery):
    """
    Task to execute a SQL script against database. The SQL script is retrieved from a file, rather than generated
    in the task.

    Overwrite filename() to provide the name of the file containing the statement(s) to run.
    """

    @abc.abstractproperty
    def stage_name(self):
        return ""

    def filename(self):
        return ""

    @property
    def object_name(self):
        return self.filename()

    @property
    def query(self):
        return self.query_helper.load_query_from_script(self.stage_name, self.filename(), self.format_parameters,
                                                        format_parameter_kwargs=self.config_manager.default_format_kwargs)

    @property
    def format_parameters(self):
        """
        Override this property to return a list of format parameters to replace in the script. The script defines
        parameters using the syntax {0}, {1}, etc, and the string at index 0 in the returned list will replace {0} in
        the script, whereas the string at index 1 in the returned list will replace {1} and so on.

        :return: A list of values to replace parameters in the script.
        """
        return []


class RefreshTable(RunDatabaseQuery):
    """
    A task that truncates or creates a table, then inserts all records from the source_name into the target_name.

    This task can be used to move the contents of a view into a table, for example.
    """

    def requires(self):

        if self.does_table_exist():
            tasks = self.table_operations.generate_tasks_to_align_columns(self.run_timestamp,
                                                                          self.stage_name,
                                                                          self.source_name,
                                                                          self.target_name)
            return tasks

    @property
    def table_operations(self):
        return TableOperations(self.run_timestamp)

    @abc.abstractproperty
    def target_name(self):
        return ""

    @abc.abstractproperty
    def source_name(self):
        return ""

    @property
    def object_name(self):
        return self.target_name

    @property
    def query(self):

        columns = get_columns_for_table(self.domain, self.target_name)

        if not self.does_table_exist():
            query = "create table " + self.target_name + " as "
        else:
            query = "truncate table " + self.target_name + ";"
            query += "insert into " + self.target_name + " (" + ", ".join(columns) + ")\n"
        query += "" + self.config_manager.select() + " " + ", ".join(columns) + " from " + self.source_name + ";"

        return query


class CreateEmptyTableFromSourceObject(RunDatabaseQuery):
    priority = 0
    source_object = luigi.Parameter()
    target_name = luigi.Parameter()
    required_task = luigi.Parameter(default='')
    add_dimension_metadata = luigi.BoolParameter(default=False)

    def requires(self):
        if self.required_task:
            return self.required_task

    @property
    def object_name(self):
        return self.target_name

    @property
    def query(self):
        if not self.does_table_exist():
            ddl = self.table_operations.return_ddl_for_object(self.source_object,
                                                              self.target_name, False, True,
                                                              self.add_dimension_metadata)
            return ddl


class CreateViewFromScript(RunScriptDatabaseQuery):
    """
    A task that creates a view with the object name, using the SQL in a script file.
    """
    _drop_view = luigi.Parameter(default=False)

    # def __init__(self, *args, **kwargs):
    #    super(CreateViewFromScript, self).__init__(*args, **kwargs)
    #   self._drop_view = False

    @property
    def drop_view(self):
        """
        A property specifying if the view should be dropped before creation.
        :return: A boolean indicating whether the view should be dropped prior to creation.
        """
        return self._drop_view

    @property
    def query(self):
        query = ""
        script = super(CreateViewFromScript, self).query
        if self.drop_view:
            query += "DROP VIEW IF EXISTS " + self.object_name + " CASCADE;\n"
        return query + " CREATE OR REPLACE VIEW " + self.object_name + " AS " + script

    @property
    def autocommit(self):
        return True


class CreateViewFromScriptGivenFilename(CreateViewFromScript):
    filename_param = luigi.Parameter()
    stage_name_param = luigi.Parameter()
    object_name_param = luigi.Parameter()

    @property
    def stage_name(self):
        return self.stage_name_param

    @property
    def object_name(self):
        return self.object_name_param

    def filename(self):
        return self.filename_param


class LoadDataUsingLoadStrategy(RunDatabaseQuery):
    """
    This task loads the target_table from the source object using a chosen load strategy.

    Upsert, refresh_by_keys and full load strategies require keys to be set so existing records can be identified.

    The available load strategies are:
      * upsert - Delete records from target that exist in source, then insert all records from source into the target
      * insert - Insert all records from source into the target
      * full - Mark records in the target that don't exist in the source as etl_deleted_flag = True, then perform refresh_by_keys
      * refresh - Delete all records in the target, then insert all records from source into the target
      * refresh_by_keys - To replace upsert.
            It does the same thing as upsert, that is to delete data in target table found in source table and insert all data from source table.
            The only difference is that it delete by a distinct list of values of the join keys.
            It performs much better when the join keys specified have duplicated values in source tables.
    """

    def required_task(self):
        return ''

    @abc.abstractproperty
    def stage_name(self):
        return ""

    @abc.abstractproperty
    def target_table(self):
        return ""

    @abc.abstractproperty
    def source_object(self):
        return ""

    @property
    def exclusive_lock(self):
        return False

    @property
    def target_columns(self):
        """
        Specify the list of columns to be loaded into the target table, in order.

        By default, it's assumed target and source have the same column names, and the list of columns to be loaded
        is the intersection of columns on the target table, and the source object.

        Overwrite this property and self.source_columns property if the column names in the target table differ from
        from column names in the source.

        The order of columns is important as fully specified INSERT statements are used.

        :return: Returns a list of the columns to be loaded in the target table.
        """
        target_table_columns = get_columns_for_table(self.domain, self.target_table)
        source_table_columns = get_columns_for_table(self.domain, self.source_object)
        return list(set(target_table_columns) & set(source_table_columns))

    @property
    def source_columns(self):
        """
        Specify the list of source columns that are used to feed the target table.

        By default, it's assumed target and source have the same column names, and the list of columns to be loaded
        is the intersection of columns on the target table, and the source object.

        Overwrite this property and self.target_columns property if the column names in the target table differ from
        from column names in the source.

        The order of columns is important as fully specified INSERT statements are used.

        :return: Returns a list of columns in the source object that feed into the target table.
        """
        return self.target_columns

    @property
    def join_keys(self):
        """
        A list of tuples of column joins, used to match records between the source object and target table.
        e.g. [("right_table_key_column", "left_table_key_column", <optional data type>)]

        A data type can be optionally, provided if the right and left join columns need to be converted to a specific
        datatype for the join to succeed.

        Required for upserts, refresh_by_keys and full load strategies.

        :return: Returns a list of tuples of columns joins.
        """
        if self.load_strategy in ('upsert', 'full', 'refresh_by_keys'):
            raise Exception("Join key is required for upsert, refresh_by_keys and full load strategy")
        return []

    @property
    def load_strategy(self):
        return "upsert"

    @property
    def delete_condition(self):  # This property is helpful to restrict further your delete from the target table.
        # For example to delete only specific event from user_events table specify " event_type='ebrouchureclickthroughs'.
        # Also helpful to add an extra condition during null checks for join keys in datahouse_common_functions.create_join_using_keys
        return None

    @property
    def full_record_comparison_source(self):
        return self.source_object

    @property
    def stage_source_in_temporary_table(self):
        return True if self.load_strategy in ('upsert', 'refresh_by_keys') else False

    @property
    def temp_source(self):
        if (self.stage_source_in_temporary_table):
            return "temp_" + self.stage_name + "_" + extract_table_name_from_schema_and_table_name(self.source_object)
        else:
            return self.source_object

    @property
    def delete_source(self):
        source_table = self.temp_source if (
            self.stage_source_in_temporary_table) else self.full_record_comparison_source
        source_key_columns = "::varchar(2048), ".join(map(process_column_name, [key[0] for key in
                                                                                self.join_keys])) + "::varchar(2048)" if self.load_strategy == 'full' \
            else ', '.join(map(process_column_name, [key[0] for key in self.join_keys]))
        return '''(SELECT DISTINCT {columns} FROM {source}) delete_source '''.format(columns=source_key_columns,
                                                                                     source=source_table)

    @property
    def object_name(self):
        return self.target_table

    @property
    def sort_keys(self):
        return None

    @property
    def query(self):
        return self.generate_load_data_sql(self.target_table, self.source_object, self.target_columns,
                                           self.source_columns, self.join_keys,
                                           self.load_strategy, self.full_record_comparison_source, self.exclusive_lock,
                                           self.stage_source_in_temporary_table, self.temp_source, self.delete_source,
                                           self.load_constraint)

    @property
    def load_constraint(self):
        # include where statement
        return ""

    def generate_load_data_sql(self, target_table, source_object, target_columns_list, source_columns_list, join_keys,
                               load_strategy='upsert', full_record_comparison_source=None, exclusive_lock=False,
                               stage_source_in_temporary_table=False, temp_source=None,
                               delete_source=None, load_constraint=''):
        """

        :param target_table: The fully specified name of the target table
        :param source_object: The fully specified name of the source object
        :param target_columns_list: Specify the list of columns to be loaded into the target table, in order.
        :param source_columns_list: Specify the list of source columns that are used to feed the target table, in order.
        :param join_keys: Specify a list of tuples of columns joins, e.g. [(source key column 1, target key column 1)].
        :param load_strategy: Specify either full, insert, upsert, refresh_by_keys or refresh. refresh_by_keys is the default.
        :param full_record_comparison_source: Specify table to use to determine if a record exists for a full refresh.
        :param exclusive_lock: flag to indicate if table should be locked
        :param stage_source_in_temporary_table, if true, then source will be staged into a temp table before processing
        :param temp_source: the temp table's name when stage_source_in_temporary_table is true, otherwise, it's the source table
        :param delete_source: Delete data from target table base on this delete_source on join keys
        :param load_constraint: Additional condition for insertion.
        :return: SQL to execute load of target table using specified load strategy.
        """
        target_columns = ", ".join(map(process_column_name, target_columns_list))
        source_columns = ", ".join(map(process_column_name, source_columns_list))

        etl_updated_timestamp = ''' '{timestamp}' '''.format(timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        etl = "--Scripts for loading strategy: " + load_strategy
        if exclusive_lock:
            etl = "LOCK TABLE " + target_table + ";"
        else:
            etl = ""

        if stage_source_in_temporary_table:
            etl += '''
                -- Create temporary staging table for source data
                CREATE TEMP TABLE {temp_table} AS {select} * FROM {source};                
                '''.format(temp_table=temp_source, select=self.config_manager.select(), source=source_object)

        if load_strategy in ('upsert', 'full', 'refresh_by_keys'):
            etl += '''
                --Delete data that's found in the delete_source. 
                DELETE FROM {target} USING {delete_source} WHERE {condition};
            '''.format(target=target_table, delete_source=delete_source,
                       condition=create_join_using_keys(join_keys, target_table, "delete_source",
                                                        self.delete_condition))
        elif load_strategy == 'refresh':
            etl += '''
                DELETE FROM {target};
            '''.format(target=target_table)

        if load_strategy == "full":
            etl += '''
                --UPDATE etl_delete_flag for data not found in delete_source
                UPDATE {target}
                SET etl_deleted_flag = true, etl_updated_timestamp = CONVERT_TIMEZONE ('Australia/Sydney', GETDATE())
                WHERE etl_deleted_flag = false;
            '''.format(target=target_table)

        etl += '''
            -- Now append records from source into temporary table, so it contains both deleted and new/updated records.
            INSERT INTO {target_table} ({target_columns}, etl_updated_timestamp)
            {select} {source_columns}, {etl_updated_timestamp} FROM {source_table} {load_constraint};

        '''.format(target_table=target_table, target_columns=target_columns, select=self.config_manager.select(),
                   source_columns=source_columns, source_table=temp_source,
                   etl_updated_timestamp=etl_updated_timestamp, load_constraint=load_constraint)

        return etl

    @property
    def add_dimension_metadata_to_target(self):
        """
        If the target is created on the fly, this property sets whether the dimension metadata columns are added
        to the table. If no table is created on the fly this property is ignored

        :return: True if dimension metadata should be added, other wise False
        """
        return False

    def pre_run_tasks(self):
        tasks = []
        if self.does_table_exist():
            tasks += self.table_operations.generate_tasks_to_align_columns(self.run_timestamp,
                                                                           self.stage_name, self.source_object,
                                                                           self.target_table, True)
        else:
            tasks.append(
                CreateEmptyTableFromSourceObject(source_object=self.source_object, target_name=self.target_table,
                                                 run_timestamp=self.run_timestamp, required_task=self.required_task(),
                                                 add_dimension_metadata=self.add_dimension_metadata_to_target))
        return tasks


class LoadDailySnapshot(RunDatabaseQuery):
    date = luigi.Parameter()

    def requires(self):
        tasks = self.table_operations.generate_tasks_to_align_columns(self.run_timestamp, self.stage_name,
                                                                      self.source_object, self.target_table)
        return tasks

    @property
    def table_operations(self):
        return TableOperations(self.run_timestamp)

    @property
    def object_name(self):
        return self.target_table

    @abc.abstractproperty
    def target_table(self):
        return ""

    @abc.abstractproperty
    def source_object(self):
        return ""

    @property
    def query(self):
        columns = get_columns_for_table(self.domain, self.target_table)
        columns_capital = [item.upper() for item in columns]
        snapshop_date_index = columns_capital.index("SNAPSHOT_DATE")
        del columns[snapshop_date_index] # as this column is re-generated now
        query_string = "delete from {} where SNAPSHOT_DATE = '{}';\n".format(self.target_table, self.date)
        query_string += "insert into " + self.target_table + \
                        " (SNAPSHOT_DATE, " + ", ".join(
            columns) + ")\n"
        query_string += "" + self.config_manager.select() + " to_date('" + self.date + "'), " + ", ".join(
            columns) + " from " + self.source_object
        return query_string


class AddColumnToTable(RunDatabaseQuery):
    """
    A task to add a column to a table.
    """
    from framework.core.datahouse_common_functions import DEFAULT_DATATYPE
    priority = 100
    tablename = luigi.Parameter()
    column = luigi.Parameter()
    datatype = luigi.Parameter(default=DEFAULT_DATATYPE)
    relevant_stage_name = luigi.Parameter()
    default = luigi.Parameter(default='none')

    @property
    def object_name(self):
        return self.tablename + "." + self.column

    @property
    def stage_name(self):
        return self.relevant_stage_name

    @property
    def query(self):
        ddl = "ALTER TABLE {} ADD {} {} ".format(self.tablename, self.column, self.datatype)

        if self.datatype == 'boolean' and self.default == 'none':
            self.default = "false"

        if self.default != 'none':
            ddl += " default " + self.default
        ddl += ";"

        return ddl


class DomainS3FileToRedshiftTable(luigi.contrib.redshift.S3CopyToTable):
    """
    A task (configured for the Domain environment) to load an S3 file object into a Redshift table.
    """
    rs_cluster_override = luigi.Parameter(default='', significant=False)
    run_timestamp = luigi.Parameter()
    _config = None

    def __init__(self, *args, **kwargs):
        super(DomainS3FileToRedshiftTable, self).__init__(*args, **kwargs)
        self._rs_cluster_section = None
        if self.rs_cluster_override != '':
            self._rs_cluster_section = self.rs_cluster_override
        elif self.rs_cluster_section != '':
            self._rs_cluster_section = self.rs_cluster_section

        self.s3_base_address = 's3://' + self.domain.env_config.s3_bucket + '/'
        self.config_manager = ConfigManager(self.run_timestamp)

    @property
    def domain(self):
        return DomainEnvironment(self._rs_cluster_section)

    @property
    def host(self):
        return self.domain.env_config.rs_server

    @property
    def database(self):
        return self.domain.env_config.rs_database

    @property
    def user(self):
        return self.domain.env_config.rs_user

    @property
    def password(self):
        return self.domain.env_config.rs_password

    @property
    def rs_cluster_section(self):
        return ''

    def output(self):
        control = DomainEnvironment("redshift_control")
        return JobCompletedLogRecord(
            host=control.env_config.rs_server,
            database=control.env_config.rs_database,
            user=control.env_config.rs_user,
            password=control.env_config.rs_password,
            table=self.task_comments,
            update_id=self.task_id,
            run_timestamp=self.run_timestamp
        )

    def s3_load_path(self):
        return self.s3_base_address + self.s3_key

    @property
    def aws_access_key_id(self):
        return self.domain.env_config.s3_access_key_id

    @property
    def aws_secret_access_key(self):
        return self.domain.env_config.s3_secret_access_key

    @property
    def task_comments(self):
        return self.table

    def run(self):
        """
        If the target table doesn't exist, self.create_table
        will be called to attempt to create the table.
        """
        if not (self.table):
            raise Exception("table need to be specified")

        path = self.s3_load_path()
        connection = self.domain.connect()
        if not self.does_table_exist(connection):
            # try creating table
            connection.reset()
            self.create_table(connection)
        elif self.do_truncate_table:
            self.truncate_table(connection)

        cursor = connection.cursor()
        self.init_copy(connection)
        self.copy(cursor, path)
        self.output().touch()
        connection.commit()

        # commit and clean up
        connection.close()


class DomainRedshiftManifestTask(RedshiftManifestTask):
    """
    Task (configured for the Domain environment) to generate a manifest file that can be used to load data into Redshift

    For full description on how to use the manifest file see
    http://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html

    Usage:

        * requires parameters
            * path - s3 path to the generated manifest file, including the
                     name of the generated file
                     to be copied into a redshift table
            * folder_paths - s3 paths to the folders containing files you wish to be copied

    Output:

        * generated manifest file
    """

    # should be over ridden to point to a variety
    # of folders you wish to copy from
    folder_paths = luigi.Parameter()
    text_target = True
    filter = luigi.Parameter()

    @property
    def domain(self):
        return DomainEnvironment()

    def run(self):
        import json
        entries = []
        self.folder_paths = literal_eval(str(self.folder_paths))

        for folder_path in self.folder_paths:
            bucket, path = extract_bucket_and_path(folder_path)
            keys = self.domain.s3().Bucket(bucket).objects.filter(Prefix=path)

            for object_details in keys:
                if self.filter in ('None', '') or (self.filter and self.filter in object_details.key):
                    entries.append({
                        'url': 's3://%s/%s' % (bucket, object_details.key),
                        'mandatory': True
                    })

        manifest = {'entries': entries}
        target = self.output().open('w')
        dump = json.dumps(manifest)
        if not self.text_target:
            dump = dump.encode('utf8')
        target.write(dump)
        target.close()


class ReplaceMethodInScript(RunScriptDatabaseQuery):
    """
    A task to run a script on Redshift, but replace the any occurrence in the script of @generic_function with the
    command specified in method().

    This task is used to run the vacuum, and analyze tasks. The same script is used (that specifies the tables), and the
    appropriate command is swapped in.
    """

    @property
    def save_query(self):
        return False

    @abc.abstractproperty
    def method(self):
        return ""

    @property
    def query(self):
        base_query = self.query_helper.load_query_from_script(self.stage_name, self.filename(), self.format_parameters,
                                                              format_parameter_kwargs=self.config_manager.default_format_kwargs)
        query = base_query.replace("@generic_function", self.method)
        return query


class UnloadQueryResultToS3(RunDatabaseQuery):
    """
    A task to unload the results of a query to S3.
    """
    filename = luigi.Parameter()
    s3_target_path = luigi.Parameter()
    parallel = luigi.Parameter(default=False)

    @property
    def aws_access_key_id(self):
        return self.domain.env_config.s3_access_key_id

    @property
    def aws_secret_access_key(self):
        return self.domain.env_config.s3_secret_access_key

    def output(self):
        return s3.S3Target(
            "s3://" + self.s3_target_path )  # At a minimum, the first file (of potentially many) should exist.

    @property
    def object_name(self):
        return self.filename

    @property
    def query(self):
        return "COPY into 's3://" + self.s3_target_path  +"' FROM ( "+ self.query_to_unload + ") " \
                "CREDENTIALS = ( AWS_KEY_ID='" + self.aws_access_key_id +"' AWS_SECRET_KEY='" + self.aws_secret_access_key + "' ) " \
               + self.unload_options + " " + self.parallel_option + ";"

        # return "COPY into ('" + self.query_to_unload + "')  's3://" + self.s3_target_path + \
        #        "' CREDENTIALS 'aws_access_key_id=" + self.aws_access_key_id + ";aws_secret_access_key=" + \
        #        self.aws_secret_access_key + "' " + self.unload_options + " " + self.parallel_option + ";"

    @abc.abstractproperty
    def query_to_unload(self):
        pass

    @property
    def delimiter(self):
        return ","

    @property
    def unload_options(self):
        # return "delimiter as '" + self.delimiter + "' escape OVERWRITE = TRUE addquotes"
        return "FILE_FORMAT =(FIELD_DELIMITER  = '"+ self.delimiter + "' ESCAPE = '/\' COMPRESSION = None )"

    @property
    def parallel_option(self):
        if bool(self.parallel):
            return "SINGLE = FALSE"
        else:
            return "OVERWRITE = TRUE  MAX_FILE_SIZE = 5000000000 SINGLE = TRUE"



class CreateSourceObjectViewWithKey(RunDatabaseQuery):
    """
    This task creates a view on top of a source object (created by self.source_task_param, or provided as
    self.source_object_param) that links the source records with the key that has been generated. This view can
    then be used as the source object for a LoadDataFromSourceTaskWithKeyUsingLoadStrategy task.

    This task is not designed to be overwritten, as it should be transparently used when the
    LoadDataFromSourceTaskWithKeyUsingLoadStrategy task is used.


    For more information see class information for LoadDataFromSourceTaskWithKeyUsingLoadStrategy.

    Usage:
        This task should be provided with either of the following params, but not both, as either the source object
        is created at run time using the source_task, or it is pre-existing, i.e. source_object_param
            source_task_param: Contains the task to execute to generate the source object used by this task.
            source_object_param: Contains the source object used by this task.
    """
    multiple_source_systems = luigi.BoolParameter()
    originating_business_key_name = luigi.Parameter()
    originating_source_system_code = luigi.Parameter()
    entity = luigi.Parameter()
    key_table_prefix = luigi.Parameter()
    key = luigi.Parameter()
    stage_name_param = luigi.Parameter()
    key_manager = luigi.Parameter()
    source_task_param = luigi.Parameter(default='')
    source_object_param = luigi.Parameter(default='')

    def requires(self):
        if self.source_task_param:
            return self.source_task_param

    @property
    def join_keys(self):
        return [[self.originating_business_key_name, "source_business_key"]]

    @property
    def stage_name(self):
        return self.stage_name_param

    @property
    def object_name(self):
        key_table_prefix = self.key_table_prefix + "_" if self.key_table_prefix else ""
        return self.config_manager.staging + "." + key_table_prefix + self.entity + "_v"

    @property
    def source_object(self):
        if self.source_object_param and not self.source_task_param:
            return self.source_object_param
        else:
            return self.source_task_param.object_name

    @property
    def keys_table_name(self):
        from framework.core.key_management import key_table_name_generator
        return key_table_name_generator(self.config_manager.keys, self.key_table_prefix, self.entity)

    @property
    def keys_map_table_name(self):
        from framework.core.key_management import key_map_table_name_generator
        return key_map_table_name_generator(self.config_manager.keys, self.key_table_prefix, self.entity)

    @property
    def view_ddl(self):
        if not self.multiple_source_systems:
            originating_business_key_column_name = "\'" + self.originating_business_key_name + "\'"
            originating_source_system_code_column_name = "\'" + self.originating_source_system_code + "\'"
        else:
            originating_business_key_column_name = "originating_business_key_name"
            originating_source_system_code_column_name = "originating_source_system_code"

        return "(select map." + self.key + ", so.* from " + self.source_object + " so inner join " + self.keys_map_table_name + " map on " + \
               create_join_using_keys(self.join_keys, "map", "so") + " and map.source_business_key_name = " + \
               originating_business_key_column_name + " and map.source_system_code = " + \
               originating_source_system_code_column_name + ")"

    @property
    def query(self):
        query = ""
        if self.drop_view:
            query += "DROP VIEW IF EXISTS " + self.object_name + ";\n"
        query += " CREATE OR REPLACE VIEW " + self.object_name + " AS "
        return query + self.view_ddl

    @property
    def key_generator(self):
        from framework.core.key_management import KeyGenerator
        return KeyGenerator(run_timestamp=self.run_timestamp,
                            multiple_source_systems=self.multiple_source_systems, source_object=self.source_object,
                            keys_table=self.keys_table_name, keys_map_table=self.keys_map_table_name, key=self.key,
                            key_manager=self.key_manager,
                            originating_business_key_name=self.originating_business_key_name,
                            originating_source_system_code=self.originating_source_system_code)

    @property
    def drop_view(self):
        return True


class CreateIntermediateViewWithKey(CreateSourceObjectViewWithKey):
    """
    This task creates a view on top of a source object (created by self.source_task) that links the source records with
    the key that has been generated. This view can then be used as the source object for a
    LoadDataFromSourceTaskWithKeyUsingLoadStrategy task.

    This task is not designed to be overwritten, as it should be transparently used when the
    LoadDataFromSourceTaskWithKeyUsingLoadStrategy task is used.

    For more information see class information for LoadDataFromSourceTaskWithKeyUsingLoadStrategy.
    """
    filename_for_intermediate_view = luigi.Parameter()
    filename_for_key_generator_input_view = luigi.Parameter()
    object_name_for_intermediate_view = luigi.Parameter()
    object_name_for_key_generator_input_view = luigi.Parameter()

    def requires(self):
        task = CreateViewFromScriptGivenFilename(run_timestamp=self.run_timestamp,
                                                 filename_param=self.filename_for_key_generator_input_view,
                                                 object_name_param=self.object_name_for_key_generator_input_view,
                                                 stage_name_param=self.stage_name,
                                                 instance_module_location_override
                                                 =self.instance_module_location_override
                                                 )
        task._drop_view = True
        return task

    @property
    def object_name(self):
        return self.object_name_for_intermediate_view

    @property
    def source_object(self):
        return self.object_name_for_key_generator_input_view

    @property
    def query(self):
        query = ""
        script = self.query_helper.load_query_from_script(self.stage_name, self.filename_for_intermediate_view,
                                                          format_parameter_args=[],
                                                          format_parameter_kwargs=self.config_manager.default_format_kwargs)
        if self.drop_view:
            query += "DROP VIEW IF EXISTS " + self.object_name + " CASCADE;\n"
        return query + " CREATE OR REPLACE VIEW " + self.object_name + " AS " + script + ";\n"


class LoadDataFromSourceTaskWithKeyUsingLoadStrategy(LoadDataUsingLoadStrategy):
    """
    This class loads data into a table using a load strategy, with a generated key that is created using the Key
    management classes. The execution process to perform this is as follows.

    1. self.source_object_task runs, creating the database object used to drive key generation.
    2. CreateSourceObjectViewWithKey runs, firstly generating the keys required, and then creating a view that links
    the keys with the database object created in step 1.
    3. This task runs, loading data from the source object created in step 2 using the specified load strategy.

    """

    def requires(self):
        if self.source_object_task:
            source_object_task = self.source_object_task()
        else:
            source_object_task = ''
        source_view_with_key = CreateSourceObjectViewWithKey(run_timestamp=self.run_timestamp,
                                                             multiple_source_systems=self.multiple_source_systems,
                                                             originating_business_key_name=self.originating_business_key_name,
                                                             originating_source_system_code=self.originating_source_system_code,
                                                             entity=self.entity, key_table_prefix=self.table_prefix,
                                                             stage_name_param=self.stage_name,
                                                             key=self.key, key_manager=self.key_manager,
                                                             source_task_param=source_object_task,
                                                             source_object_param=self.source_object_override
                                                             )
        tasks = [source_view_with_key]

        if self.does_table_exist():
            tasks += self.table_operations.generate_tasks_to_align_columns(self.run_timestamp,
                                                                           self.stage_name, self.source_object,
                                                                           self.target_table, True)
        else:
            tasks.append(
                CreateEmptyTableFromSourceObject(source_object=self.source_object, target_name=self.target_table,
                                                 run_timestamp=self.run_timestamp, required_task=source_view_with_key,
                                                 add_dimension_metadata=self.add_dimension_metadata_to_target))
        return tasks

    @property
    def originating_business_key_name(self):
        """
        Provide the name of the field in the source that contains the business key to use to generate new
        surrogate keys. This property should be overwritten with the name of the column that contains the key.

        If self.multiple_source_systems = True, is is assumed there will be a field in the soruce named
        "originating_business_key_name" that contains the name of the field the the key is sourced from applicable to
        each row.
        """
        return "originating_business_key"

    @property
    def originating_source_system_code(self):
        """
        Provide the code of the source system/location where the data is sourced from, unless data is from multiple systems.

        If self.multiple_source_systems = True, it is assumed there will be a field in the source named
        originating_source_system_code that contains the source system code applicable to each row.

        :return: The source system code where the data is from, if data is from a single system.
        """

        if not self.multiple_source_systems:
            raise Exception(
                "LoadDataWithKeyUsingLoadStrategy.originating_source_system_code required if not multiple source "
                "systems. Overwrite method to return the source system code.")

    @property
    def multiple_source_systems(self):
        """
        Indicates whether the source is a result of unioning data from multiple systems/locations.
        :return: True if the data consists of data from multiple systems/locations.
        """
        return True

    @property
    def join_keys(self):
        return [[self.key, self.key]]

    @abc.abstractproperty
    def entity(self):
        """
        Overwrite with the name of the entity / table this task relates to.
        :return: The name of the entity this task relates to.
        """
        return ""

    @property
    def source_object_task(self):
        """
        The Luigi task that will be used to create or load the pre-key source object required for the load. The database
        object created by this task should not have a key field, as the keys are generated subsequent to the completion
        of this task.

        If self.multiple_source_systems = True, then it is assumed that the source object created will have in addition
        to the business key column, two additional columns, "originating_source_system_code" and
        "originating_business_key_name", that contain the code of the source system that the data is from, and the
        name of the field in the source that is the business key, respectively.

        The task needs to be specified here, rather than in the requires, as there are other task dependencies that
        need to run in between this LoadDataFromSourceTaskWithKeyUsingLoadStrategy task, and the source_object_task.
        :return: A Luigi task that to create or load the source object required for the load.
        """
        return ""

    @property
    def source_object_override(self):
        """
        If this value is provided, then self.source_object_task should be left blank.

        This is the source object that will be used to generate the key, which is then used in another view which links
        the source object to the key, which is the source object for the final load.

        If self.multiple_source_systems = True, then it is assumed that the source object created will have in addition
        to the business key column, two additional columns, "originating_source_system_code" and
        "originating_business_key_name", that contain the code of the source system that the data is from, and the
        name of the field in the source that is the business key, respectively.

        :return: An existing database object, used to generate the keys for this task, and the subsequent view
        """
        return ""

    @property
    def table_prefix(self):
        return ""

    @property
    def key(self):
        return self.entity + "_sk"

    @property
    def source_object(self):
        table_prefix = self.table_prefix + "_" if self.table_prefix else ""
        return self.config_manager.staging + "." + table_prefix + self.entity + "_v"

    @property
    def key_manager(self):
        return None  # Overwrite to use a specific key manager

    @property
    def add_dimension_metadata_to_target(self):
        """
        If the target is created on the fly, this property sets whether the dimension metadata columns are added
        to the table. If no table is created on the fly this property is ignored

        :return: True if dimension metadata should be added, other wise False
        """
        return False


class LoadTableWithKeyFromScriptWithSeparateKeyScript(LoadDataFromSourceTaskWithKeyUsingLoadStrategy):
    """
    This class loads data into a table using a load strategy, with a generated key that is created using the Key
    management classes. The execution process to perform this is as follows.

    1. self.source_object_task runs, creating the database object used to drive key generation.
    2. CreateSourceObjectViewWithKey runs, firstly generating the keys required, and then creating a view that links
    the keys with the database object created in step 1.
    3. This task runs, loading data from the source object created in step 2 using the specified load strategy.

    """

    @abc.abstractproperty
    def filename_for_key_generator_input_view(self):
        return ""

    @abc.abstractproperty
    def filename_for_intermediate_view(self):
        return ""

    @abc.abstractproperty
    def object_name_for_key_generator_input_view(self):
        return ""

    @abc.abstractproperty
    def object_name_for_intermediate_view(self):
        return ""

    def requires(self):
        source_view_with_key = CreateIntermediateViewWithKey(run_timestamp=self.run_timestamp,
                                                             multiple_source_systems=self.multiple_source_systems,
                                                             originating_business_key_name=self.originating_business_key_name,
                                                             originating_source_system_code=self.originating_source_system_code,
                                                             entity=self.entity, key_table_prefix=self.table_prefix,
                                                             stage_name_param=self.stage_name,
                                                             key=self.key, key_manager=self.key_manager,
                                                             filename_for_intermediate_view=self.filename_for_intermediate_view,
                                                             filename_for_key_generator_input_view
                                                             =self.filename_for_key_generator_input_view,
                                                             object_name_for_intermediate_view=self.source_object,
                                                             object_name_for_key_generator_input_view
                                                             =self.object_name_for_key_generator_input_view,
                                                             instance_module_location_override=self.instance_module_location
                                                             )
        tasks = [source_view_with_key]

        if self.does_table_exist():
            tasks += self.table_operations.generate_tasks_to_align_columns(self.run_timestamp,
                                                                           self.stage_name, self.source_object,
                                                                           self.target_table, True)
        else:
            tasks.append(
                CreateEmptyTableFromSourceObject(source_object=self.source_object, target_name=self.target_table,
                                                 run_timestamp=self.run_timestamp, required_task=source_view_with_key))
        return tasks


class LoadTableWithKeyFromScript(LoadDataFromSourceTaskWithKeyUsingLoadStrategy):
    @property
    def task_comments(self):
        return "Load " + self.target_table

    @abc.abstractproperty
    def source_view(self):
        return None

    def source_object_task(self):
        return CreateViewFromScriptGivenFilename(run_timestamp=self.run_timestamp, filename_param=self.source_view,
                                                 stage_name_param=self.stage_name,
                                                 object_name_param="{}.{}".format(self.config_manager.staging,
                                                                                  self.source_view),
                                                 instance_module_location_override=self.instance_module_location,
                                                 _drop_view=True)

    @property
    def load_strategy(self):
        return "upsert"

    @property
    def multiple_source_systems(self):
        return False

    @property
    def unique_columnsets(self):
        return [self.key]


class ExecuteSQLForEachTableInSchema(RunDatabaseQuery):
    schemas = luigi.Parameter()

    @property
    def object_name(self):
        return None

    @property
    def pre_task_sql(self):
        return ""

    @property
    def query(self):
        query = ""

        schema_list = str(self.schemas).split(",")

        for schema in schema_list:
            query += "\n\n"
            table_query = DatabaseQuery(
                "select table_name from INFORMATION_SCHEMA.TABLES where table_schema = '{}';".format(schema))
            tables = table_query.result()
            for row in tables:
                query += self.sql_to_run("{}.{}".format(schema, row[0])) + ";\n"

        return self.pre_task_sql + query

    @staticmethod
    def sql_to_run(table):
        return ""


class DataExistsInObjectTarget(luigi.Target):
    '''
    Use this class to use the existence of data as the output(), instead of using the control table.

    The control table should still be logged to for auditing purposes. This typically happens automatically for any task
    that subclasss a DomainTask and doesn't overwrite the run() method.
    '''

    def __init__(self, db_object, column, value, connection_override=None, **kwarg):
        super(DataExistsInObjectTarget, self).__init__(**kwarg)
        if not connection_override:
            self._connection = connection_override
        self.db_object = db_object
        self.column = column
        self.value = value

    def exists(self):

        exists_qry = DatabaseQuery('select {column} from {db_object} where {column} = \'{value}\' limit 1'.format(
            column=self.column, db_object=self.db_object, value=self.value))
        result = exists_qry.result(self._connection)
        if not result:
            return False
        return True

