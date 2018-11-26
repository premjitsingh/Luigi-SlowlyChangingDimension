__description__ = '''
==== OVERVIEW ====

ORIGINAL AUTHOR Premjit Singh
PURPOSE         All generic core classes that are used across the various stages of loading the data warehouse.
NOTES           None at this time.

==================
'''

import abc
import codecs
from datetime import datetime, date, timedelta
from luigi import notifications, task
import luigi
import psycopg2
from framework.core.datahouse_common_functions import get_logger, slice_timestamp_to_date_string, treat_as_prod_environment
from framework.core.config_manager import ConfigManager
import snowflake.connector

DEFAULT_S3_KEY_DATE_FORMAT = "%Y%m%d"

# Handle character set for Windows
codecs.register(lambda name: codecs.lookup('utf-8') if name == 'cp65001' else None)


class EnvironmentConfig(object):
    """
    This class loads the environment information from the Luigi configuration file.

    This class loads environment information from the Luigi configuration file, and provides numerous properties that
    can be used to access different data stores with ease, such as S3, and Redshift schemas.
    ------ SECTION BELOW MAY NO LOGER BE APPLICABLE TO SNOWFLAKE -------
    A switch is provided in the configuration file that allows non-production environments running on the Redshift
    production box to make use of the production schemas for testing purposes. See the switches 'force-prod-sources'
    and 'force-prod-core'. This switches can be used to facilitate testing, e.g. test core layer changes using the
    production clean data, or test datamart tables using the core and clean layers.

    """
    def __init__(self, sf_cluster_section):

        self.snow_user = luigi.configuration.get_config().get(sf_cluster_section, 'snow_user')
        self.snow_password = luigi.configuration.get_config().get(sf_cluster_section, 'snow_password')
        self.snow_account = luigi.configuration.get_config().get(sf_cluster_section, 'snow_account')
        self.snow_database= luigi.configuration.get_config().get(sf_cluster_section, 'snow_database')
        self.snow_warehouse = luigi.configuration.get_config().get(sf_cluster_section, 'snow_warehouse')
        self.snow_schema = luigi.configuration.get_config().get(sf_cluster_section, 'snow_schema')
        self.maker_table = luigi.configuration.get_config().get(sf_cluster_section, 'marker-table')

        self.s3_bucket = 'your bucket'
        self.s3_bucket_path = "s3://" + self.s3_bucket
        self.s3_host = 'your host'
        self.s3_access_key_id = luigi.configuration.get_config().get('s3', 'aws_access_key_id')
        self.s3_secret_access_key = luigi.configuration.get_config().get('s3', 'aws_secret_access_key')

        self.directory_root = luigi.configuration.get_config().get('domain', 'dir-root')

        self.sf_user = luigi.configuration.get_config().get('salesforce', 'user_name')
        self.sf_password = luigi.configuration.get_config().get('salesforce', 'password')
        self.sf_security = luigi.configuration.get_config().get('salesforce', 'security_token')
        self.sf_sandbox = luigi.configuration.get_config().get('salesforce', 'sandbox')

        super(EnvironmentConfig, self).__init__()


class DomainEnvironment(object):
    """
    This class contains environmental configuration environment, and some environment related task helpers.
    """

    def __init__(self, sf_cluster_section='snowflake'):
        sf_cluster_section = 'snowflake' if not sf_cluster_section else sf_cluster_section
        self.sf_cluster_section = sf_cluster_section
        self.env_config = EnvironmentConfig(self.sf_cluster_section)

    def get_bucket(self, s3_bucket_overwrite=None):
        from boto.s3.connection import S3Connection

        if s3_bucket_overwrite:
            s3_bucket = s3_bucket_overwrite
        else:
            s3_bucket = self.env_config.s3_bucket

        conn = S3Connection(self.env_config.s3_access_key_id, self.env_config.s3_secret_access_key, is_secure=False,
                            host=self.env_config.s3_host)
        return conn.get_bucket(s3_bucket)

    @staticmethod
    def s3():
        """
        Return an S3 resource configured for the Domain environment.

        :return: S3 resource configured for the Domain environment.
        """
        from boto3.session import Session
        session = Session(aws_access_key_id=luigi.configuration.get_config().get('s3', 'aws_access_key_id'),
                          aws_secret_access_key=luigi.configuration.get_config().get('s3', 'aws_secret_access_key'),
                          region_name='ap-southeast-2')
        return session.resource('s3')

    def get_cursor(self, search_schema=None, connection=None, autocommit=False):
        if not connection:
            connection = self.connect()
            connection.autocommit = autocommit
        curs = connection.cursor()
        return curs

    def connect(self):

        connection = snowflake.connector.connect(  user= self.env_config.snow_user,
                                                   password=self.env_config.snow_password,
                                                   account=self.env_config.snow_account,
                                                   warehouse=self.env_config.snow_warehouse,
                                                   database=self.env_config.snow_database,
                                                   schema=self.env_config.snow_schema
                                                   )
        return connection


class DomainTask(luigi.Task):
    """
    Root task used by all custom Domain tasks, unless an alternative Luigi class was used.

    This class provides access to Domain's environment, processes the timestamp,and configures the logging / task
    output approach used for Datahouse, which is to write task completion data to the control.luigi_datahouse_control
    table.

    To run a test at the completion of a task, overwrite the test() method with a Test class. See tests.py for more
    information

    """
    run_timestamp = luigi.Parameter(default="")
    load_type = luigi.Parameter(default='load', significant=False)  # load/reload (Use reload to rerun completed tasks)
    instance_module_location_override = luigi.Parameter(default='', significant=False)

    test_object = None

    def __init__(self, *args, **kwargs):
        super(DomainTask, self).__init__(*args, **kwargs)
        self.config_manager = ConfigManager(self.run_timestamp)

    @property
    def domain(self):
        return DomainEnvironment()

    # Should override task_comments where applicable
    @property
    def task_comments(self):
        return "Default comment"

    def task_log(self):
        domain = DomainEnvironment("snowflake")  # Task log records should always be written to default RS cluster
        return JobCompletedLogRecord(   snow_user=domain.env_config.snow_user,
                                        snow_password=domain.env_config.snow_password,
                                        snow_database=domain.env_config.snow_database,
                                        snow_account=domain.env_config.snow_account,
                                        snow_warehouse=domain.env_config.snow_warehouse,
                                        snow_schema=domain.env_config.snow_schema,
                                        table=self.task_comments,
                                        update_id=str(self),
                                        run_timestamp=self.run_timestamp
                                    )

    def output(self):
        """
        The default output for a DomainTask is the JobCompletedLogRecord stored in the
        control.luigi_datahouse_control table, therefore completion of a task is by default indicated by the presence
        of a record in that table with the applicable Task name and parameters.

        Overwrite this method when generating files, or other external output.

        :return: Luigi target that is created by the task.
        """
        return self.task_log()

    def log_task(self, connection=None):
        """
        Log the successful completion of the task to a db.

        This method writes to the task_log() target completion details of the task. It is called as part of the
        default run() method. This method should be called manually when overwriting the run() method, even when a
        different output() is defined.

        :param connection: The connection to the db to use when logging the task completion. This is now obsolete as
                           all log file records should be written to the redshift_control connection.
        :return: Nothing
        """
        self.task_log().touch()

    def run(self):
        if self.pre_run_tasks() and self.execute_pre_run_tasks:
            yield self.pre_run_tasks()

        connection = self.domain.connect()

        if self.test():
            self.test().run_test(connection)
        self.log_task(connection)

    @property
    def stage_name(self):
        return ""

    @property
    def tasks(self):
        from framework.stage_core.s3_to_clean_core import S3toCleanStageTaskFactory
        return S3toCleanStageTaskFactory.load(self.stage_name)

    def test(self):
        """
        Overwrite this test method to return a Test class, to implement a test upon task completion.
        """
        return False

    @property
    def query_helper(self):
        from database_connection import QueryHelper
        return QueryHelper(self.instance_module_location)

    @property
    def instance_module_location(self):
        """
        Return the directory containing the instance class, to allow scripts to be picked up from the correct path.

        :return: The file path (directory only) of the instance class
        """
        import inspect
        import os
        if self.instance_module_location_override == '':
            return os.path.dirname(inspect.getfile(self.__class__))
        else:
            return self.instance_module_location_override

    @property
    def processed_timestamp(self):
        """
        Process timestamp so all tasks are run once per day, unless reload requested.

        :return: Timestamp converted to date for 'load', otherwise current timestamp
        """
        if self.config_manager.test_config("test_mode"):
            return self.run_timestamp
        return slice_timestamp_to_date_string(self.run_timestamp) if self.load_type == 'load' else self.run_timestamp

    @property
    def logger(self):
        return get_logger()

    def pre_run_tasks(self):
        pass

    @property
    def execute_pre_run_tasks(self):
        return False

    @property
    def email_on_success(self):
        return False


@DomainTask.event_handler(luigi.Event.FAILURE)
def mourn_failure(task, exception):
    """Will be called directly after a failed execution
    of `run` on any DomainTask subclass
    """
    get_logger().exception("Failure in task: " + str(task) + "\nException is: " + str(exception))


@luigi.Task.event_handler(luigi.Event.SUCCESS)
def celebrate_success(task_param):
    """Will be called directly after a successful execution
       of `run` on any Task subclass (i.e. all luigi Tasks)
    """
    task_id_str = task.Task.__repr__(task_param)
    if hasattr(task_param, 'email_on_success') and task_param.email_on_success:
        # Send to same email list that error emails go out on
        notifications.send_error_email("SUCCESS: " + task_id_str, task_id_str +
                                       " completed at " + str(datetime.now()))


class JobCompletedLogRecord(luigi.Target):
    """
    This class persists task completion details for Datahouse jobs into control.luigi_datahouse_control.

    The log_task() method on DomainTask writes to this job completed target as part of the default run(). The log_task
    method should be called manually when overwriting the run() method.

    """
    domain = DomainEnvironment()
    marker_table=domain.env_config.maker_table
    use_db_timestamps = False

    def __init__(self, *args, **kwargs):
        run_timestamp       = kwargs.pop('run_timestamp')
        self.snow_user      = kwargs.get('snow_user')
        self.snow_password  = kwargs.get('snow_password')
        self.snow_account   = kwargs.get('snow_account')
        self.snow_warehouse = kwargs.get('snow_warehouse')
        self.snow_database  = kwargs.get('snow_database')
        self.snow_schema    = kwargs.get('snow_schema')
        self.table          = kwargs.get('table')
        self.update_id      = kwargs.get('update_id')

        #super(JobCompletedLogRecord, self).__init__(*args, **kwargs)
        self.config_manager = ConfigManager(run_timestamp)

    def connect(self):
        """
        Get a psycopg2 connection object to the database where the table is.
        """
        connection = snowflake.connector.connect(user= self.snow_user,
                                                 password=self.snow_password,
                                                 account=self.snow_account,
                                                 warehouse=self.snow_warehouse,
                                                 database=self.snow_database,
                                                 schema=self.snow_schema
                                                 )
        return connection

    def touch(self, connection=None):
        """
        Mark this update as complete.

        Important: If the marker table doesn't exist, the connection transaction will be aborted
        and the connection reset.
        Then the marker table will be created.
        """
        self.create_marker_table()

        if connection is None:
            # TODO: test this
            connection = self.connect()
            connection.autocommit = True  # if connection created here, we commit it here

        if self.use_db_timestamps:
            connection.cursor().execute(
                """INSERT INTO {marker_table} (update_id, target_table)
                   VALUES (%s, %s)
                """.format(marker_table=self.marker_table),
                (self.update_id, self.table))
        else:
            connection.cursor().execute(
                """INSERT INTO {marker_table} (update_id, target_table, inserted)
                         VALUES (%s, %s, %s);
                    """.format(marker_table=self.marker_table),
                (self.update_id, self.table,
                 datetime.datetime.now()))

    def exists(self, connection=None):
        if connection is None:
            connection = self.connect()
            connection.autocommit = True
        cursor = connection.cursor()
        try:
            sql_to_check_task_completion = """SELECT 1 FROM {marker_table} WHERE update_id = {update_id} LIMIT 1""".format(marker_table=self.marker_table, update_id=self.update_id)
            get_logger().debug("Checking for completion of task using this sql " + str(sql_to_check_task_completion))
            cursor.execute("""SELECT 1 FROM {marker_table}
                                WHERE update_id = %s
                                LIMIT 1""".format(marker_table=self.marker_table),
                                           (self.update_id,)
                                           )
            row = cursor.fetchone()
        except snowflake.connector.errors.ProgrammingError as e :
            row = None
            raise
        return row is not None

    # def __init__(self, user, password, account, warehouse, database, schema,autocommit,update_id, table):
    #     self.account = account
    #     self.warehouse = warehouse
    #     self.database = database
    #     self.user = user
    #     self.password = password
    #     self.schema =schema
    #     self.autcommit=autocommit
    #     self.table = table
    #     self.update_id = update_id


    @property
    def marker_table(self):
        return self.config_manager.control + "." + luigi.configuration.get_config().get(self.domain.sf_cluster_section, 'marker-table')

    def touch(self, connection=None):
        """
        Mark this update as complete.

        Important: If the marker table doesn't exist, the connection transaction will be aborted
        and the connection reset.
        Then the marker table will be created.
        """
        if not self.config_manager.is_prod():
            self.create_marker_table()

        if connection is None:
            # TODO: test this
            connection = self.connect()
            connection.autocommit = True  # if connection created here, we commit it here

        if self.use_db_timestamps:
            connection.cursor().execute("""INSERT INTO {marker_table} (update_id, related)
                                           VALUES (%s, %s)
                                           """.format(marker_table=self.marker_table), (self.update_id, self.table)
                                        )
        else:
            connection.cursor().execute(
                """
                            --begin transaction;
                            --lock table {marker_table};
                            INSERT INTO {marker_table} (update_id, related, inserted)
                            VALUES (%s, %s, %s);
                            --end transaction;
                                    """.format(marker_table=self.marker_table), (self.update_id, self.table, datetime.now())
            )

        # make sure update is properly marked
        assert self.exists(connection)
        connection.close()

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.

        Using a separate connection since the transaction might have to be reset.
        """
        connection = self.connect()
        connection.autocommit = True
        cursor = connection.cursor()
        if self.use_db_timestamps:
            sql = """ CREATE TABLE {marker_table} (
                      update_id VARCHAR(max) PRIMARY KEY,
                      related TEXT,
                      inserted TIMESTAMP DEFAULT NOW())
                """.format(marker_table=self.marker_table)
        else:
            sql = """ CREATE TABLE {marker_table} (
                      update_id VARCHAR(max) PRIMARY KEY,
                      related TEXT,
                      inserted TIMESTAMP);
                  """.format(marker_table=self.marker_table)
        try:
            cursor.execute(sql)
        except psycopg2.ProgrammingError as e:
            if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
                pass
            else:
                raise
        connection.close()

    def open(self, mode):
        raise NotImplementedError("Cannot open() JobCompletedLogRecord")


class RunDailyTask(DomainTask):
    """
    This abstract task builds a task dependency graph, that requires the specified daily_task() be executed every
    day for the last 14 days.

    It is intended to be used for tasks that require data loaded on a daily basis (e.g. daily snapshots). To use this
    task, overwrite the daily_task method to return the task that performs the daily task, ensure that task
    has a date parameter, and the dt parameter is passed to that task.

    The days_back property can be overwritten for historical loads.
    """

    @abc.abstractmethod
    def daily_task(self, dt, dependent_task=None, *args, **kwargs):
        return None

    @property
    def days_back(self):
        if self.config_manager.is_prod():
            return 30
        else:
            return 2

    def requires(self):
        dt = date.today() + timedelta(days=-self.days_back)
        if self.parallel_load:
            tasks = []
            while dt < date.today():
                if self.config_manager.test_config('test_mode'):
                    tasks.append(self.daily_task(dt, run_timestamp=self.run_timestamp))
                else:
                    tasks.append(self.daily_task(dt))
                dt = dt + timedelta(days=1)
            return tasks
        else:
            next_task = None
            while dt < date.today():
                if not next_task:
                    if self.config_manager.test_config('test_mode'):
                        next_task = self.daily_task(dt, run_timestamp=self.run_timestamp)
                    else:
                        next_task = self.daily_task(dt)
                else:
                    prior_task = next_task
                    if self.config_manager.test_config('test_mode'):
                        next_task = self.daily_task(dt, prior_task, run_timestamp=self.run_timestamp)
                    else:
                        next_task = self.daily_task(dt, prior_task)
                dt = dt + timedelta(days=1)
            return next_task

    @property
    def parallel_load(self):
        """
        Can they daily tasks be executed in parallel, or should they be run in date ascending order.
        :return: Return True if tasks can be executed in parallel.
        """
        return True


class FrameworkToCompletePriorRunsBeforeStartingNewRun(DomainTask):
    '''
    This framework task ensures any prior started runs are completed, before a new run of a task is begun,
    by confirming whether prior runs generated their output. If a prior task needs to be resumed, it is done
    by loading their state from a stored text file on S3.

    It is especially useful when a process' state can change from run to run. For e.g. A random selection of tables
    have maintenance performed on them. If the maintenance process is interrupted it is important the tables under
    maintenance have that process completed before a new set of tables is selected.

    How to use: Subclass this class, and set the task to run, and the stage name. It is expected that the task to run
    stores neccessary state information in S3 at the worklist path, allowing the state to resume.

    The worklist path that is generated has the format
    <worklist path base>/<stage name>/run_timestamps/><date>/<run timestamp>/, e.g.
    "datatech/dg/WorkflowManagement/deep_copy/run_timestamps/20180115/2018-01-1511:45:30.976661/". The worklist path
    is generated automatically based properties of the class. The files to store state should be stored at the worklist
    path, however the creation and reading of the state is performed by the process to run, so there is no
    prescriptive format required.

    Example: To see how this class is used, see the deep_copy_process.py
    '''

    @property
    def base_worklist_path(self):
        return self.config_manager.get_s3_location('s3_location_luigi_workflow_management') + self.stage_name + "/run_timestamps/"

    def requires(self):
        import re
        tasks = []

        # Check S3 for prior runs, and run them. Any prior runs not finished will be completed.
        dt = date.today() + timedelta(days=-int(self.days_back))
        client = self.domain.s3().meta.client
        paginator = client.get_paginator('list_objects')
        while dt <= date.today():
            date_string = dt.strftime(DEFAULT_S3_KEY_DATE_FORMAT)
            dt = dt + timedelta(days=1)
            folder_path = self.base_worklist_path + date_string
            for result in paginator.paginate(Bucket=self.domain.env_config.s3_bucket,
                                             Prefix=folder_path):
                if "Contents" in result:
                    for key in result["Contents"]:
                        m = re.search(folder_path + '/([0-9\.\-:]+)/(.+)$', key["Key"])
                        if m:
                            # timestamp found
                            tasks.append(self.task(run_timestamp=m.group(1),
                                                   worklist_path=self.base_worklist_path + date_string + "/" + m.group(1) + "/"))
                        else:
                            raise Exception("Can't find timestamp in path - key: " + key['Key'])

        return tasks

    def run(self):
        # Start a new run with the current timestamp
        yield self.task(run_timestamp=self.run_timestamp,
                        worklist_path=self.base_worklist_path + date.today().strftime(DEFAULT_S3_KEY_DATE_FORMAT) + "/" + self.run_timestamp + "/")
        self.log_task()

    @property
    def days_back(self):
        return 14

    @abc.abstractmethod
    def task(self, **kwargs):
        '''
        Overwrite this method to return the Task you wish to execute. See below notes for requirements.

        1. Ensure your Task defines a luigi Parameter named 'worklist_path', and use it to store/read files required
        for the execution

        2. Ensure you include **kwargs in the method definition, and in the return statement. See example below.

        :return: A Luigi Task with required parameters, excluding run_timestamp, but including **kwargs
        '''
        return DomainTask(example_parameter=self.example_property, **kwargs)

    @property
    def task_comments(self):
        return "Ensure prior executions are finalised before starting new run"


class DummyTask(luigi.Task):

    def complete(self):
        return True