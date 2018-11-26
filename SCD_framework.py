__description__ = '''
==== OVERVIEW ====

    The default names of few columns are as below. If there is a requirement to have a specific name, override the column names
        1. business key or primary key :-  Can be only one column and by default it's entity_name_cid
        2. current_ind_column :- row_current_flag
        3. end_date_column :- row_expiry_date
        4. update_date_column :- row_update_date
        5. Key map table name :- keys.map_dim_entity_name
        6. Key table name :- keys.keys_dim_entity_name
        7. Target table name :- datamart.dim_entity_name
        7. Source table name :- core.core_entity_name
        8. Surrogate key name :- dim_entity_name_sk

==================
'''

import luigi
import abc
from datetime import datetime
from database_connection import RunDatabaseQuery, TableOperations, DomainTask, AddColumnToTable


class LoadUsingSCD(DomainTask):
    """
    This class is the start of the SCD process, over ride the properties to suit your need

    """
    run_timestamp = luigi.Parameter(default=str(datetime.now()).replace(" ", ""))

    @property
    def table_operations(self):
        return TableOperations(self.run_timestamp)

    @property
    def task_comments(self):
        return "Load {} dimension using SCD".format(self.entity_name)

    @abc.abstractproperty
    def scd2_column(self):
        return ""

    @abc.abstractproperty
    def entity_name(self):
        return ""

    @property
    def originating_source_system_code(self):
        # Populated in key table
        return "core"

    @property
    def surrogate_key(self):
        # Dim table surrogate key name
        return "dim_{}_sk".format(self.entity_name)

    @property
    def dimension_table(self):
        return self.target_schema + "." + self.target_table

    @property
    def staging_dimension_table(self):
        return self.staging_schema + "." + self.target_table

    @property
    def staging_source_table(self):
        return self.staging_schema + "." + self.source_table

    @property
    def staging_schema(self):
        # Used to create copies of source and target tables for SCD process, Tables are dropped after successful run
        return self.config_manager.staging

    @property
    def source_table(self):
        # Usually starts with core. If the name is different override this or pass as parameter while calling this class
        return "core_{}".format(self.entity_name)

    @property
    def target_table(self):
        # Usually starts with dim. If the name is different override this or pass as parameter while calling this class
        return "dim_{}" .format(self.entity_name)

    @property
    def source_schema(self):
        # Usually core schema; if the name is different override this or pass as parameter while calling this class
        return self.config_manager.core

    @property
    def target_schema(self):
        # Usually datamart schema; if the name is different override this or pass as parameter while calling this class
        return self.config_manager.datamart

    @property
    def business_key(self):
        # Usually entity_name_cid; if the name is different over ride this or pass as parameter while calling this class
        return self.entity_name+"_cid"

    @property
    def current_ind_column(self): # Current flag column in the target table
        return "row_current_flag"

    @property
    def end_date_column(self): # Expiry date column in target table
        return "row_expiry_timestamp"

    @property
    def start_date_column(self): # Start date column in target table
        return "row_effective_timestamp"

    @property
    def update_date_column(self): # Update date column in the target table to record scd1 update date
        return "row_update_timestamp"

    @property  # Combination of columns which uniquely generates the surrogate key
    def originating_business_key_name(self):
        return self.business_key + '||' + self.start_date_column

    @property  # key table name
    def keys_table(self):
        return "{}.keys_dim_{}".format(self.config_manager.keys, self.entity_name)

    @property  # Key map table name
    def keys_map_table(self):
        return "{}.map_dim_{}".format(self.config_manager.keys, self.entity_name)

    @property
    def key(self):  # Surrogate key name in the target table
        return "dim_{}_sk".format(self.entity_name)

    @property  # Factory method
    def key_factory_manager(self):
        return ''

    @property  # staging target table which is the source for generating the sk
    def source_object(self):
        return self.source_schema + "." + self.source_table

    @property
    def default_end_date(self):
        return "TO_DATE('01012400','DDMMYYYY')"

    @property
    def task_comments(self):
        return "Load Using SCD"

    @property
    def object_name(self):
        return self.dimension_table

    def run(self):
        yield self.pre_run_tasks()

        yield LoadFinalTargetTable(run_timestamp= self.run_timestamp,
                                   dimension_table=self.dimension_table,
                                   staging_dimension_table=self.staging_dimension_table,
                                   staging_source_table=self.staging_source_table,
                                   source_object = self.source_object,
                                   surrogate_key= self.surrogate_key,
                                   scd2_column= self.scd2_column,
                                   business_key= self.business_key,
                                   current_ind_column= self.current_ind_column,
                                   end_date_column= self.end_date_column,
                                   start_date_column=self.start_date_column,
                                   update_date_column=self.update_date_column,
                                   entity_name = self.entity_name,
                                   originating_source_system_code=self.originating_source_system_code,
                                   originating_business_key_name = self.originating_business_key_name,
                                   keys_table = self.keys_table,
                                   keys_map_table = self.keys_map_table,
                                   key = self.key,
                                   key_manager = self.key_factory_manager,
                                   default_end_date = self.default_end_date
                                   )

        self.log_task()


    def pre_run_tasks(self):
        tasks = []
        if self.does_table_exist():
            tasks += self.table_operations.generate_tasks_to_align_columns(self.run_timestamp,
                                                                           self.stage_name, self.source_object,
                                                                           self.dimension_table, True)
            yield tasks

        else:
            yield AddSkeyTODimTable(source_object=self.source_object,
                                    datatype="bigint",
                                    tablename=self.dimension_table,
                                    relevant_stage_name="datamart",
                                    column=self.key,
                                    run_timestamp=self.run_timestamp
                                    )


    def does_table_exist(self):
        """
        Determine whether the table already exists.
        """
        schemaname, tablename = self.object_name.split('.')
        query = ("select 1 as table_exists from information_schema.tables where lower(table_name) = lower('{}') and lower(table_schema)= lower('{}') limit 1").format(tablename,schemaname)
        cursor = self.domain.get_cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            return bool(result)
        finally:
            cursor.close()

    @property
    def execute_pre_run_tasks(self):
        return True

class AddSkeyTODimTable(AddColumnToTable):
    source_object = luigi.Parameter()

    def requires(self):
        from database_connection import CreateEmptyTableFromSourceObject
        return CreateEmptyTableFromSourceObject(source_object=self.source_object,
                                               target_name=self.tablename,
                                               run_timestamp=self.run_timestamp,
                                               add_dimension_metadata=True
                                               )


class LoadFinalTargetTable(RunDatabaseQuery):
    """
        This class loads final Target table from the staging target table once all the tests are passed.
        Surrogate key is also generated here and populated in the final table. Surrogate key is generated based on core id (cid) and effective start date instead of only cid to generate new skey for SCD Type 2 records

    """

    surrogate_key = luigi.Parameter()
    business_key = luigi.Parameter()  # Primary keys
    scd2_column = luigi.Parameter()
    current_ind_column = luigi.Parameter()  # Current flag
    end_date_column = luigi.Parameter()  # Expiry Date-Used to record SCD2 changes
    start_date_column = luigi.Parameter()  # Start Date-Used to record SCD2 changes
    update_date_column = luigi.Parameter(default='')  # Updated Date - Used to record SCD1 changes
    dimension_table = luigi.Parameter()
    staging_dimension_table = luigi.Parameter()
    staging_source_table = luigi.Parameter()
    source_object = luigi.Parameter()
    originating_source_system_code = luigi.Parameter()
    entity_name = luigi.Parameter()
    originating_business_key_name = luigi.Parameter()
    keys_table = luigi.Parameter()
    keys_map_table = luigi.Parameter()
    key = luigi.Parameter()
    key_manager = luigi.Parameter()
    default_end_date = luigi.Parameter()

    @property
    def table_operations(self):
        return TableOperations(self.run_timestamp)

    @property
    def object_name(self):
        return 'InsertFinalTargetTable_setp_5'

    @property
    def stage_name(self):
        return "SCD"

    def requires(self):
        return InsertSCD2AndNewRecords(run_timestamp=self.run_timestamp,
                                       dimension_table=self.dimension_table,
                                       staging_dimension_table=self.staging_dimension_table,
                                       staging_source_table=self.staging_source_table,
                                       source_object = self.source_object,
                                       surrogate_key=self.surrogate_key,
                                       scd2_column=self.scd2_column,
                                       business_key=self.business_key,
                                       current_ind_column=self.current_ind_column,
                                       end_date_column=self.end_date_column,
                                       start_date_column=self.start_date_column,
                                       update_date_column=self.update_date_column,
                                       originating_source_system_code=self.originating_source_system_code,
                                       entity_name=self.entity_name,
                                       default_end_date=self.default_end_date
                                       )

    @property
    def query(self):
        # Get all the columns as a list of tuples
        target_table_columns = self.table_operations.get_all_columns(self.dimension_table.split('.')[0], self.dimension_table.split('.')[1])

        # Convert the list of tuples into comma separated string
        insert_columns_list = []  # Declare an empty list
        for item in target_table_columns:
            insert_columns_list.append(item[0])
        insert_column_str = "\n\t,".join(insert_columns_list)
        sql = '''
--------------------------------------------STEP 5 - Insert into final target table---------------------------------------------------------'
DELETE FROM {dimension_table} WHERE {sk} >= 0;
INSERT INTO {dimension_table}
(
'''.format(dimension_table=self.dimension_table, sk=self.surrogate_key)

        sql += insert_column_str

        # Formation of SELECT clause
        select_columns_list = []
        for item in target_table_columns:
            select_columns_list.append(item[0])
        select_columns_str = "\n\t,src.".join(select_columns_list)
        select_columns_str = "src."+select_columns_str #append src. for the fist column

        select_columns_str = select_columns_str.replace('src.' + self.surrogate_key,'key_tab.' + self.surrogate_key)  # replace src.sk with key_tab.sk

        sql += '''
)
{select} {columns}
FROM {staging_dimension_table} src
JOIN {key_table} key_tab ON key_tab.originating_source_business_key = src.{business_key}||src.{start_dt};

DROP TABLE {staging_dimension_table};
DROP TABLE {staging_source_table};
'''.format(columns=select_columns_str,
           key_table=self.keys_table,
           business_key=self.business_key,
           start_dt=self.start_date_column,
           staging_dimension_table=self.staging_dimension_table,
           staging_source_table=self.staging_source_table,
           select=self.config_manager.select())

        return sql

    # Surrogate key generation using the source table
    @property
    def key_generator(self):
        from framework.core.key_management import KeyGenerator
        return KeyGenerator(run_timestamp=self.run_timestamp,
                            originating_business_key_name=self.originating_business_key_name,
                            originating_source_system_code=self.originating_source_system_code,
                            source_object=self.staging_dimension_table,
                            keys_table=self.keys_table,
                            keys_map_table=self.keys_map_table,
                            key=self.key,
                            key_manager=self.key_manager
                            )


class InsertSCD2AndNewRecords(RunDatabaseQuery):
    """
        This class loads the changed Type 2 and new records with exp date set to future date and current flag as True from the staging source.
        Surrogate key is not populated in the staging target table
    """

    surrogate_key = luigi.Parameter()
    business_key = luigi.Parameter()  # Primary keys
    scd2_column = luigi.Parameter()
    current_ind_column = luigi.Parameter()  # Current flag
    end_date_column = luigi.Parameter()  # Expiry Date-Used to record SCD2 changes
    start_date_column = luigi.Parameter()  # Start Date-Used to record SCD2 changes
    update_date_column = luigi.Parameter(default='')  # UUpdated Date - Used to record SCD1 changes
    dimension_table = luigi.Parameter()
    staging_dimension_table = luigi.Parameter()
    staging_source_table = luigi.Parameter()
    source_object = luigi.Parameter()
    run_timestamp = luigi.Parameter(default=str(datetime.now()).replace(" ", ""))
    entity_name = luigi.Parameter()
    originating_source_system_code = luigi.Parameter()
    default_end_date = luigi.Parameter() # Default Expiry date

    @property
    def table_operations(self):
        return TableOperations(self.run_timestamp)

    @property
    def object_name(self):
        return self.staging_dimension_table

    @property
    def stage_name(self):
        return "SCD"

    def requires(self):
        return ProcessSCD1Records(run_timestamp= self.run_timestamp,
                                  dimension_table=self.dimension_table,
                                  staging_dimension_table=self.staging_dimension_table,
                                  staging_source_table=self.staging_source_table,
                                  source_object=self.source_object,
                                  surrogate_key=self.surrogate_key,
                                  scd2_column=self.scd2_column,
                                  business_key=self.business_key,
                                  current_ind_column=self.current_ind_column,
                                  end_date_column=self.end_date_column,
                                  update_date_column=self.update_date_column,
                                  default_end_date=self.default_end_date)

    @property
    def query(self):

        # Forms a string having all the column names of the source table. Other columns like expiry_date, current_flag are added manually below to make it similar to the target table
        table_columns_list = self.table_operations.get_all_columns(self.source_object.split('.')[0], self.source_object.split('.')[1])

        source_columns_list = []
        for item in table_columns_list: # convert the list of tuples to list only
            source_columns_list.append(item[0])
        insert_columns_str = "\n\t,".join(source_columns_list) # forms a comma seperated insert string
        select_column_str  = "\n\t,src.".join(source_columns_list) # forms a comma seperated select string
        select_column_str = select_column_str.replace('src.etl_updated_timestamp',"CURRENT_TIMESTAMP AS etl_updated_timestamp")

        sql= '''
---------------------------------------------STEP 4 - Insert type 2 and New records---------------------------------------------------'
-- Formation of Insert Into staging target table statement.Columns list is the source columns. Update column is for SCD 1 records only, so not updating this column for SCD 2 and new records
INSERT INTO {staging_dimension_table}
(
    {insert_columns_str}
    ,{sk}
    ,{active_flag}
    ,{start_date}
    ,{end_date}
)
{select} src.{select_column_str}
    ,null AS {sk}
    ,TRUE AS {active_flag}
    ,NVL(dateadd(microsecond, 1, tgt.max_row_expiry_timestamp), '1970-01-01' ) AS {start_date}
    ,{default_end_date} AS {end_date}
FROM {staging_source_table} src
LEFT JOIN (select {bks}, max({end_date}) max_row_expiry_timestamp from {staging_dimension_table} group by {bks}) tgt ON 1 = 1
'''.format(staging_dimension_table=self.staging_dimension_table,
           insert_columns_str=insert_columns_str,
           sk=self.surrogate_key,
           active_flag=self.current_ind_column,
           start_date=self.start_date_column,
           end_date=self.end_date_column,
           select_column_str=select_column_str,
           default_end_date=self.default_end_date,
           staging_source_table=self.staging_source_table,
           bks=self.business_key,
           select=self.config_manager.select())

        # Loop through the list of primary key to form the where clause columns comparison
        for pk_col in self.business_key.split(','):
            sql += ' AND src.{pk_col} = tgt.{pk_col}'.format(pk_col=pk_col)

        return sql

    @property
    def unique_columnsets(self):
        columns = self.business_key+","+self.start_date_column
        return [columns]

    def test(self):

        start_date_test_sql = ''' -- Test to validate start date is always less than end date
                                    SELECT 1 FROM {target_table} WHERE {start_date} >= {end_date} AND {active_flag} = TRUE LIMIT 1'''.format(target_table=self.staging_dimension_table, start_date=self.start_date_column, end_date=self.default_end_date, active_flag=self.current_ind_column)

        end_date_test_sql = ''' -- Test to check if any active records's end date is not set properly to the end date
                                    SELECT 1 FROM {target_table} WHERE {current_flag} = TRUE AND {exp_date} != {default_exp_date} LIMIT 1'''.format(target_table=self.staging_dimension_table, current_flag=self.current_ind_column, exp_date=self.end_date_column, default_exp_date=self.default_end_date)

        return MultipleTests([TestNoRecordsReturned(end_date_test_sql),
                              TestNoRecordsReturned(start_date_test_sql)]
                             )


class ProcessSCD1Records(RunDatabaseQuery):
    """
    This class processes the SCD type 1 records by updating the SCD 1 columns and Deleting it from the source table
    """
    surrogate_key = luigi.Parameter()
    business_key = luigi.Parameter()  # Primary keys
    scd2_column = luigi.Parameter()
    current_ind_column = luigi.Parameter()  # Current flag
    end_date_column = luigi.Parameter()  # Used to record SCD2 changes
    update_date_column = luigi.Parameter(default='')  # Used to record SCD1 changes
    dimension_table = luigi.Parameter()
    staging_dimension_table = luigi.Parameter()
    staging_source_table = luigi.Parameter()
    source_object = luigi.Parameter()
    default_end_date = luigi.Parameter()
    run_timestamp = luigi.Parameter(default=str(datetime.now()).replace(" ", ""))

    @property
    def table_operations(self):
        return TableOperations(self.run_timestamp)

    @property
    def object_name(self):
        return 'ProcessSCD1Records_setp_3'

    @property
    def stage_name(self):
        return "SCD"

    def requires(self):
        return RetireSCD2Records(run_timestamp= self.run_timestamp,
                                 dimension_table=self.dimension_table,
                                 staging_dimension_table=self.staging_dimension_table,
                                 staging_source_table=self.staging_source_table,
                                 source_object = self.source_object,
                                 scd2_column= self.scd2_column,
                                 business_key= self.business_key,
                                 current_ind_column= self.current_ind_column,
                                 end_date_column= self.end_date_column
                                 )

    @property
    def query(self):
        table_columns_dict = self.table_operations.get_all_columns(self.source_object.split('.')[0], self.source_object.split('.')[1])
        table_columns_str = ''

        first_element=True
        for list in table_columns_dict:
            for tuple in list:
                if first_element:
                    table_columns_str += tuple
                    first_element = False
                table_columns_str += ',' +tuple

        table_columns_str = table_columns_str.replace(',etl_updated_timestamp','') #etl_updated_timestamp is not considered in SCD1

        # Get SCD1 columns -Convert the strings into a list first and then to a set. Sets operation are easier to get the scd1 columns
        table_columns_list = table_columns_str.split(',') # Covert all columns string to a List
        table_column_set=set(table_columns_list) # Convert all columns list into set
        scd2_column_list = self.scd2_column.split(',')  # Covert SCD2 string to a List
        scd2_column_set = set(scd2_column_list) # Convert SCD2 list into set
        surrogate_key_list = self.surrogate_key.split(',') # Covert surrogate_key string to a List
        surrogate_key_set = set(surrogate_key_list) # Covert surrogate_key list to a Set
        business_key_list =  self.business_key.split(',')
        business_key_set = set(business_key_list)

        # Set operation:- Removes Surrogate key,Primary key, SCD2 columns column from all columns to get the SCD1 columns only
        scd1_column_set = table_column_set-scd2_column_set-surrogate_key_set-business_key_set


        sql = '\n-------------------------------------------STEP 3 - SCD type 1 update---------------------------------------------------------'
        sql += '\n UPDATE {} \n SET '.format(self.staging_dimension_table)

        # if update_date_column is available, record the updated date
        if self.update_date_column:
            sql += "\n\t\t{} = CURRENT_TIMESTAMP,\n".format(self.update_date_column)

        # Loop through the SCD1 columns to form the update column set
        counter = 0
        for scd1_column in scd1_column_set:
            counter =  counter+1
            if counter == scd1_column_set.__len__(): # Last column in the set
                sql += '\t\t{} = src.{} \n FROM  {} src \n WHERE '.format(scd1_column, scd1_column, self.staging_source_table)
            else:
                sql += '\t\t{} = src.{}, \n '.format(scd1_column, scd1_column)

        # Loop through the list of primary key to form the where clause columns comparison
        for pk_col in self.business_key.split(','):
            sql += '\n \t\t{}.{} = src.{} AND '.format(self.staging_dimension_table, pk_col, pk_col)

        # Add the current flag filter
        sql += '\n \t\t{}.{} = true \n AND'.format(self.staging_dimension_table, self.current_ind_column)  ;

        # Update only the changes scd 1 rows
        first_loop = True # Add a '(' at the start for the first time
        for scd1_column in scd1_column_set:
            if first_loop:
                sql += '''
(
    (( {staging_dimension_table}.{scd1_column} !=  src.{scd1_column} )
    OR (( {staging_dimension_table}.{scd1_column} IS NOT NULL ) AND (src.{scd1_column} IS NULL)) OR (( {staging_dimension_table}.{scd1_column} IS NULL ) AND (src.{scd1_column} IS NOT NULL)))
'''.format(staging_dimension_table=self.staging_dimension_table, scd1_column=scd1_column)
                first_loop = False
            else:
                sql += '''
OR (( {staging_dimension_table}.{scd1_column} !=  src.{scd1_column} )
   OR (( {staging_dimension_table}.{scd1_column} IS NOT NULL ) AND (src.{scd1_column} IS NULL)) OR (( {staging_dimension_table}.{scd1_column} IS NULL ) AND (src.{scd1_column} IS NOT NULL)))
'''.format(staging_dimension_table=self.staging_dimension_table, scd1_column=scd1_column)

        sql += '''
 ) ; -- Add the last brace


-----------------------------------------Commit-----------------------------------------------------------------------
COMMIT;

-----------------------------------------SCD type 1 delete------------------------------------------------------------
--Delete SCD 1 records from the staging.source_table after the update

DELETE FROM {}
USING(SELECT {}, {} FROM {}) tgt
WHERE '''.format(self.staging_source_table, pk_col, self.current_ind_column, self.staging_dimension_table)

        # Loop through the list of primary key to form the where clause columns comparision
        for pk_col in self.business_key.split(','):
            sql += '\n \t {}.{} = tgt.{} AND '.format(self.staging_source_table, pk_col, pk_col)

        # Add the current flag filter
        sql += '\n \t tgt.{} = true ;'.format(self.current_ind_column);

        return sql


class RetireSCD2Records(RunDatabaseQuery):
    """
    This class identifies the SCD2 records in the source table and retires them if there's a change in the source table
    """

    business_key = luigi.Parameter() # Primary keys
    scd2_column = luigi.Parameter()
    current_ind_column = luigi.Parameter() # Current flag
    end_date_column = luigi.Parameter()  # Used to record SCD2 changes
    update_date_column = luigi.Parameter(default='')  # Used to record SCD1 changes
    dimension_table = luigi.Parameter()
    staging_dimension_table = luigi.Parameter()
    staging_source_table = luigi.Parameter()
    source_object = luigi.Parameter()

    @property
    def object_name(self):
        return 'RetireSCD2Records_setp_2'

    @property
    def stage_name(self):
        return "SCD"

    def requires(self): # Create and load the source and the target table in the staging schema
        return [CreateAndLoadTable( run_timestamp=self.run_timestamp,
                                    mirror= self.source_object,
                                    target= self.staging_source_table
                                   ),
                CreateAndLoadTable( run_timestamp=self.run_timestamp,
                                    mirror= self.dimension_table,
                                    target= self.staging_dimension_table
                                   )
                ]

    @property
    def query(self):

        remove_default_records_sql = ""
        for pk_col in self.business_key.split(','):
            remove_default_records_sql += ' AND {} < 0'.format(pk_col)
        sql = '''
----------------------------STEP 2 - Type 2 to expire old records and remove default records---------------------------'
DELETE FROM {staging_dimension_table} WHERE 1=1 {remove_default_records};
UPDATE {staging_dimension_table}
SET  {current_ind_column} = false,
     {end_date_column} = CURRENT_TIMESTAMP
'''.format(staging_dimension_table=self.staging_dimension_table, current_ind_column=self.current_ind_column,
           end_date_column=self.end_date_column, remove_default_records=remove_default_records_sql)

        if self.update_date_column:
            sql += ", \n \t {} = CURRENT_TIMESTAMP".format(self.update_date_column)

        sql += '\n FROM {} src \n WHERE '.format(self.staging_source_table)

        # Loop through the list of primary key to form the where clause columns comparison
        for pk_col in self.business_key.split(','):
            sql += '\n\t{}.{} = src.{} AND '.format(self.staging_dimension_table, pk_col, pk_col)

        # Add the current flag filter
        sql += '\n\t{}.{} = true AND '.format(self.staging_dimension_table, self.current_ind_column )

        # loop through the SCD2 columns to include in the AND condition
        first_loop = True # Add a '(' at the start for the first time
        for column in self.scd2_column.split(','):
            line = '''
    (( {staging_dimension_table}.{column} != src.{column} ) OR (( {staging_dimension_table}.{column} IS NOT NULL ) AND (src.{column} IS NULL)) OR (( {staging_dimension_table}.{column} IS NULL ) AND (src.{column} IS NOT NULL)))
'''.format(staging_dimension_table=self.staging_dimension_table, column=column.strip(' \t\n\r'))

            if first_loop:
                sql += "(\n\t" + line
                first_loop = False
            else:
                sql += "\nOR\t" + line

        sql +=  '\n) ' # Add the last brace

        return sql


class CreateAndLoadTable(RunDatabaseQuery):
    """
    This class creates a replica of the source and the target table in the staging schema along with the data. The SCD process doesn't touch the acutal source and target inorder to avoid
    wrong updates to the actual table incase of error.
    """

    mirror = luigi.Parameter()
    target = luigi.Parameter()

    @property
    def table_operations(self):
        return TableOperations(self.run_timestamp)

    @property
    def stage_name(self):
        return "SCD"

    @property
    def object_name(self):
        return self.target

    @property
    def query(self):
        mirror_schema = self.mirror.split('.')[0]
        mirror_table = self.mirror.split('.')[1]
        target_schema = self.target.split('.')[0]
        target_table = self.target.split('.')[1]
        return self.table_operations.generate_ctas_sql(mirror_schema, target_schema, mirror_table, target_table)


