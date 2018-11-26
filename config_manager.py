import luigi
import ConfigParser
import os
from datahouse_common_functions import get_logger


class ConfigManager:
    test_run_config_dir = luigi.configuration.get_config().get('domain', 'temp-dir') + "test_run_configs" + os.sep

    def __init__(self, pipeline_timestamp=None):
        self.pipeline_timestamp = pipeline_timestamp
        self._test_config = None
        self._s3_config = None

        self.staging = "staging" + self.schema_suffix()
        self.audit = "audit"
        self.landing = "landing" + self.schema_suffix()
        self.tableau = "tableau" + self.schema_suffix()
        self.control = "control" + self.schema_suffix()
        self.keys = "keys" + self.schema_suffix()
        self.datamart = "datamart" + self.schema_suffix()
        self.sandbox = "sandbox" + self.schema_suffix()
        self.bad_data = "bad_data" + self.schema_suffix()
        self.clean = "clean" + self.schema_suffix()
        self.clean_tgt = self.clean
        self.core = "core" + self.schema_suffix()
        self.clean_secure = "clean_secure" + self.schema_suffix()
        self.core_secure = "core_secure" + self.schema_suffix()
        self.datamart_secure = "datamart_secure" + self.schema_suffix()

        from ConfigParser import NoSectionError
        try:
            if self.test_config('force-prod-sources'):
                self.bad_data = "bad_data"
                self.clean = "clean"
                self.clean_tgt = "clean" + self.schema_suffix()
                self.clean_secure = "clean_secure"
                self.landing = "landing"
        #except ConfigParser.NoOptionError:
        #    pass
        #except NoSectionError:
        #    pass
        except:
            raise
        try:
            if self.test_config('force-prod-core'):
                self.core = "core"
                self.core_secure = "core_secure"
        # except ConfigParser.NoOptionError:
        #     pass
        # except NoSectionError:
        #     pass
        except:
            raise


    @classmethod
    def generate_test_config(cls, testConfig, timestamp, otherConfig=None):
        """

        :param testConfig: The test configuration to use
        :param timestamp: The original timestamp for the task
        :param otherConfig: The additional configuration to use

        :return: A generated timestamp that allows the task to retrieve the correct configuration
        """
        config = None
        if (testConfig and otherConfig):
            config = {key: value for (key, value) in (testConfig.items() + otherConfig.items())}
        elif testConfig:
            config = testConfig
        elif otherConfig:
            config = otherConfig

        return cls.dump_config_to_file(cls.test_run_config_dir, config, timestamp)

    @classmethod
    def test_run_unique_id(cls, original_timestamp, config):
        import hashlib
        return hashlib.md5(original_timestamp + str(config)).hexdigest()

    def dynamic_get_config(self, section, parameter):
        if self.is_prod() or not self.pipeline_timestamp or not self.read_test_config():
            if section == 'test_configuration':
                return False  # default does not have a test configuration
            return luigi.configuration.get_config().get(section, parameter)
        else:
            if section == 'test_configuration':
                config = self.read_test_config()
                return config[parameter]
            else:
                raise Exception("{} from {} not yet implemented.".format(parameter, section))

    def test_config(self, parameter):
        return self.dynamic_get_config('test_configuration', parameter)

    def read_test_config(self):
        if not self._test_config:
            import json
            try:
                with open('{}{}.json'.format(self.test_run_config_dir, self.pipeline_timestamp), 'r') as json_data:
                    self._test_config = json.load(json_data)
            except IOError:
                get_logger().warning("Could not find configuration for {} - reverting to default".format(self.pipeline_timestamp))

        return self._test_config

    @property
    def env_type(self):
        return luigi.configuration.get_config().get('domain', 'env-type')

    @classmethod
    def is_prod(cls):
        return True if luigi.configuration.get_config().get('domain', 'env-type') == 'prod' else False

    def schema_suffix(self):
        """
        Suffixes are added to schemas to differentiate environments. For example, production doesn't have a suffix, so
        it's schemas are e.g. landing, clean, staging, etc. Whereas the dev environment might have a suffix of dev1, so it's
        schemas as landing_dev1, clean_dev1, staging_dev1.

        This method gets the suffix (i.e. environment to use) from the configuration file.

        :return: Return the suffix to use on schemas.
        """

        env = luigi.configuration.get_config().get('domain', 'env-type')
        if env == "prod":
            return ""
        else:
            return "_" + env

    @property
    def default_format_kwargs(self):
        return {
            "staging": self.staging,
            "clean_secure": self.clean_secure,
            "clean": self.clean,
            "core_secure": self.core_secure,
            "core": self.core,
            "datamart_secure": self.datamart_secure,
            "datamart": self.datamart,
            "landing": self.landing,
            "keys": self.keys,
            "control": self.control,
            "audit": self.audit,
            "select": self.select()
        }

    def select(self):
        if self.test_config('run_queries') and isinstance(self.test_config('run_queries'), (int, long)):
            return "select top {}".format(self.test_config('run_queries'))
        else:
            return "select"

    def s3_source_config(self):
        if self.is_prod() or self.test_config('use_prod_s3_cfg'):
            config = ConfigParser.ConfigParser()
            config.read(os.path.join(os.environ['DATAHOUSE_CONFIG_PATH'], "production_s3_sources.cfg"))
        else:
            config = ConfigParser.ConfigParser()
            config.read(os.path.join(os.environ['DATAHOUSE_CONFIG_PATH'], "development_s3_sources.cfg"))
        return config

    def get_s3_location(self, parameter):
        if self.is_prod() or not self.pipeline_timestamp or not self.read_test_config() or self.test_config('use_prod_s3_cfg'):
            return self.s3_source_config().get('s3sources', parameter)
        else:
            s3_config = self.read_test_config()
            if(s3_config[parameter]):
                return s3_config[parameter]
            else:
                return self.s3_source_config().get('s3sources', parameter)

    @classmethod
    def dump_config_to_file(cls, path, config, timestamp):
        """
        :param path: The location where the config will be dumped to.
        :param config: The configuration to use
        :param timestamp: The original timestamp for the task
        :return: A generated timestamp that allows the task to retrieve the correct configuration
        """
        import json

        # create local folder
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except os.error:
                pass
        file = cls.test_run_unique_id(timestamp, config)

        with open('{}{}.json'.format(path, file), 'w') as outfile:
            json.dump(config, outfile)

        return file