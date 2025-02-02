"""
spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

from pyspark.sql.session import SparkSession

class SparkSessionManager:
    
    def __init__(self, app_name = "default_application", master=None, configurations=None):
        """ 
            Initialize Spark Session Manager
            :param app_name: Application name for spark
            :param master (optional): Spark cluster configuration
            :param configuration: Dict containing a set of cofigurations for spark
        """
        self.app_name = app_name
        self.master = master
        self.configurations = configurations or {}
        
    def create_session(self) -> SparkSession:
        """ 
        Create and return Spark Session
        with specified configurations

        :return: spark session object
        """
        spark_builder = SparkSession.builder.appName(self.app_name)

        # define master if not none
        if self.master:
            spark_builder = spark_builder.master(self.master)

        # add custom configurations
        for key, value in self.configurations.items():
            spark_builder = spark_builder.config(key, value)

        # create spark session
        spark = (spark_builder
                 .enableHiveSupport()
                 .getOrCreate()
                )

        return spark