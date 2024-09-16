#!/usr/bin/python3
import os
from configuration import ConfigLoader
from postgresql import PsqlDB
from app_exceptions import DatabaseError, MissingParameterError


class CopyDao:

    """
    The Copy Data Access Object reads the most recent data from DETER Cerrado production table
    and write them in one output table to DETER Cerrado publish table.

    The input and output databases may are in different hosts.
    See the db.cfg file for access the databases configuration definitions.
    See the model.cfg file for another configurations about the database tables names.

    """
    #constructor
    def __init__(self):
        relative_path = 'cerrado-deter/src/config/'
        self.inputdb = PsqlDB(relative_path,'db.cfg','production')
        self.outputdb = PsqlDB(relative_path,'db.cfg','publish')
        self.__loadConfigurations(relative_path)
        # get env var setted in Dockerfile
        self.is_docker_env = os.getenv("DOCKER_ENV", False)
        # get env var setted in docker stack
        self.date_to_copy = os.getenv("CREATED_DATE", '2022-12-01')
        # If the environment is docker then use the absolute path to write log file
        if self.is_docker_env:
            self.data_dir='/usr/local/data/'
        else:
            self.data_dir=os.path.realpath(os.path.dirname(__file__) + '/../') + '/'


    def __loadConfigurations(self,relative_path):
        # read model parameters
        try:
            productioncfg = ConfigLoader(relative_path, 'model.cfg', 'production')
            self.production_cfg = productioncfg.get()
            publishcfg = ConfigLoader(relative_path, 'model.cfg', 'publish')
            self.publish_cfg = publishcfg.get()
        except Exception as configError:
            raise configError


    def copy(self, renew=False):
        """
        Start copy process

        The renew parameter is used to configure the behaviour of the copy process.
        If renew is equal True them the output table are dropped and all data will be copied.

        Return start and end date values.
        
        Will raise a DatabaseError if exception occured.

        Warning: This method opens connection, run the process and close connection.
        """

        start_date = max_created_date = max_view_date = None

        try:

            if renew:
                # verify if table exists
                if self.__outputTableExists():
                    # DROP the output table for renew all data
                    self.__dropOutputTable()
                # fixed date based on last clean interpretation database (created_date)
                start_date = self.date_to_copy
            else:
                start_date, max_view_date = self.__getLastDate()

            self.__generateInsertScript(start_date)
            self.__writeToOutputTable()

            max_created_date, max_view_date = self.__getLastDate()

        except BaseException as error:
            raise error
        
        return start_date, max_created_date, max_view_date

    def __dropOutputTable(self):
        """
        Drop output table from the database.
        We using this method when want copy all data from input table and process that data and provide for API.

        No return value but in error raise a DatabaseError exception.
        Warning: This method opens connection, run the process and close connection.
        """

        drop_table = "DROP TABLE IF EXISTS"
        try:
            self.outputdb.connect()
            sql = '{0} {1}.{2}'.format(drop_table, self.publish_cfg["schema"], self.publish_cfg["table"])
            self.outputdb.execQuery(sql)
            self.outputdb.commit()
        except Exception as error:
            self.outputdb.rollback()
            raise DatabaseError('Database error:', error)
        finally:
            self.outputdb.close()

    def __getLastDate(self):
        """
        Read the last date from output table to created date and to view date.

        @return string, two values, the max created date and the max view date.
        """
        created = view = None

        if self.__outputTableExists():
            # select max date from output table
            sql = "SELECT MAX(created_date::date)::varchar, MAX(view_date::date)::varchar "
            sql += "FROM {0}.{1} ".format(self.publish_cfg["schema"], self.publish_cfg["table"])
            try:
                self.outputdb.connect()
                data = self.outputdb.fetchData(sql)
            except BaseException:
                # by default return None
                return created, view
            finally:
                self.outputdb.close()

            if(len(data)==1 and len(data[0])==2):
                created = data[0][0]
                view = data[0][1]
        
        return created, view

    def __generateInsertScript(self, from_date=None, filter_area=0.03, file_name=None):
        """
        Read data from output table and generate a set of INSERT statements as SQL Script.

        @return string, the path and name for output file with SQL insert statements or false if error.
        """
        read_from_table = sql_filter = write_to_table = ""
        write_to_table = "{0}.{1}".format(self.publish_cfg["schema"], self.publish_cfg["table"])
        read_from_table = "{0}.{1}".format(self.production_cfg["schema"], self.production_cfg["table"])

        if filter_area:
            sql_filter = "ST_Area(ST_Transform(spatial_data,4326)::geography)/1000000 > {0}".format(filter_area)

        if from_date:
            sql_filter = " {0} AND created_date::date > '{1}' AND auditar='0' ".format(sql_filter, from_date)

        sql = "SELECT ('INSERT INTO {0} (object_id, cell_oid, local_name, class_name, scene_id, task_id,".format(write_to_table)
        sql += "satellite, sensor, spatial_data, area_total_km, path_row, quadrant,view_date, created_date, updated_date, auditar, control) VALUES(' || "
        sql += "object_id || ',' || quote_nullable(cell_oid) || ',' || quote_nullable(local_name) || ',''' || class_name || ''',' || quote_nullable(scene_id) || ',' || quote_nullable(task_id) || ',''' || "
        sql += "satellite || ''',''' || sensor || ''',''' || ST_Multi(spatial_data)::text || ''',' || ST_Area(ST_Transform(spatial_data,4326)::geography)/1000000 || ',' || "
        sql += "quote_nullable(path || '_' || row) || ',' || quote_nullable(quadrant) || ',''' || view_date || ''',' || quote_nullable(created_date) || ',' || quote_nullable(updated_date) || ',' || "
        sql += "quote_nullable(auditar) || ',' || quote_nullable(control) || ');') as inserts "
        sql += "FROM {0} ".format(read_from_table)

        if sql_filter:
            sql += "WHERE {0}".format(sql_filter)

        data_file = False

        try:
            self.inputdb.connect()
            data = self.inputdb.fetchData(sql)
            data_file = self.__writeScriptToFile(data, file_name)
        except BaseException as error:
            raise error
        finally:
            self.inputdb.close()

        return data_file


    def __writeToOutputTable(self):
        
        # Before insert data, verify if table exists.
        try:
            if not self.__outputTableExists():
                # Case not it'll be created.
                self.__createOutputTable()
        except BaseException as error:
            raise error

        data_file = self.data_dir + self.publish_cfg['output_data_file']
        if not os.path.exists(data_file):
            raise MissingParameterError('Import data file','File, {0}, was not found.'.format(data_file))
        
        inserts = None
        try:
            inserts = [line.rstrip('\r\n') for line in open(data_file)]
        except Exception as error:
            raise MissingParameterError('Import data file','Error: {0}'.format(error))
        
        try:
            self.outputdb.connect()
            for insert in inserts:
                self.outputdb.execQuery(insert)
            self.outputdb.commit()
        except BaseException as error:
            self.outputdb.rollback()
            raise DatabaseError('Database error:', error)
        finally:
            self.outputdb.close()

    
    def __writeScriptToFile(self, content, file_name=None):

        output_file = self.publish_cfg['output_data_file']

        if file_name:
            output_file = file_name

        data_file = self.data_dir + output_file
        f = open(data_file,"w+")
        for i in content:
            if i[0]:
                f.write("{0}\r\n".format(i[0]))
        f.close()
        if os.path.exists(data_file):
            return os.path.realpath(data_file)
        else:
            return False

    def __outputTableExists(self):

        sql = "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='{0}')".format(self.publish_cfg["table"])

        try:
            self.outputdb.connect()
            data = self.outputdb.fetchData(sql)
        except BaseException as error:
            raise error
        finally:
            self.outputdb.close()
        
        return data[0][0]

    def __createOutputTable(self):

        sql = "CREATE TABLE {0}.{1} ".format(self.publish_cfg["schema"], self.publish_cfg["table"])
        sql += "( "
        sql += "object_id integer NOT NULL, "
        sql += "cell_oid character varying(255), "
        sql += "local_name text, "
        sql += "class_name text, "
        sql += "scene_id integer, "
        sql += "task_id integer, "
        sql += "satellite text, "
        sql += "sensor text, "
        sql += "spatial_data geometry(MultiPolygon,4674), "
        sql += "area_total_km double precision, "
        sql += "path_row character varying(10), "
        sql += "quadrant character varying(1), "
        sql += "view_date date, "
        sql += "created_date timestamp without time zone, "
        sql += "updated_date timestamp without time zone, "
        sql += "auditar character varying(5), "
        sql += "control integer, "
        sql += "CONSTRAINT {0}_pk PRIMARY KEY (object_id) ".format(self.publish_cfg["table"])
        sql += ") "
        sql += "WITH ( "
        sql += "OIDS = FALSE "
        sql += ")"

        try:
            self.outputdb.connect()
            self.outputdb.execQuery(sql)
            self.outputdb.commit()
        except BaseException as error:
            self.outputdb.rollback()
            raise DatabaseError('Database error:', error)
        finally:
            self.outputdb.close()
