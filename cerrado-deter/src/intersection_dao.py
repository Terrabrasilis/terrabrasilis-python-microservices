#!/usr/bin/python3
import os, sys
sys.path.insert(0, os.path.realpath( os.path.realpath(os.path.dirname(__file__))+'/../../' ) )
from common_modules.configuration.src.common_config import ConfigLoader
from common_modules.postgresql.src.psqldb import PsqlDB
from app_exceptions import DatabaseError, MissingParameterError


class IntersectionDao:

    """
    The Intersection Data Access Object runs intersections over all
    features in DETER Cerrado publish table for prepare data in comply
    the SFS standart to deploy on GeoServer.

    See configuration file, db.cfg for database connection definitions.

    """
    #constructor
    def __init__(self):
        relative_path = 'cerrado-deter/src/config/'
        self.db = PsqlDB(relative_path,'db.cfg','publish')
        self.__loadConfigurations(relative_path)


    def __loadConfigurations(self,relative_path):
        # read model parameters
        try:
            cfg = ConfigLoader(relative_path, 'model.cfg', 'publish_sfs')
            self.cfg_data = cfg.get()
        except Exception as configError:
            raise configError


    def intersections(self, renew=False):
        """
        Start intersections process.

        The renew parameter is used to configure the behaviour of the intersection process.
        If renew is equal True them the output table are dropped and all data will be process.

        Return the most recent date for the data.
        
        Will raise a DatabaseError if exception occured.

        Warning: This method opens connection, run the process and close connection.
        """

        # used to return the date for the most recent data.
        end_date = None

        try:

            if renew:
                # Truncate the output table for renew all data
                self.truncateOutputTable()
            """
            # Drop the table is bad practice because can be cause an connection error on GeoServer.
            if renew:
                # DROP the output table for renew all data
                self.dropOutputTable()
            """


            self.dropIntermediaryTables()

            self.db.connect()
            last_date = self.__getLastDate()

            self.__createSequence()
            self.__createDataTable(last_date)
            self.__intersectAlertsAndCounty()
            self.__intersectAlertsAndUC()

            self.__dropSequence()

            end_date = self.__getLastDate()

            self.db.commit()

        except BaseException as error:
            self.db.rollback()
            raise error
        finally:
            self.db.close()

        #self.dropIntermediaryTables()

        return end_date

    def dropIntermediaryTables(self):
        """
        Drop intermediary tables from the database.

        No return value but in error raise a DatabaseError exception.
        Warning: This method opens connection, run the process and close connection.
        """

        drop_table = "DROP TABLE IF EXISTS"
        jobber_tables = self.cfg_data["jobber_tables"]
        try:
            self.db.connect()
            for key in jobber_tables:
                sql = '{0} {1}.{2}'.format(drop_table, self.cfg_data["jobber_schema"], jobber_tables[key])
                self.db.execQuery(sql)
            self.db.commit()
        except Exception as error:
            self.db.rollback()
            raise DatabaseError('Database error:', error)
        finally:
            self.db.close()

    def dropOutputTable(self):
        """
        Drop output table from the database.
        We using this method when want copy all data from input table and process that data and provide for API.
        The issue related is the cenarium where one or more geometries was removed and recriated on reclassification
        by TerraAmazon leaving the new portion of geometry out of filter by date.

        No return value but in error raise a DatabaseError exception.
        Warning: This method opens connection, run the process and close connection.
        """

        drop_table = "DROP TABLE IF EXISTS"
        try:
            self.db.connect()
            sql = '{0} {1}.{2}'.format(drop_table, self.cfg_data["output_schema"], self.cfg_data["output_table"])
            self.db.execQuery(sql)
            self.db.commit()
        except Exception as error:
            self.db.rollback()
            raise DatabaseError('Database error:', error)
        finally:
            self.db.close()

    def truncateOutputTable(self):
        """
        Truncate output table from the database.
        We using this method when want copy all data from input table and process that data and provide for API.
        The issue related is the cenarium where one or more geometries was removed and recriated on reclassification
        by TerraAmazon leaving the new portion of geometry out of filter by date.

        No return value but in error raise a DatabaseError exception.
        Warning: This method opens connection, run the process and close connection.
        """

        truncate_table = "TRUNCATE {0}.{1} RESTART IDENTITY".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
        try:
            self.db.connect()
            sql = truncate_table
            self.db.execQuery(sql)
            self.db.commit()
        except Exception as error:
            self.db.rollback()
            raise DatabaseError('Database error:', error)
        finally:
            self.db.close()

    def __outputTableExists(self):

        sql = "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='{0}')".format(self.cfg_data["output_table"])

        data = False
        try:
            data = self.__fetchExecute(sql)
        except BaseException as error:
            raise error
        
        if(len(data)==1 and len(data[0])==1):
                data = data[0][0]
        
        return data


    def __getLastDate(self):
        """
        Read the last date from output table to created date.

        @return string, one value, the last created date.
        """
        date = None
        outputTableExists = self.__outputTableExists()

        if outputTableExists:
            # select max date from output table
            sql = "SELECT MAX(created_date::date)::varchar "
            sql += "FROM {0}.{1} ".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
            try:
                data = self.__fetchExecute(sql)
            except BaseException:
                # by default return None
                return date

            if(len(data)==1 and len(data[0])==1):
                date = data[0][0]
        
        return date

    def __getLastIdentifier(self):
        """
        Read the last identifier from output table to reset sequence.

        @return gid, the last identifier for output table.
        """
        gid = 1

        if self.__outputTableExists():
            # select max gid from output table
            sql = "SELECT MAX(gid) "
            sql += "FROM {0}.{1} ".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
            try:
                data = self.__fetchExecute(sql)
            except BaseException:
                # by default return 1
                return gid

            if(len(data)==1 and len(data[0])==1):
                gid = (data[0][0] if data[0][0] else 1)

        return (gid + 1)

    def __createDataTable(self, last_date):
        sql = "CREATE TABLE {0}.{1} AS ".format(self.cfg_data["jobber_schema"], self.cfg_data["jobber_tables"]["tb1"])
        sql += "SELECT nextval('{0}.{1}') as gid, ".format(self.cfg_data["jobber_schema"], self.cfg_data["sequence"])
        sql += "alerts.object_id as origin_gid, "
        sql += "alerts.cell_oid, "
        sql += "alerts.class_name, "
        sql += "alerts.quadrant, "
        sql += "alerts.path_row, "
        sql += "alerts.view_date, "
        sql += "alerts.created_date::date as created_date, "
        sql += "alerts.sensor, "
        sql += "alerts.satellite, "
        sql += "alerts.spatial_data as geometries "
        sql += "FROM {0}.{1} as alerts ".format(self.cfg_data["input_schema"], self.cfg_data["input_table"])
        sql += "WHERE alerts.view_date IS NOT NULL "
        #sql += "AND alerts.created_date IS NOT NULL "

        if last_date:
            sql += "AND alerts.created_date::date > '{0}'".format(last_date)

        self.__resetSequence()
        self.__basicExecute(sql)

        # calculate total area to complete table contents
        self.__computeTotalArea()
        # create gist index to improve intersections
        self.__createSpatialIndex(self.cfg_data["jobber_schema"],self.cfg_data["jobber_tables"]["tb1"])

    def __computeTotalArea(self):

        sql = "ALTER TABLE {0}.{1} ADD COLUMN area_total_km double precision".format(self.cfg_data["jobber_schema"], self.cfg_data["jobber_tables"]["tb1"])
        self.__basicExecute(sql)
        sql = "UPDATE {0}.{1} SET area_total_km = ST_Area((geometries)::geography)/1000000".format(self.cfg_data["jobber_schema"], self.cfg_data["jobber_tables"]["tb1"])
        self.__basicExecute(sql)

    def __createSpatialIndex(self, schema, table, column="geometries"):

        sql = "CREATE INDEX sidx_{0}_{1}_{2} ON {0}.{1} USING gist ({2}) TABLESPACE pg_default".format(schema,table,column)
        self.__basicExecute(sql)

    def __intersectAlertsAndUC(self):

        sql  = "WITH UCS AS ( "
        sql += "	SELECT nome_uc1, esfera_pri, geom "
        sql += "	FROM {0}.{1} ".format(self.cfg_data["jobber_schema"], self.cfg_data["uc_table"])
        sql += "	ORDER BY esfera_pri DESC "
        sql += "), ALERTS_UCS AS ( "
        sql += "	SELECT alerts.gid, ST_Area(ST_Intersection(alerts.geom, uc.geom)::geography)/1000000 as area, "
        sql += "    uc.nome_uc1 as nome "
        sql += "	FROM {0}.{1} as alerts ".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
        sql += "	INNER JOIN UCS as uc ON ( (alerts.geom && uc.geom) AND ST_Intersects(alerts.geom, uc.geom) ) "
        sql += ") "
        sql += "UPDATE {0}.{1} ".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
        sql += "SET areauckm=uc.area,uc=uc.nome "
        sql += "FROM ALERTS_UCS as uc "
        sql += "WHERE {0}.{1}.gid=uc.gid ".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
        self.__resetSequence()
        self.__basicExecute(sql)

    def __intersectAlertsAndCounty(self):

        tableExists = self.__outputTableExists()

        last_gid = 1

        sql = "SELECT alerts.origin_gid, "
        sql += "alerts.class_name as classname, "
        sql += "alerts.quadrant, "
        sql += "alerts.path_row, "
        sql += "alerts.view_date, "
        sql += "alerts.created_date, "
        sql += "alerts.sensor, "
        sql += "alerts.satellite, "
        sql += "alerts.area_total_km as areatotalkm, "
        sql += "NULL::double precision as areauckm, "
        sql += "NULL::character varying(255) as uc, "
        sql += "ST_Area(ST_Intersection(alerts.geometries, mun.geom)::geography)/1000000 as areamunkm, "
        sql += "mun.nm_municip as county, "
        sql += "mun.nm_sigla as uf, "
        sql += "mun.cd_geocmu as geocod, "
        sql += "coalesce(ST_Multi(ST_CollectionExtract( ST_Intersection(alerts.geometries, mun.geom) ,3)), alerts.geometries) as geom, "
        sql += "nextval('{0}.{1}') as gid ".format(self.cfg_data["jobber_schema"], self.cfg_data["sequence"])
        sql += "FROM {0}.{1} as alerts ".format(self.cfg_data["jobber_schema"], self.cfg_data["jobber_tables"]["tb1"])
        sql += "LEFT JOIN {0}.{1} as mun ".format(self.cfg_data["jobber_schema"], self.cfg_data["county_table"])
        sql += "ON ( (alerts.geometries && mun.geom) AND ST_Intersects(alerts.geometries, mun.geom) )"

        if tableExists:
            
            last_gid = self.__getLastIdentifier()

            sql_insert = "INSERT INTO {0}.{1} ".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
            sql_insert += "(origin_gid, classname, quadrant, path_row, view_date, "
            sql_insert += "created_date, sensor, satellite, areatotalkm, areauckm, "
            sql_insert += "uc, areamunkm, county, uf, geocod, geom, gid) "
            sql_insert += "SELECT tb1.origin_gid, tb1.classname, tb1.quadrant, tb1.path_row, tb1.view_date, "
            sql_insert += "tb1.created_date, tb1.sensor, tb1.satellite, tb1.areatotalkm, tb1.areauckm, "
            sql_insert += "tb1.uc, tb1.areamunkm, tb1.county, tb1.uf, tb1.geocod, tb1.geom, tb1.gid "
            sql_insert += "FROM ({0}) as tb1 ".format(sql)

            sql = sql_insert
        else:
            sql = "CREATE TABLE {0}.{1} AS {2}".format(self.cfg_data["output_schema"], self.cfg_data["output_table"], sql)
            
        self.__resetSequence(last_gid)
        self.__basicExecute(sql)

        """ Remove alerts that are out of bounds therefore areamunkm IS NULL """
        sql = "DELETE FROM {0}.{1} WHERE areamunkm IS NULL ".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
        self.__basicExecute(sql)

        if not tableExists:
            
            """
            Alter table to add temporal column to publish on geoserver with monthly granularity
            """
            sql = "ALTER TABLE {0}.{1} ADD COLUMN publish_month date".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
            self.__basicExecute(sql)
            sql = "UPDATE {0}.{1} SET publish_month=overlay(view_date::varchar placing '01' from 9 for 2)::date".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
            self.__basicExecute(sql)
            sql = "CREATE INDEX publish_month_idx ON {0}.{1} USING btree (publish_month ASC NULLS LAST)".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
            self.__basicExecute(sql)
            sql = "ALTER TABLE {0}.{1} CLUSTER ON publish_month_idx".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
            self.__basicExecute(sql)

            sql = "ALTER TABLE {0}.{1} ALTER COLUMN gid SET NOT NULL".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
            self.__basicExecute(sql)
            sql = "ALTER TABLE {0}.{1} ADD PRIMARY KEY (gid)".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
            self.__basicExecute(sql)

            """
            Create geographic index
            """
            sql = "CREATE INDEX {1}_geom_index ON {0}.{1} USING gist (geom)".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
            self.__basicExecute(sql)
        else:
            sql = "UPDATE {0}.{1} SET publish_month=overlay(view_date::varchar placing '01' from 9 for 2)::date".format(self.cfg_data["output_schema"], self.cfg_data["output_table"])
            self.__basicExecute(sql)


    def __resetSequence(self, next_gid=1):
        sql = "ALTER SEQUENCE {0}.{1} RESTART WITH {2}".format(self.cfg_data["jobber_schema"], self.cfg_data["sequence"], next_gid)
        self.__basicExecute(sql)


    def __createSequence(self):
        """
        Create one sequence used to intermediary tables
        """
        sql = "CREATE SEQUENCE {0}.{1} INCREMENT 1 MINVALUE 1 ".format(self.cfg_data["jobber_schema"], self.cfg_data["sequence"])
        sql += "MAXVALUE 9223372036854775807 START 1 CACHE 1"
        self.__basicExecute(sql)


    def __dropSequence(self):
        """
        Drop the sequence used to intermediary tables
        """
        sql = "DROP SEQUENCE {0}.{1}".format(self.cfg_data["jobber_schema"], self.cfg_data["sequence"])
        self.__basicExecute(sql)


    def __basicExecute(self, sql):
        """
        Execute a basic SQL statement.
        """
        try:
            self.db.execQuery(sql)
        except Exception as error:
            self.db.rollback()
            raise DatabaseError('Database error:', error)


    def __fetchExecute(self, sql):
        """
        Execute a SQL statement and fetch the result data.
        """
        data = False
        try:
            data = self.db.fetchData(sql)
        except BaseException as error:
            raise DatabaseError('Database error:', error)
        
        return data