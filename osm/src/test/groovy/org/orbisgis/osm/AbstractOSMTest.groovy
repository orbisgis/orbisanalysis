package org.orbisgis.osm

import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.GeometryFactory
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2GIS;

/**
 * Abstract for OSM tests. It contains some utilities methods and static variable in order to simplify test write.
 *
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019
 */
abstract class AbstractOSMTest {
    /** Main path for the databases. */
    private static final def PATH = "./target/"
    /** Main database option to make it openable from external tools. */
    private static final def DB_OPTION = ";AUTO_SERVER=TRUE"

    /** Generation of string {@link UUID}.*/
    protected static final def uuid(){ "_"+UUID.randomUUID().toString().replaceAll("-", "_")}
    /** Used to store the OSM request to ensure the good query is generated. */
    protected static def query
    /** Generation of a random named database. */
    protected static final def RANDOM_DS = { H2GIS.open(PATH + uuid() + DB_OPTION)}
    /** Regex for the string UUID. */
    protected static def uuidRegex = "[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}"


    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def executeOverPassQuery
    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def getAreaFromPlace
    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def extract
    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def load

    void beforeEach(){
        //Store the modified object
        executeOverPassQuery = OSMTools.Loader.&executeOverPassQuery
        extract = OSMTools.Loader.&extract
        load = OSMTools.Loader.&load
        getAreaFromPlace = OSMTools.Utilities.&getAreaFromPlace
    }

    void afterEach(){
        //Restore the modified object
        OSMTools.Loader.metaClass.static.executeOverPassQuery = executeOverPassQuery
        OSMTools.Loader.metaClass.static.extract = extract
        OSMTools.Loader.metaClass.static.load = load
        OSMTools.Utilities.metaClass.static.getAreaFromPlace = getAreaFromPlace
    }

    /**
     * Override the 'executeOverPassQuery' methods to avoid the call to the server
     */
    protected static void sampleOverpassQueryOverride(){
        OSMTools.Loader.metaClass.static.executeOverPassQuery = {query, outputOSMFile ->
            LoaderTest.query = query
            outputOSMFile << LoaderTest.getResourceAsStream("sample.osm").text
            return true
        }
    }

    /**
     * Override the 'executeOverPassQuery' methods to avoid the call to the server
     */
    protected static void badOverpassQueryOverride(){
        OSMTools.Loader.metaClass.static.executeOverPassQuery = {query, outputOSMFile ->
            LoaderTest.query = query
            return false
        }
    }

    /**
     * Override the 'getAreaFromPlace' methods to avoid the call to the server
     */
    protected static void sampleGetAreaFromPlace(){
        OSMTools.Utilities.metaClass.static.getAreaFromPlace = {placeName ->
            def coordinates = [new Coordinate(0, 0), new Coordinate(4, 8), new Coordinate(7, 5),
                               new Coordinate(0, 0)] as Coordinate[]
            def geom = new GeometryFactory().createPolygon(coordinates)
            geom.SRID = 4326
            return geom
        }
    }

    /**
     * Override the 'getAreaFromPlace' methods to avoid the call to the server
     */
    protected static void badGetAreaFromPlace(){
        OSMTools.Utilities.metaClass.static.getAreaFromPlace = {placeName -> }
    }

    /**
     * Override the 'extract' process to make it fail
     */
    protected static void badExtract(){
        OSMTools.Loader.metaClass.static.extract = {
            return OSMTools.Loader.create({
                title "Extract the OSM data using the overpass api and save the result in an XML file"
                inputs overpassQuery: String
                outputs outputFilePath: String
                run { overpassQuery -> }
            })
        }
    }

    /**
     * Override the 'load' process to make it fail
     */
    protected static void badLoad(){
        OSMTools.Loader.metaClass.static.load = {
            return OSMTools.Loader.create({
                title "Load an OSM file to the current database"
                inputs datasource: JdbcDataSource, osmTablesPrefix: String, osmFilePath: String
                outputs datasource: JdbcDataSource
                run { JdbcDataSource datasource, osmTablesPrefix, osmFilePath -> }
            })
        }
    }
}
