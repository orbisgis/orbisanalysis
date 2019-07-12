package org.orbisgis.osm


import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Disabled
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanagerapi.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.*

class OSMHelperTests {

    private static final Logger logger = LoggerFactory.getLogger(OSMHelperTests.class)

    @Test
    void extractTest() {
        def extract = OSMHelper.Loader.extract()
        assertTrue extract.execute(overpassQuery: "(node[\"building\"](48.73254476110234,-3.0790257453918457,48.73565477358819,-3.0733662843704224); " +
                "way[\"building\"](48.73254476110234,-3.0790257453918457,48.73565477358819,-3.0733662843704224); " +
                "relation[\"building\"](48.73254476110234,-3.0790257453918457,48.73565477358819,-3.0733662843704224); );");
        assertTrue new File(extract.results.outputFilePath).exists()
        assertTrue new File(extract.results.outputFilePath).length() > 0
    }

    @Test
    void extractTestWrongQuery() {
        def extract = OSMHelper.Loader.extract()
        assertFalse extract.execute(overpassQuery: "building =yes");
    }

    @Disabled
    @Test
    void loadTest() {
        def h2GIS = H2GIS.open('./target/osmhelper')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix, osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())
        //Check tables
        assertEquals 10, h2GIS.getTableNames().count{
            it =~ /^OSMHELPER.PUBLIC.${prefix}.*/
        }
        //Count nodes
        h2GIS.eachRow "SELECT count(*) as nb FROM OSMHELPER.PUBLIC.${prefix}_NODE",{ row ->
            assertEquals 1176, row.nb }

        //Count ways
        h2GIS.eachRow "SELECT count(*) as nb FROM OSMHELPER.PUBLIC.${prefix}_WAY",{ row ->
            assertEquals 1176, row.nb }

        //Count relations
        h2GIS.eachRow "SELECT count(*) as nb FROM OSMHELPER.PUBLIC.${prefix}_RELATION",{ row ->
            assertEquals 1176, row.nb }

        //Check specific tags
        h2GIS.eachRow "SELECT count(*) as nb FROM OSMHELPER.PUBLIC.${prefix}_WAY_TAG WHERE ID_WAY=305633653",{ row ->
            assertEquals 2, row.nb }
    }

    @Test
    void loadTransformPolygonsTest() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())
        def transform = OSMHelper.Transform.toPolygons()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix,
                epsgCode :2154)
        assertEquals 126, h2GIS.getTable(transform.results.outputTableName).rowCount
    }

    @Test
    void loadTransformPolygonsFilteredTest() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())

        def transform = OSMHelper.Transform.toPolygons()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix,
                epsgCode :2154,
                tag_keys:["building"])
        assertEquals 125, h2GIS.getTable(transform.results.outputTableName).rowCount
    }

    @Test
    void loadTransformLinesTest() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())
        def transform = OSMHelper.Transform.toLines()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154)
        h2GIS.save(transform.results.outputTableName, "/tmp/osm.shp")

        assertEquals 167, h2GIS.getTable(transform.results.outputTableName).rowCount
    }

    @Test
    void loadTransformPointsFilteredTest() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())

        def transform = OSMHelper.Transform.toPoints()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix,
                epsgCode :2154, tag_keys:["place"])
        assertEquals 3, h2GIS.getTable(transform.results.outputTableName).rowCount

    }


    @Test
    void extractLandUseTest() {
        File dbFile = File.createTempFile("osmhelper", ".db")
        JdbcDataSource dataSource = H2GIS.open(dbFile.path)
        assertNotNull(dataSource)
        logger.info("DataSource : "+dbFile.path)
        IProcess process = OSMHelper.OSMTemplate.LANDCOVER()

        process.execute(bbox: "45.1431837,5.6428528,45.2249744,5.7877350",datasource:null)
        // process.execute(bbox: "47.7411704,-3.1272411,47.7606760,-3.0910206", datasource: dataSource)
        assertNotNull(process.results.datasource.getTable(process.results.outputTableName))
        File dbFile2 = File.createTempFile("osmhelperlandcover", ".shp")

        process.results.datasource.save(process.results.outputTableName, dbFile2.path)
    }

    @Test
    void extractBuildingTest() {
            File dbFile = File.createTempFile("osmhelper", ".db")
            JdbcDataSource dataSource = H2GIS.open(dbFile.path)
            assertNotNull(dataSource)
            logger.info("DataSource : "+dbFile.path)
            IProcess process = OSMHelper.OSMTemplate.BUILDING()

            process.execute(bbox: "45.1431837,5.6428528,45.2249744,5.7877350", datasource: null)
            assertNotNull(process.results.datasource.getTable(process.results.outputTableName))
            File dbFile2 = File.createTempFile("osmhelperbuilding", ".shp")
            process.results.datasource.save(process.results.outputTableName, dbFile2.path)
        //OSMTemplate.BUILDING(place : , bbox:"", area:"", adminLevel:"", inseecode:"").save()
        // voir osmnix

    }

    @Test
    void extractPlace() {
        //OSMHelper.Utilities.getArea("vannes");
    }

}