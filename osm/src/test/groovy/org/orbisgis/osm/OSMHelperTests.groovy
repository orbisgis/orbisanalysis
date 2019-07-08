package org.orbisgis.osm

import org.orbisgis.datamanagerapi.dataset.ITable
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS


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


    @Test
    void loadTest() {
        def h2GIS = H2GIS.open('./target/osmhelper')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix, osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())
        //Check tables
        assertEquals 11, h2GIS.getTableNames().count{
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
    void extractLoadChainTest() {
        def h2GIS = H2GIS.open('./target/osmhelper')
    }


    @Test
    void loadTransformTest() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File("/home/ebocher/Autres/codes/OSMHelper/src/test/resources/org/orbisgis/osmFileForTest.osm").getAbsolutePath())

        def transform = OSMHelper.Transform.toPolygons()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix,
                epsgCode :2154)

        h2GIS.save(transform.results.outputTableName,"/tmp/osm_polygons.shp")
    }

    @Test
    void extractLoadTransformBuildingTest() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def extract = OSMHelper.Loader.extract()
        assertTrue extract.execute(overpassQuery: "[bbox:47.641682554548815,-2.104182243347168,47.660502734818216,-2.0664596557617188];((node[building=yes];way[building=yes];relation[building=yes];);>;);out;");

        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix, osmFilePath : extract.results.outputFilePath)

        def transform = OSMHelper.Transform.toPolygons()
        assertTrue transform.execute( datasource:h2GIS, osmTablesPrefix:prefix,epsgCode :2154)
    }

    @Test
    void extractLoadTransformLandcoverTest() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def extract = OSMHelper.Loader.extract()
        assertTrue extract.execute(overpassQuery: "[bbox:48.78537820754818,2.195892333984375,48.93242424920101,2.4976730346679688];" +
                "(((node[landuse];node[leisure];);(way[landuse];way[leisure];);(relation[landuse];relation[leisure];););>;);out;");

        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix, osmFilePath : extract.results.outputFilePath)

        def transform = OSMHelper.Transform.toPolygons()
        assertTrue transform.execute( datasource:h2GIS, osmTablesPrefix:prefix,epsgCode :2154)


        h2GIS.save(transform.results.outputTableName,"/tmp/osm_polygons.shp")

    }

    @Test
    void extractBuilding() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')

        //OSMTemplate.dataSource=""
        //OSMTemplate.BUILDING(place : , bbox:"", area:"", adminLevel:"", inseecode:"").save()
        // voir osmnix

    }

}