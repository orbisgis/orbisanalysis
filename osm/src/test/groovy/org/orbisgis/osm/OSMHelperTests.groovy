package org.orbisgis.osm


import org.junit.jupiter.api.Test
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.WKTReader
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanagerapi.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.*
import static org.orbisgis.osm.OSMElement.NODE
import static org.orbisgis.osm.OSMElement.RELATION
import static org.orbisgis.osm.OSMElement.WAY

class OSMHelperTests {

    private static final Logger logger = LoggerFactory.getLogger(OSMHelperTests.class)

    @Test
    void extractTest() {
        def extract = OSMHelper.Loader.extract()
        assertTrue extract.execute(overpassQuery: "(node(48.780889172043,-3.0626213550568,48.783356423929,-3.0579113960266); " +
                "way(48.780889172043,-3.0626213550568,48.783356423929,-3.0579113960266); " +
                "relation(48.780889172043,-3.0626213550568,48.783356423929,-3.0579113960266); );\n" +
                "out;\n" +
                ">;");
        assertTrue new File(extract.results.outputFilePath).exists()
        assertTrue new File(extract.results.outputFilePath).length() > 0
    }

    @Test
    void extractTestFromPlace() {
        def extract = OSMHelper.Loader.extract()
        Geometry geom = OSMHelper.Utilities.getAreaFromPlace("Cliscouët, vannes")
        def query = OSMHelper.Utilities.buildOSMQuery(new GeometryFactory().toGeometry(geom.getEnvelopeInternal()), [],
                OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)
        assertTrue extract.execute(overpassQuery: query);
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
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix, osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())

        //Count nodes
        h2GIS.eachRow "SELECT count(*) as nb FROM OSMHELPER.PUBLIC.${prefix}_NODE",{ row ->
            assertEquals 1176, row.nb }

        //Count ways
        h2GIS.eachRow "SELECT count(*) as nb FROM OSMHELPER.PUBLIC.${prefix}_WAY",{ row ->
            assertEquals 171, row.nb }

        //Count relations
        h2GIS.eachRow "SELECT count(*) as nb FROM OSMHELPER.PUBLIC.${prefix}_RELATION",{ row ->
            assertEquals 0, row.nb }

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
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154)
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
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154, tags:['building'])
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
        assertEquals 167, h2GIS.getTable(transform.results.outputTableName).rowCount
    }

    @Test
    void loadTransformPointsNotFilteredTest() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())

        def transform = OSMHelper.Transform.toPoints()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154)
        assertEquals 4, h2GIS.getTable(transform.results.outputTableName).rowCount
    }

    @Test
    void loadTransformPointsFilteredTest() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())

        def transform = OSMHelper.Transform.toPoints()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154, tags:['place'])
        assertEquals 3, h2GIS.getTable(transform.results.outputTableName).rowCount
    }

    @Test
    void loadTransformPointsFilteredWithColumnsTest() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())

        def transform = OSMHelper.Transform.toPoints()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154, tags:['amenity'], columnsToKeep:['iut','operator', 'amenity', 'wheelchair'])
        h2GIS.getTable(transform.results.outputTableName).save("/tmp/points_osm.shp")
        //assertEquals 3, h2GIS.getTable(transform.results.outputTableName).rowCount
    }


    @Test
    void loadTransformLinesWithoutKeysTest() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())
        def transform = OSMHelper.Transform.toLines()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154, tags:['toto'])
        assertNull(transform.results.outputTableName)
    }


    @Test
    void extractLandCoverTest() {
        JdbcDataSource dataSource = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        assertNotNull(dataSource)
        IProcess process = OSMHelper.OSMTemplate.LANDCOVER()
        process.execute(filterArea: new Envelope(-3.1153294444084167 , -3.112971782684326, 48.73787306084071 , 48.73910952775334),datasource:dataSource)
        assertNotNull(process.results.datasource.getTable(process.results.outputPolygonsTableName))
        assertEquals 3, dataSource.getTable(process.results.outputPolygonsTableName).rowCount
    }

    @Test
    void extractBuildingTest() {
        JdbcDataSource dataSource = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        assertNotNull(dataSource)
        IProcess process = OSMHelper.OSMTemplate.BUILDING()
        process.execute(filterArea: new Envelope(26.87346652150154 , 26.874645352363586, -31.899314836854924 , -31.898518974220607), datasource: dataSource)
        assertNotNull(process.results.datasource.getTable(process.results.outputPolygonsTableName))
        assertEquals 1, dataSource.getTable(process.results.outputPolygonsTableName).rowCount
    }

    @Test
    void extractWaterTest() {
        JdbcDataSource dataSource = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        assertNotNull(dataSource)
        IProcess process = OSMHelper.OSMTemplate.WATER()
        process.execute(filterArea: new Envelope(-3.143248558044433 , -3.124387264251709, 48.25956997946164 , 48.269554636080265), datasource: dataSource)
        assertNotNull(dataSource.getTable(process.results.outputPolygonsTableName))
        assertEquals 1, dataSource.getTable(process.results.outputPolygonsTableName).rowCount
    }

    @Test
    void extractPlace() {
        WKTReader wktReader = new WKTReader();
        Geometry geom = OSMHelper.Utilities.getAreaFromPlace("vannes")
        assertNotNull geom;
        geom = OSMHelper.Utilities.getAreaFromPlace("lyon");
        assertNotNull geom;
        geom = OSMHelper.Utilities.getAreaFromPlace("Baarle-Nassau");
        assertNotNull geom;
        assertEquals(9,geom.getNumGeometries())
        geom = OSMHelper.Utilities.getAreaFromPlace("Baerle-Duc");
        assertNotNull geom;
        assertEquals(24,geom.getNumGeometries())
        geom = OSMHelper.Utilities.getAreaFromPlace("séné");
        assertNotNull geom;
        assertEquals(6,geom.getNumGeometries())
    }



    @Test
    void buildOSMQuery() {
        WKTReader  wktReader = new WKTReader()
        Geometry p = wktReader.read("POLYGON ((4 2, 10 20, 30 20, 30 0, 4 2))")
        assertEquals "[bbox:0.0,4.0,20.0, 30.0];\n" +
                "(\n" +
                "\tnode[\"water\"];\n" +
                "\tnode[\"building\"];\n" +
                "\trelation[\"water\"];\n" +
                "\trelation[\"building\"];\n" +
                "\tway[\"water\"];\n" +
                "\tway[\"building\"];\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;" ,
                OSMHelper.Utilities.buildOSMQuery(p.getEnvelopeInternal(), ["WATER", "BUILDING"], OSMElement.NODE, OSMElement.RELATION,OSMElement.WAY)
        assertEquals "[bbox:0.0,4.0,20.0, 30.0];\n" +
                "(\n" +
                "\tnode[\"water\"](poly:\"2.0 4.0 20.0 10.0 20.0 30.0 0.0 30.0\");\n" +
                "\trelation[\"water\"](poly:\"2.0 4.0 20.0 10.0 20.0 30.0 0.0 30.0\");\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMHelper.Utilities.buildOSMQuery(p,["WATER"], OSMElement.NODE, OSMElement.RELATION)
    }

    @Test
    void buildOSMQueryEmptyKeys() {
        WKTReader  wktReader = new WKTReader()
        Geometry p = wktReader.read("POLYGON ((4 2, 10 20, 30 20, 30 0, 4 2))")
        assertEquals "[bbox:0.0,4.0,20.0, 30.0];\n" +
                "(\n" +
                "\tnode;\n" +
                "\trelation;\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMHelper.Utilities.buildOSMQuery(p.getEnvelopeInternal(),[], OSMElement.NODE, OSMElement.RELATION)
    }

    @Test
    void extractPlaceLoadTransformPolygonsFilteredBboxTest() {
        JdbcDataSource dataSource = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        assertNotNull(dataSource)
        Geometry geom = OSMHelper.Utilities.getAreaFromPlace("Cliscouët, vannes");
        IProcess process = OSMHelper.OSMTemplate.BUILDING()
        process.execute(filterArea: geom.getEnvelopeInternal(),datasource:dataSource)
        File output = new File("./target/osm_building_bbox_from_place.shp")
        if(output.exists()){
            output.delete()
        }
        assertTrue process.results.datasource.save(process.results.outputPolygonsTableName, './target/osm_building_bbox_from_place.shp')
    }


    //@Test doesn't work here... POST method must be used
    void extractPlaceLoadTransformPolygonsFilteredPolyTest() {
        JdbcDataSource dataSource = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        assertNotNull(dataSource)
        Geometry geom = OSMHelper.Utilities.getAreaFromPlace("Cliscouët, vannes")
        IProcess process = OSMHelper.OSMTemplate.BUILDING()
        process.execute(filterArea: geom,datasource:dataSource)
        File output = new File("./target/osm_building_poly_from_place.shp")
        if(output.exists()){
            output.delete()
        }
        assertTrue process.results.datasource.save(process.results.outputPolygonsTableName, './target/osm_building_poly_from_place.shp')
    }

    @Test
    void extractLoadTransformRelationTest() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osm_one_relation.osm").toURI()).getAbsolutePath())
        IProcess transform = OSMHelper.Transform.extractRelationsAsLines()
        transform.execute(datasource:  h2GIS,osmTablesPrefix:  prefix, epsgCode:  2154, tags : ['building'])
        assertEquals 1, h2GIS.getTable(transform.getResults().outputTableName).rowCount
        def row = h2GIS.firstRow("SELECT * FROM $transform.results.outputTableName WHERE ID='r2614051'")
        assertEquals("yes", row.'BUILDING')
    }


    @Test
    void getSERVER_STATUS(){
         assertNotNull OSMHelper.SERVER_STATUS
    }

    @Test
    void extractBuilding() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())

        def transform = OSMHelper.Transform.toPolygons()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154, tags : ['building','building:levels'])
        assertNotNull(transform.results.outputTableName)
        assertEquals 1038, h2GIS.getTable(transform.results.outputTableName).rowCount
        assertEquals "BUILDING,BUILDING:LEVELS,ID,THE_GEOM", h2GIS.getTable(transform.results.outputTableName).columnNames.join(",")
        def row = h2GIS.firstRow("SELECT * FROM $transform.results.outputTableName WHERE ID='w122538293'")
        assertEquals("retail", row.'BUILDING')
        assertEquals("0", row.'BUILDING:LEVELS')
    }

    @Test
    void extractWaysRoads() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())

        def transform = OSMHelper.Transform.extractWaysAsLines()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154, tags : ['highway'])
        assertNotNull(transform.results.outputTableName)
        assertEquals 360, h2GIS.getTable(transform.results.outputTableName).rowCount
        assertEquals(0, h2GIS.firstRow("SELECT count(*) as count FROM $transform.results.outputTableName WHERE HIGHWAY IS NULL").count)
        def row = h2GIS.rows "SELECT count(*) as count, HIGHWAY FROM $transform.results.outputTableName group by HIGHWAY"
        Map results = [:]
        row.collect { results.put(it.'HIGHWAY' , it."count")}
        assertTrue(["cycleway" : 1, "footway" : 115, "living_street" : 2,
                      "path" : 7, "pedestrian" : 11, "primary" : 23, "primary_link" : 1,
                      "residential" : 71, "secondary" : 6, "service" : 66, "steps" : 24,
                      "tertiary" : 13, "track" : 3, "traffic_island" : 2, "unclassified" : 15]==
                 results)
    }

    @Test
    void extractLinesRoads() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())

        def transform = OSMHelper.Transform.toLines()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154, tags : ['highway'])
        assertNotNull(transform.results.outputTableName)
        assertEquals 362, h2GIS.getTable(transform.results.outputTableName).rowCount

        assertEquals(0, h2GIS.firstRow("SELECT count(*) as count FROM $transform.results.outputTableName WHERE HIGHWAY IS NULL").count)
        def row = h2GIS.rows "SELECT count(*) as count, HIGHWAY FROM $transform.results.outputTableName group by HIGHWAY"
        Map results = [:]
        row.collect { results.put(it.'HIGHWAY' , it."count")}
        assertTrue(["cycleway" : 1, "footway" : 116, "living_street" : 2,
                    "path" : 7, "pedestrian" : 12, "primary" : 23, "primary_link" : 1,
                    "residential" : 71, "secondary" : 6, "service" : 66, "steps" : 24,
                    "tertiary" : 13, "track" : 3, "traffic_island" : 2, "unclassified" : 15]==
                results)
    }


    @Test
    void extractLandcover() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())

        def transform = OSMHelper.Transform.toPolygons()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154, tags : ['landcover', 'natural', 'landuse', 'water', 'waterway', 'leisure', 'aeroway', 'amenity', 'layer'])
        assertNotNull(transform.results.outputTableName)
        assertEquals (207, h2GIS.getTable(transform.results.outputTableName).rowCount)
        assertEquals ("AMENITY,ID,LANDUSE,LAYER,LEISURE,NATURAL,THE_GEOM,WATER,WATERWAY", h2GIS.getTable(transform.results.outputTableName).columnNames.join(","))
        def row = h2GIS.firstRow("SELECT * FROM $transform.results.outputTableName WHERE ID='r278505'")
        assertEquals("meadow", row.'LANDUSE')
        assertNull(row.'LEISURE')
    }


    @Test
    void extractGeometryBorder() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')
        def load = OSMHelper.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())

        def transform = OSMHelper.Transform.toPolygons()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154, tags : ['leisure'])
        assertNotNull(transform.results.outputTableName)
        h2GIS.getTable(transform.results.outputTableName).save("/tmp/redon_osm.shp")
        assertEquals 6, h2GIS.getTable(transform.results.outputTableName).rowCount
        assertEquals "ID,LEISURE,THE_GEOM", h2GIS.getTable(transform.results.outputTableName).columnNames.join(",")
        def row = h2GIS.firstRow("SELECT * FROM $transform.results.outputTableName WHERE ID='w717203616'")
        assertEquals("park", row.'LEISURE')
    }

    @Test
    void dev() {
        def h2GIS = H2GIS.open('./target/osmhelper;AUTO_SERVER=TRUE')

        Geometry geom = OSMHelper.Utilities.getAreaFromPlace("Vannes");

        def query = OSMHelper.Utilities.buildOSMQuery(new GeometryFactory().toGeometry(geom.getEnvelopeInternal())
                , [], NODE, WAY, RELATION)
        def extract = OSMHelper.Loader.extract()
        if (!query.isEmpty()) {
            if (extract.execute(overpassQuery: query)) {
                def prefix = "OSM_FILE_${OSMHelper.getUuid()}"
                def load = OSMHelper.Loader.load()
                //info "Loading"
                if (load(datasource: h2GIS, osmTablesPrefix: prefix, osmFilePath: extract.results.outputFilePath)) {

                    //def tags = ['building']

                    //def tags = ['highway']

                    def tags = ['landcover', 'landuse']

                    def transform = OSMHelper.Transform.toPolygons()
                    transform.execute(datasource: h2GIS, osmTablesPrefix: prefix, epsgCode: 2154, tags: tags)
                    assertNotNull(transform.results.outputTableName)
                    h2GIS.getTable(transform.results.outputTableName).save("/tmp/redon_osm.shp")
                }
            }
        }
    }
}