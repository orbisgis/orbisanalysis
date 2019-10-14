package org.orbisgis.osm

import org.h2gis.functions.spatial.crs.ST_Transform
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

class OSMToolsTests {

    private static final Logger logger = LoggerFactory.getLogger(OSMToolsTests.class)

    @Test
    void extractTest() {
        def extract = OSMTools.Loader.extract()
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
        def extract = OSMTools.Loader.extract()
        Geometry geom = OSMTools.Utilities.getAreaFromPlace("Cliscouët, vannes")
        def query = OSMTools.Utilities.buildOSMQuery(new GeometryFactory().toGeometry(geom.getEnvelopeInternal()), [],
                OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)
        assertTrue extract.execute(overpassQuery: query);
        assertTrue new File(extract.results.outputFilePath).exists()
        assertTrue new File(extract.results.outputFilePath).length() > 0
    }

    @Test
    void extractTestWrongQuery() {
        def extract = OSMTools.Loader.extract()
        assertFalse extract.execute(overpassQuery: "building =yes");
    }

    @Test
    void loadTest() {
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix, osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())

        //Count nodes
        h2GIS.eachRow "SELECT count(*) as nb FROM OSMTools.PUBLIC.${prefix}_NODE",{ row ->
            assertEquals 1176, row.nb }

        //Count ways
        h2GIS.eachRow "SELECT count(*) as nb FROM OSMTools.PUBLIC.${prefix}_WAY",{ row ->
            assertEquals 171, row.nb }

        //Count relations
        h2GIS.eachRow "SELECT count(*) as nb FROM OSMTools.PUBLIC.${prefix}_RELATION",{ row ->
            assertEquals 0, row.nb }

        //Check specific tags
        h2GIS.eachRow "SELECT count(*) as nb FROM OSMTools.PUBLIC.${prefix}_WAY_TAG WHERE ID_WAY=305633653",{ row ->
            assertEquals 2, row.nb }
    }

    @Test
    void loadTransformPolygonsTest() {
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())
        def transform = OSMTools.Transform.toPolygons()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154)
        assertEquals 126, h2GIS.getTable(transform.results.outputTableName).rowCount
    }

    @Test
    void loadTransformPolygonsFilteredTest() {
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())
        def transform = OSMTools.Transform.toPolygons()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154, tags:['building'])
        assertEquals 125, h2GIS.getTable(transform.results.outputTableName).rowCount
    }

    @Test
    void loadTransformLinesTest() {
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())
        def transform = OSMTools.Transform.toLines()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154)
        assertEquals 167, h2GIS.getTable(transform.results.outputTableName).rowCount
    }

    @Test
    void loadTransformPointsNotFilteredTest() {
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())

        def transform = OSMTools.Transform.toPoints()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154)
        assertEquals 4, h2GIS.getTable(transform.results.outputTableName).rowCount
    }

    @Test
    void loadTransformPointsFilteredTest() {
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())

        def transform = OSMTools.Transform.toPoints()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154, tags:['place'])
        assertEquals 3, h2GIS.getTable(transform.results.outputTableName).rowCount
    }

    @Test
    void loadTransformPointsFilteredWithColumnsTest() {
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())

        def transform = OSMTools.Transform.toPoints()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154, tags:['amenity'], columnsToKeep:['iut','operator', 'amenity', 'wheelchair'])
        assertEquals 223, h2GIS.getTable(transform.results.outputTableName).rowCount
        assertEquals "ID_NODE,THE_GEOM,AMENITY,OPERATOR,WHEELCHAIR", h2GIS.getTable(transform.results.outputTableName).columnNames.join(",")
    }


    @Test
    void loadTransformLinesWithoutKeysTest() {
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())
        def transform = OSMTools.Transform.toLines()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154, tags:['toto'])
        assertNull(transform.results.outputTableName)
    }


    @Test
    void extractLandCoverTest() {
        JdbcDataSource dataSource = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        assertNotNull(dataSource)
        def paramsDefaultFile = this.class.getResource("landcover_tags.json").toURI()
        Map  parameters = OSMTools.Utilities.readJSONParameters(paramsDefaultFile)
        IProcess process = OSMTools.Loader.fromArea()
        assertTrue process.execute(filterArea: new Envelope(-3.1153294444084167 , -3.112971782684326, 48.73787306084071 , 48.73910952775334),datasource:dataSource)
        IProcess processTrans = OSMTools.Transform.toPolygons()
        assertTrue processTrans.execute(datasource: dataSource,
                osmTablesPrefix: process.getResults().osmTablesPrefix,
                epsgCode:process.getResults().epsg, tags: parameters["tags"])
        assertNotNull(dataSource.getTable(processTrans.results.outputTableName))
        assertEquals 3, dataSource.getTable(processTrans.results.outputTableName).rowCount
    }

    @Test
    void extractBuildingTest() {
        JdbcDataSource dataSource = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        assertNotNull(dataSource)
        def paramsDefaultFile = this.class.getResource("building_tags.json").toURI()
        Map  parameters = OSMTools.Utilities.readJSONParameters(paramsDefaultFile)
        IProcess process = OSMTools.Loader.fromArea()
        assertTrue process.execute(filterArea: new Envelope(26.87346652150154 , 26.874645352363586, -31.899314836854924 , -31.898518974220607), datasource: dataSource)
        IProcess processTrans = OSMTools.Transform.toPolygons()
        assertTrue processTrans.execute(datasource: dataSource,
                osmTablesPrefix: process.getResults().osmTablesPrefix,
                epsgCode:process.getResults().epsg, tags: parameters["tags"])
        assertNotNull(dataSource.getTable(processTrans.results.outputTableName))
        assertEquals 1, dataSource.getTable(processTrans.results.outputTableName).rowCount
    }

    @Test
    void extractPlace() {
        WKTReader wktReader = new WKTReader();
        Geometry geom = OSMTools.Utilities.getAreaFromPlace("vannes")
        assertNotNull geom;
        geom = OSMTools.Utilities.getAreaFromPlace("lyon");
        assertNotNull geom;
        geom = OSMTools.Utilities.getAreaFromPlace("Baarle-Nassau");
        assertNotNull geom;
        assertEquals(9,geom.getNumGeometries())
        geom = OSMTools.Utilities.getAreaFromPlace("Baerle-Duc");
        assertNotNull geom;
        assertEquals(24,geom.getNumGeometries())
        geom = OSMTools.Utilities.getAreaFromPlace("séné");
        assertNotNull geom;
        assertEquals(6,geom.getNumGeometries())
        geom = OSMTools.Utilities.getAreaFromPlace("Pékin");
        assertNotNull geom;
        geom = OSMTools.Utilities.getAreaFromPlace("Chongqing");
        assertNotNull geom;
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
                OSMTools.Utilities.buildOSMQuery(p.getEnvelopeInternal(), ["WATER", "BUILDING"], OSMElement.NODE, OSMElement.RELATION,OSMElement.WAY)
        assertEquals "[bbox:0.0,4.0,20.0, 30.0];\n" +
                "(\n" +
                "\tnode[\"water\"](poly:\"2.0 4.0 20.0 10.0 20.0 30.0 0.0 30.0\");\n" +
                "\trelation[\"water\"](poly:\"2.0 4.0 20.0 10.0 20.0 30.0 0.0 30.0\");\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(p,["WATER"], OSMElement.NODE, OSMElement.RELATION)
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
                "out;", OSMTools.Utilities.buildOSMQuery(p.getEnvelopeInternal(),[], OSMElement.NODE, OSMElement.RELATION)
    }

    @Test
    void extractPlaceLoadTransformPolygonsFilteredBboxTest() {
        JdbcDataSource dataSource = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        assertNotNull(dataSource)
        Geometry geom = OSMTools.Utilities.getAreaFromPlace("Cliscouët, vannes");
        def paramsDefaultFile = this.class.getResource("building_tags.json").toURI()
        Map  parameters = OSMTools.Utilities.readJSONParameters(paramsDefaultFile)
        IProcess process = OSMTools.Loader.fromArea()
        assertTrue process.execute(filterArea: geom.getEnvelopeInternal(),datasource:dataSource)
        IProcess processTrans = OSMTools.Transform.toPolygons()
        assertTrue processTrans.execute(datasource: dataSource,
                osmTablesPrefix: process.getResults().osmTablesPrefix,
                epsgCode:process.getResults().epsg, tags: parameters["tags"])
        assertNotNull(dataSource.getTable(processTrans.results.outputTableName))
        File output = new File("./target/osm_building_bbox_from_place.shp")
        if(output.exists()){
            output.delete()
        }
        assertTrue dataSource.save(processTrans.results.outputTableName, './target/osm_building_bbox_from_place.shp')
    }


    @Test
    void extractLoadTransformRelationTest() {
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osm_one_relation.osm").toURI()).getAbsolutePath())
        IProcess transform = OSMTools.Transform.extractRelationsAsLines()
        transform.execute(datasource:  h2GIS,osmTablesPrefix:  prefix, epsgCode:  2154, tags : ['building'])
        assertEquals 1, h2GIS.getTable(transform.getResults().outputTableName).rowCount
        def row = h2GIS.firstRow("SELECT * FROM $transform.results.outputTableName WHERE ID='r2614051'")
        assertEquals("yes", row.'BUILDING')
    }


    @Test
    void getSERVER_STATUS(){
         assertNotNull OSMTools.SERVER_STATUS
    }

    @Test
    void extractBuilding() {
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())

        def transform = OSMTools.Transform.toPolygons()
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
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())

        def transform = OSMTools.Transform.extractWaysAsLines()
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
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())

        def transform = OSMTools.Transform.toLines()
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
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())

        def transform = OSMTools.Transform.toPolygons()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154, tags : ['landcover', 'natural', 'landuse', 'water', 'waterway', 'leisure', 'aeroway', 'amenity', 'layer'])
        assertNotNull(transform.results.outputTableName)
        assertEquals (207, h2GIS.getTable(transform.results.outputTableName).rowCount)
        assertEquals ("AMENITY,ID,LANDUSE,LAYER,LEISURE,NATURAL,THE_GEOM,WATER,WATERWAY", h2GIS.getTable(transform.results.outputTableName).columnNames.join(","))
        def row = h2GIS.firstRow("SELECT * FROM $transform.results.outputTableName WHERE ID='r278505'")
        assertEquals("meadow", row.'LANDUSE')
        assertNull(row.'LEISURE')
    }


    @Test
    void extractGeometryBorder1() {
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())

        def transform = OSMTools.Transform.toPolygons()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154, tags : ['leisure'])
        assertNotNull(transform.results.outputTableName)
        assertEquals 6, h2GIS.getTable(transform.results.outputTableName).rowCount
        assertEquals "ID,LEISURE,THE_GEOM", h2GIS.getTable(transform.results.outputTableName).columnNames.join(",")
        def row = h2GIS.firstRow("SELECT * FROM $transform.results.outputTableName WHERE ID='w717203616'")
        assertEquals("park", row.'LEISURE')
    }

    @Test
    void extractGeometryBorder2() {
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')

        WKTReader wktReader  = new WKTReader();
        Geometry geom  = wktReader.read("POLYGON ((598003.4849443114 5325978.288854313, 598019.7449348173 5325982.413495027, 598036.9966877099 5325985.176546716, 598042.8267686924 5325984.4979892755, 598046.4229000872 5325986.371658631, 598078.3655819619 5325991.34276151, 598083.8878627924 5325991.237100496, 598092.6227712187 5325992.776229829, 598096.3492982117 5325994.429808194, 598132.8710515047 5326000.424558396, 598137.6442693556 5325999.249950156, 598140.915042991 5325999.67281278, 598144.1441265537 5326002.096146798, 598163.6381410899 5326004.864608504, 598170.38531015 5326005.013471729, 598173.4626870117 5326000.196612171, 598184.5724560271 5325998.352295577, 598196.2420131462 5325997.318059992, 598205.3328128117 5325998.085218615, 598213.045235186 5325999.729315703, 598215.4667951063 5325986.6630382035, 598217.1775331628 5325972.906407454, 598219.3241563962 5325960.669248893, 598223.9881157577 5325968.92063883, 598228.4935074368 5325987.297530931, 598243.6522338333 5325996.117847299, 598268.7026505754 5326002.005880023, 598307.0470037074 5326017.70537465, 598323.2262882312 5326021.740560573, 598334.6636723676 5326022.514810426, 598335.0902894547 5326028.0476212725, 598407.4719685452 5326031.668556132, 598383.2103152417 5325844.341512322, 598399.2792301291 5325846.151465277, 598420.5409319543 5325732.493249696, 598424.7707265288 5325731.743135763, 598480.2760708437 5325737.821316522, 598508.1153171442 5325741.456893762, 598535.14038922 5325746.079210713, 598593.0807826507 5325761.316850651, 598649.7835026709 5325778.323745295, 598107.9648758237 5325707.836889621, 598109.7551539204 5325733.838410134, 598111.7318708443 5325763.311837929, 598100.7445987663 5325804.570569212, 598097.7537505792 5325822.618984062, 598084.9077483611 5325851.905560522, 598075.518004116 5325867.732179461, 598064.1509316695 5325885.481708668, 598052.6330608106 5325896.36907385, 598038.0839131859 5325907.204635104, 598026.5082231611 5325921.915542008, 598018.8355672884 5325935.370183012, 598011.6799352192 5325951.268441437, 598004.8439583599 5325968.517409957, 598004.2007638263 5325974.765667894, 598003.4849443114 5325978.288854313))")
        geom.setSRID(32630)
        geom = ST_Transform.ST_Transform(h2GIS.getConnection(), geom, 4326)

        def query = OSMTools.Utilities.buildOSMQuery(new GeometryFactory().toGeometry(geom.getEnvelopeInternal())
                , [], NODE, WAY, RELATION)
        def extract = OSMTools.Loader.extract()
        if (!query.isEmpty()) {
            if (extract.execute(overpassQuery: query)) {
                def prefix = "OSM_FILE_${OSMTools.getUuid()}"
                def load = OSMTools.Loader.load()
                if (load(datasource: h2GIS, osmTablesPrefix: prefix, osmFilePath:extract.results.outputFilePath)) {
                    def tags = ['landuse']
                    def transform = OSMTools.Transform.toPolygons()
                    transform.execute(datasource: h2GIS, osmTablesPrefix: prefix, epsgCode: 32630, tags: tags)
                    assertNotNull(transform.results.outputTableName)
                    h2GIS.getSpatialTable(transform.results.outputTableName)
                }
            }
        }
    }

    @Test
    void parseJSONParameters1(){
        def paramsDefaultFile = this.class.getResource("road_tags.json").toURI()
        Map  parameters = OSMTools.Utilities.readJSONParameters(paramsDefaultFile)
        assertEquals(["highway", "cycleway", "biclycle_road", "cyclestreet", "route", "junction"],parameters["tags"])
        assertEquals(["width","highway", "surface", "sidewalk",
                      "lane","layer","maxspeed","oneway",
                      "h_ref","route","cycleway",
                      "biclycle_road","cyclestreet","junction"],parameters["columns"])
    }

    @Test
    void parseJSONParameters2(){
        def paramsDefaultFile = this.class.getResource("road_tags_values.json").toURI()
        Map  parameters = OSMTools.Utilities.readJSONParameters(paramsDefaultFile)
        assertEquals(["highway":["motorway", "trunk", "primary"], "cycleway":[""]],parameters["tags"])
        assertEquals(["width","highway", "surface", "sidewalk",
                      "lane","layer","maxspeed","oneway",
                      "h_ref","route","cycleway",
                      "biclycle_road","cyclestreet","junction"],parameters["columns"])
    }

    @Test
    void parseJSONParameters3(){
        Map  parameters = OSMTools.Utilities.readJSONParameters("/tmp/test.ba")
        assertNull(parameters)
    }

    @Test
    void cleanTableTest() {
        JdbcDataSource h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("osmFileForTest.osm").toURI()).getAbsolutePath())
        def transform = OSMTools.Transform.toPolygons()
        transform.execute( datasource:h2GIS, osmTablesPrefix:prefix, epsgCode :2154)
        assertEquals 126, h2GIS.getTable(transform.results.outputTableName).rowCount
        OSMTools.Utilities.dropOSMTables(prefix, h2GIS)
        assertFalse h2GIS.tableNames.any {it.contains(prefix)}
    }



    //@Test //disable. It uses for test purpose
    void dev() {
        def h2GIS = H2GIS.open('./target/OSMTools;AUTO_SERVER=TRUE')

        Geometry geom = OSMTools.Utilities.getAreaFromPlace("Bouguenais");

        def query = OSMTools.Utilities.buildOSMQuery(new GeometryFactory().toGeometry(geom.getEnvelopeInternal())
                , [], NODE, WAY, RELATION)
        def extract = OSMTools.Loader.extract()
        if (!query.isEmpty()) {
            if (extract.execute(overpassQuery: query)) {
                def prefix = "OSM_FILE_${OSMTools.getUuid()}"
                def load = OSMTools.Loader.load()
                if (load(datasource: h2GIS, osmTablesPrefix: prefix, osmFilePath:extract.results.outputFilePath)) {

                    def tags = ['highway']

                    def transform = OSMTools.Transform.toLines()
                    transform.execute(datasource: h2GIS, osmTablesPrefix: prefix, epsgCode: 2154, tags: tags)
                    assertNotNull(transform.results.outputTableName)
                    h2GIS.getTable(transform.results.outputTableName).save("/tmp/data_osm_road.shp")
                }
            }
     }
 }
}