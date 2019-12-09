package org.orbisgis.osm

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Coordinates
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Polygon
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.osm.utils.OSMElement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.*

/**
 * Test class for {@link Utilities}
 *
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */
class UtilitiesTest extends AbstractOSMTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(UtilitiesTest)

    @BeforeEach
    final void beforeEach(TestInfo testInfo){
        LOGGER.info("@ ${testInfo.testMethod.get().name}()")
        super.beforeEach()
    }

    @AfterEach
    final void afterEach(TestInfo testInfo){
        super.afterEach()
        LOGGER.info("# ${testInfo.testMethod.get().name}()")
    }

    /**
     * Test the {@link Utilities#arrayToCoordinate(java.lang.Object)} method.
     */
    @Test
    void arrayToCoordinateTest(){
        def outer = []
        outer << [0.0, 0.0, 0.0]
        outer << [10.0, 0.0]
        outer << [10.0, 10.0, 0.0]
        outer << [0.0, 10.0]
        outer << [0.0, 0.0, 0.0]

        def coordinates = OSMTools.Utilities.arrayToCoordinate(outer)
        assertEquals 5, coordinates.size()
        assertEquals "(0.0, 0.0, 0.0)", coordinates[0].toString()
        assertEquals "(10.0, 0.0, NaN)", coordinates[1].toString()
        assertEquals "(10.0, 10.0, 0.0)", coordinates[2].toString()
        assertEquals "(0.0, 10.0, NaN)", coordinates[3].toString()
        assertEquals "(0.0, 0.0, 0.0)", coordinates[4].toString()
    }

    /**
     * Test the {@link Utilities#arrayToCoordinate(java.lang.Object)} method with bad data.
     */
    @Test
    void badArrayToCoordinateTest(){
        def array1 = OSMTools.Utilities.arrayToCoordinate(null)
        assertNotNull array1
        assertEquals 0, array1.length

        def array2 = OSMTools.Utilities.arrayToCoordinate([])
        assertNotNull array2
        assertEquals 0, array2.length

        def array3 = OSMTools.Utilities.arrayToCoordinate([[0]])
        assertNotNull array3
        assertEquals 0, array3.length

        def array4 = OSMTools.Utilities.arrayToCoordinate([[0, 1, 2, 3]])
        assertNotNull array4
        assertEquals 0, array4.length
    }

    /**
     * Test the {@link Utilities#parsePolygon(java.lang.Object, org.locationtech.jts.geom.GeometryFactory)}
     * method.
     */
    @Test
    void parsePolygonTest(){
        def outer = []
        outer << [0.0, 0.0, 0.0]
        outer << [10.0, 0.0]
        outer << [10.0, 10.0, 0.0]
        outer << [0.0, 10.0]
        outer << [0.0, 0.0, 0.0]

        def hole1 = []
        hole1 << [2.0, 2.0, 0.0]
        hole1 << [8.0, 2.0]
        hole1 << [8.0, 3.0]
        hole1 << [2.0, 2.0, 0.0]
        def hole2 = []
        hole2 << [2.0, 5.0, 0.0]
        hole2 << [8.0, 5.0]
        hole2 << [8.0, 7.0]
        hole2 << [2.0, 5.0, 0.0]

        def poly1 = []
        poly1 << outer

        def poly2 = []
        poly2 << outer
        poly2 << hole1
        poly2 << hole2

        assertEquals "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
                OSMTools.Utilities.parsePolygon(poly1, new GeometryFactory()).toString()

        assertEquals "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 3, 2 2), (2 5, 8 5, 8 7, 2 5))",
                OSMTools.Utilities.parsePolygon(poly2, new GeometryFactory()).toString()
    }

    /**
     * Test the {@link Utilities#parsePolygon(java.lang.Object, org.locationtech.jts.geom.GeometryFactory)}
     * method with bad data.
     */
    @Test
    void badParsePolygonTest(){
        def outer = []
        outer << [0.0, 0.0, 0.0]
        outer << [10.0, 0.0]
        outer << [10.0, 10.0, 0.0]
        outer << [0.0, 10.0]
        def poly1 = []
        poly1 << outer

        assertNull OSMTools.Utilities.parsePolygon(null, new GeometryFactory())
        assertNull OSMTools.Utilities.parsePolygon([], new GeometryFactory())
        assertNull OSMTools.Utilities.parsePolygon([[]], new GeometryFactory())
        assertNull OSMTools.Utilities.parsePolygon([[null]], new GeometryFactory())
        assertNull OSMTools.Utilities.parsePolygon(poly1, new GeometryFactory())
    }

    /**
     * Test the {@link Utilities#executeNominatimQuery(java.lang.Object, java.lang.Object)} method.
     * This test performs a web request to the Nominatim service.
     */
    @Test
    void getExecuteNominatimQueryTest(){
        def path = RANDOM_PATH()
        def file = new File(path)
        assertTrue OSMTools.Utilities.executeNominatimQuery("vannes", file)
        assertTrue file.exists()
        assertFalse file.text.isEmpty()
    }

    /**
     * Test the {@link Utilities#executeNominatimQuery(java.lang.Object, java.lang.Object)} method.
     */
    @Test
    void badGetExecuteNominatimQueryTest(){
        def file = new File(RANDOM_PATH())
        assertFalse OSMTools.Utilities.executeNominatimQuery(null, file)
        assertFalse OSMTools.Utilities.executeNominatimQuery("", file)
        assertFalse OSMTools.Utilities.executeNominatimQuery("query", file.getAbsolutePath())
        badExecuteNominatimQueryOverride()
        assertFalse OSMTools.Utilities.executeNominatimQuery("query", file)
    }

    /**
     * Test the {@link Utilities#toBBox(org.locationtech.jts.geom.Geometry)} method.
     */
    @Test
    void toBBoxTest(){
        def factory = new GeometryFactory()
        def point = factory.createPoint(new Coordinate(1.3, 7.7))
        Coordinate[] coordinates = [new Coordinate(2.0, 2.0),
                                    new Coordinate(4.0, 2.0),
                                    new Coordinate(4.0, 4.0),
                                    new Coordinate(2.0, 4.0),
                                    new Coordinate(2.0, 2.0)]
        def ring = factory.createLinearRing(coordinates)
        def polygon = factory.createPolygon(ring)

        assertEquals "(bbox:7.7,1.3,7.7,1.3)", OSMTools.Utilities.toBBox(point)
        assertEquals "(bbox:2.0,2.0,4.0,4.0)", OSMTools.Utilities.toBBox(ring)
        assertEquals "(bbox:2.0,2.0,4.0,4.0)", OSMTools.Utilities.toBBox(polygon)
    }

    /**
     * Test the {@link Utilities#toBBox(org.locationtech.jts.geom.Geometry)} method with bad data.
     */
    @Test
    void badToBBoxTest(){
        assertNull OSMTools.Utilities.toBBox(null)
    }

    /**
     * Test the {@link Utilities#toPoly(org.locationtech.jts.geom.Geometry)} method.
     */
    @Test
    void toPolyTest(){
        def factory = new GeometryFactory()
        Coordinate[] coordinates = [new Coordinate(2.0, 2.0),
                                    new Coordinate(4.0, 2.0),
                                    new Coordinate(4.0, 4.0),
                                    new Coordinate(2.0, 4.0),
                                    new Coordinate(2.0, 2.0)]
        def ring = factory.createLinearRing(coordinates)
        def poly = factory.createPolygon(ring)
        assertGStringEquals "(poly:\"2.0 2.0 2.0 4.0 4.0 4.0 4.0 2.0\")", OSMTools.Utilities.toPoly(poly)
        }

    /**
     * Test the {@link Utilities#toPoly(org.locationtech.jts.geom.Geometry)} method with bad data.
     */
    @Test
    void badToPolyTest(){
        def factory = new GeometryFactory()
        assertNull OSMTools.Utilities.toPoly(null)
        assertNull OSMTools.Utilities.toPoly(factory.createPoint(new Coordinate(0.0, 0.0)))
        assertNull OSMTools.Utilities.toPoly(factory.createPolygon())
    }

    /**
     * Test the {@link Utilities#buildOSMQuery(org.locationtech.jts.geom.Envelope, java.lang.Object, org.orbisgis.osm.utils.OSMElement[])}
     * method.
     */
    @Test
    void buildOSMQueryFromEnvelopeTest(){
        def enveloppe = new Envelope(0.0, 2.3, 7.6, 8.9)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                "\tnode[\"building\"];\n" +
                "\tnode[\"water\"];\n" +
                "\tway[\"building\"];\n" +
                "\tway[\"water\"];\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(enveloppe, ["building", "water"], OSMElement.NODE, OSMElement.WAY)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(enveloppe, ["building", "water"])
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(enveloppe, ["building", "water"], null)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(enveloppe, ["building", "water"], null)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                "\tnode;\n" +
                "\tway;\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(enveloppe, [], OSMElement.NODE, OSMElement.WAY)
    }

    /**
     * Test the {@link Utilities#buildOSMQuery(org.locationtech.jts.geom.Envelope, java.lang.Object, org.orbisgis.osm.utils.OSMElement[])}
     * method with bad data.
     */
    @Test
    void badBuildOSMQueryFromEnvelopeTest(){
        assertNull OSMTools.Utilities.buildOSMQuery((Envelope)null, ["building"], OSMElement.NODE)
    }

    /**
     * Test the {@link Utilities#buildOSMQuery(org.locationtech.jts.geom.Polygon, java.lang.Object, org.orbisgis.osm.utils.OSMElement[])}
     * method.
     */
    @Test
    void buildOSMQueryFromPolygonTest(){
        def factory = new GeometryFactory();
        Coordinate[] coordinates = [
                new Coordinate(0.0, 2.3),
                new Coordinate(7.6, 2.3),
                new Coordinate(7.6, 8.9),
                new Coordinate(0.0, 8.9),
                new Coordinate(0.0, 2.3)
        ]
        def ring = factory.createLinearRing(coordinates)
        def polygon = factory.createPolygon(ring)
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                "\tnode[\"building\"](poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                "\tnode[\"water\"](poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                "\tway[\"building\"](poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                "\tway[\"water\"](poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(polygon, ["building", "water"], OSMElement.NODE, OSMElement.WAY)
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(polygon, ["building", "water"])
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(polygon, ["building", "water"], null)
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(polygon, ["building", "water"], null)
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                "\tnode(poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                "\tway(poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                ");\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(polygon, [], OSMElement.NODE, OSMElement.WAY)
    }

    /**
     * Test the {@link Utilities#buildOSMQuery(org.locationtech.jts.geom.Polygon, java.lang.Object, org.orbisgis.osm.utils.OSMElement[])}
     * method with bad data.
     */
    @Test
    void badBuildOSMQueryFromPolygonTest(){
        assertNull OSMTools.Utilities.buildOSMQuery((Polygon)null, ["building"], OSMElement.NODE)
        assertNull OSMTools.Utilities.buildOSMQuery(new GeometryFactory().createPolygon(), ["building"], OSMElement.NODE)
    }

    /**
     * Test the {@link Utilities#readJSONParameters(java.lang.String)} method.
     */
    @Test
    void readJSONParametersTest(){
        def map = [
                "tags" : [
                        "highway", "cycleway", "biclycle_road", "cyclestreet", "route", "junction"
                ],
                "columns":["width","highway", "surface", "sidewalk",
                        "lane","layer","maxspeed","oneway",
                        "h_ref","route","cycleway",
                        "biclycle_road","cyclestreet","junction"
                ]
        ]
        assertEquals map, OSMTools.Utilities.readJSONParameters(new File(UtilitiesTest.getResource("road_tags.json").toURI()).absolutePath)
        assertEquals map, OSMTools.Utilities.readJSONParameters(UtilitiesTest.getResourceAsStream("road_tags.json"))
    }

    /**
     * Test the {@link Utilities#readJSONParameters(java.lang.Object)} method with bad data.
     */
    @Test
    void badReadJSONParametersTest(){
        assertNull OSMTools.Utilities.readJSONParameters(null)
        assertNull OSMTools.Utilities.readJSONParameters("")
        assertNull OSMTools.Utilities.readJSONParameters("toto")
        assertNull OSMTools.Utilities.readJSONParameters("target")
        assertNull OSMTools.Utilities.readJSONParameters(new File(UtilitiesTest.getResource("bad_json_params.json").toURI()).absolutePath)
        assertNull OSMTools.Utilities.readJSONParameters(UtilitiesTest.getResourceAsStream("bad_json_params.json"))
    }

    /**
     * Test the {@link Utilities#buildGeometryAndZone(org.locationtech.jts.geom.Geometry, int, java.lang.Object)} and
     * {@link Utilities#buildGeometryAndZone(org.locationtech.jts.geom.Geometry, int, int, java.lang.Object)} methods.
     */
    @Test
    //TODO is point valid ? or just polygon
    void buildGeometryAndZoneTest(){
        def ds = RANDOM_DS()
        def factory = new GeometryFactory()
        Coordinate[] coordinates = [
                new Coordinate(0, 0),
                new Coordinate(5, 0),
                new Coordinate(5, 8),
                new Coordinate(0, 8),
                new Coordinate(0, 0)
        ]
        def ring = factory.createLinearRing(coordinates)
        def polygon0 = factory.createPolygon(ring)
        def polygon_1 = factory.createPolygon(ring)
        def polygon2025 = factory.createPolygon(ring)
        polygon_1.setSRID(-1)
        polygon2025.setSRID(2025)

        def result = OSMTools.Utilities.buildGeometryAndZone(polygon0, 0, ds)
        assertEquals 2, result.size()
        assertTrue result.containsKey("geom")
        assertTrue result.containsKey("filterArea")
        assertEquals "POLYGON ((0 0, 5 0, 5 8, 0 8, 0 0))", result.geom.toString()
        assertTrue result.filterArea instanceof Polygon
        assertEquals 5, ((Polygon)result.filterArea).coordinates.length
        def filterArea0 = (Polygon)result.filterArea

        result = OSMTools.Utilities.buildGeometryAndZone(polygon_1, 0, ds)
        assertEquals 2, result.size()
        assertTrue result.containsKey("geom")
        assertTrue result.containsKey("filterArea")
        assertEquals "POLYGON ((0 0, 5 0, 5 8, 0 8, 0 0))", result.geom.toString()
        assertTrue result.filterArea instanceof Polygon
        assertEquals 5, ((Polygon)result.filterArea).coordinates.length
        def filterArea_1 = (Polygon)result.filterArea

        result = OSMTools.Utilities.buildGeometryAndZone(polygon0, 100, ds)
        assertEquals 2, result.size()
        assertTrue result.containsKey("geom")
        assertTrue result.containsKey("filterArea")
        assertEquals "POLYGON ((0 0, 5 0, 5 8, 0 8, 0 0))", result.geom.toString()
        assertTrue result.filterArea instanceof Polygon
        assertEquals 5, ((Polygon)result.filterArea).coordinates.length
        assertTrue filterArea0.length < ((Polygon)result.filterArea).length
        assertTrue filterArea0.area < ((Polygon)result.filterArea).area

        result = OSMTools.Utilities.buildGeometryAndZone(polygon_1, 100, ds)
        assertEquals 2, result.size()
        assertTrue result.containsKey("geom")
        assertTrue result.containsKey("filterArea")
        assertEquals "POLYGON ((0 0, 5 0, 5 8, 0 8, 0 0))", result.geom.toString()
        assertTrue result.filterArea instanceof Polygon
        assertEquals 5, ((Polygon)result.filterArea).coordinates.length
        assertTrue filterArea_1.length < ((Polygon)result.filterArea).length
        assertTrue filterArea_1.area < ((Polygon)result.filterArea).area

        result = OSMTools.Utilities.buildGeometryAndZone(polygon2025, 2026, 100, ds)
        assertEquals 2, result.size()
        assertTrue result.containsKey("geom")
        assertTrue result.containsKey("filterArea")
        assertTrue result.geom instanceof Polygon
        assertEquals 5, ((Polygon)result.geom).coordinates.length
        assertTrue result.filterArea instanceof Polygon
        assertEquals 5, ((Polygon)result.filterArea).coordinates.length

        result = OSMTools.Utilities.buildGeometryAndZone(polygon2025, 2026, 0, ds)
        assertEquals 2, result.size()
        assertTrue result.containsKey("geom")
        assertTrue result.containsKey("filterArea")
        assertTrue result.geom instanceof Polygon
        assertEquals 5, ((Polygon)result.geom).coordinates.length
        assertTrue result.filterArea instanceof Polygon
        assertEquals 5, ((Polygon)result.filterArea).coordinates.length
    }

    /**
     * Test the {@link Utilities#buildGeometryAndZone(org.locationtech.jts.geom.Geometry, int, java.lang.Object)} and
     * {@link Utilities#buildGeometryAndZone(org.locationtech.jts.geom.Geometry, int, int, java.lang.Object)} methods
     * with bad data.
     */
    @Test
    void badBuildGeometryAndZoneTest(){
        def ds = RANDOM_DS()
        def factory = new GeometryFactory()
        Coordinate[] coordinates = [
                new Coordinate(0, 0),
                new Coordinate(5, 0),
                new Coordinate(5, 8),
                new Coordinate(0, 8),
                new Coordinate(0, 0)
        ]
        def ring = factory.createLinearRing(coordinates)
        def polygon0 = factory.createPolygon(ring)
        assertNull OSMTools.Utilities.buildGeometryAndZone(null, 0, ds)
        assertNull OSMTools.Utilities.buildGeometryAndZone(polygon0, 0, null)
        assertNull OSMTools.Utilities.buildGeometryAndZone(null, 0, 0, ds)
        assertNull OSMTools.Utilities.buildGeometryAndZone(polygon0, 0, 0, null)
    }

    /**
     * Test the {@link Utilities#buildGeometry(java.lang.Object)} method.
     */
    @Test
    void buildGeometryTest(){
        assertEquals("POLYGON ((-3.29109 48.72223, -3.29109 48.83535, -2.80357 48.83535, -2.80357 48.72223, -3.29109 48.72223))",
                OSMTools.Utilities.buildGeometry([-3.29109,48.83535,-2.80357,48.72223]).toString())
    }

    /**
     * Test the {@link Utilities#buildGeometry(java.lang.Object)} method with bad data.
     */
    @Test
    void badBuildGeometryTest(){
        assertNull OSMTools.Utilities.buildGeometry([-3.29109,48.83535,-2.80357])
        assertNull OSMTools.Utilities.buildGeometry([-Float.MAX_VALUE,48.83535,-2.80357,48.72223])
        assertNull OSMTools.Utilities.buildGeometry([-3.29109,Float.MAX_VALUE,-2.80357,48.72223])
        assertNull OSMTools.Utilities.buildGeometry([-3.29109,48.83535,-Float.MAX_VALUE,48.72223])
        assertNull OSMTools.Utilities.buildGeometry([-3.29109,48.83535,-2.80357,Float.MAX_VALUE])
        assertNull OSMTools.Utilities.buildGeometry(null)
        assertNull OSMTools.Utilities.buildGeometry()
        assertNull OSMTools.Utilities.buildGeometry(new GeometryFactory())
    }

    /**
     * Test the {@link Utilities#geometryFromNominatim(java.lang.Object)} method.
     */
    @Test
    void geometryFromNominatimTest(){
        assertEquals("POLYGON ((-3.29109 48.72223, -3.29109 48.83535, -2.80357 48.83535, -2.80357 48.72223, -3.29109 48.72223))",
                OSMTools.Utilities.geometryFromNominatim([48.83535,-3.29109,48.72223,-2.80357]).toString())
    }

    /**
     * Test the {@link Utilities#geometryFromNominatim(java.lang.Object)} method with bad data.
     */
    @Test
    void badGeometryFromNominatimTest(){
        assertNull OSMTools.Utilities.geometryFromNominatim([-3.29109,48.83535,-2.80357])
        assertNull OSMTools.Utilities.geometryFromNominatim(null)
        assertNull OSMTools.Utilities.geometryFromNominatim()
        assertNull OSMTools.Utilities.geometryFromNominatim(new GeometryFactory())
    }

    /**
     * Test the {@link Utilities#geometryFromOverpass(java.lang.Object)} method.
     */
    @Test
    void geometryFromOverpassTest(){
        assertEquals("POLYGON ((-3.29109 48.72223, -3.29109 48.83535, -2.80357 48.83535, -2.80357 48.72223, -3.29109 48.72223))",
                OSMTools.Utilities.geometryFromOverpass([48.83535,-3.29109,48.72223,-2.80357]).toString())
    }

    /**
     * Test the {@link Utilities#geometryFromOverpass(java.lang.Object)} method with bad data.
     */
    @Test
    void badGeometryFromOverpassTest(){
        assertNull OSMTools.Utilities.geometryFromOverpass([-3.29109,48.83535,-2.80357])
        assertNull OSMTools.Utilities.geometryFromOverpass(null)
        assertNull OSMTools.Utilities.geometryFromOverpass()
        assertNull OSMTools.Utilities.geometryFromOverpass(new GeometryFactory())
    }

    /**
     * Test the {@link Utilities#dropOSMTables(java.lang.String, org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource)}
     * method.
     */
    @Test
    void dropOSMTablesTest(){
        def ds = RANDOM_DS()
        ds.execute "CREATE TABLE prefix_node"
        ds.execute "CREATE TABLE prefix_node_member"
        ds.execute "CREATE TABLE prefix_node_tag"
        ds.execute "CREATE TABLE prefix_relation"
        ds.execute "CREATE TABLE prefix_relation_member"
        ds.execute "CREATE TABLE prefix_relation_tag"
        ds.execute "CREATE TABLE prefix_way"
        ds.execute "CREATE TABLE prefix_way_member"
        ds.execute "CREATE TABLE prefix_way_node"
        ds.execute "CREATE TABLE prefix_way_tag"
        assertTrue OSMTools.Utilities.dropOSMTables("prefix", ds)

        ds.execute "CREATE TABLE _node"
        ds.execute "CREATE TABLE _node_member"
        ds.execute "CREATE TABLE _node_tag"
        ds.execute "CREATE TABLE _relation"
        ds.execute "CREATE TABLE _relation_member"
        ds.execute "CREATE TABLE _relation_tag"
        ds.execute "CREATE TABLE _way"
        ds.execute "CREATE TABLE _way_member"
        ds.execute "CREATE TABLE _way_node"
        ds.execute "CREATE TABLE _way_tag"
        assertTrue OSMTools.Utilities.dropOSMTables("", ds)

    }

    /**
     * Test the {@link Utilities#dropOSMTables(java.lang.String, org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource)}
     * method with bad data.
     */
    @Test
    void badDropOSMTablesTest(){
        def ds = RANDOM_DS()
        assertFalse OSMTools.Utilities.dropOSMTables("prefix", null)
        assertFalse OSMTools.Utilities.dropOSMTables(null, ds)
    }

    /**
     * Test the {@link Utilities#getAreaFromPlace(java.lang.Object)} method.
     */
    @Test
    void getAreaFromPlaceTest(){
        sampleExecuteNominatimPolygonQueryOverride()
        assertEquals "POLYGON ((0 0, 0 2, 2 2, 2 2, 2 0, 0 0))", OSMTools.Utilities.getAreaFromPlace("Place name").toString()
        assertEquals "Place name", query
        sampleExecuteNominatimMultipolygonQueryOverride()
        assertEquals "MULTIPOLYGON (((0 0, 0 2, 2 2, 2 2, 2 0, 0 0)), ((3 3, 3 4, 4 4, 4 3, 3 3)))", OSMTools.Utilities.getAreaFromPlace("Place name").toString()
        assertEquals "Place name", query
    }

    /**
     * Test the {@link Utilities#getAreaFromPlace(java.lang.Object)} method with bad data.
     */
    @Test
    void badGetAreaFromPlaceTest(){
        sampleExecuteNominatimPolygonQueryOverride()
        assertNull OSMTools.Utilities.getAreaFromPlace(null)
        badExecuteNominatimQueryOverride()
        assertNull OSMTools.Utilities.getAreaFromPlace("place")
    }
}
