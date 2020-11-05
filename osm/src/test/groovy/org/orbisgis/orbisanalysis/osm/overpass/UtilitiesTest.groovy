/*
 * Bundle OSM is part of the OrbisGIS platform
 *
 * OrbisGIS is a java GIS application dedicated to research in GIScience.
 * OrbisGIS is developed by the GIS group of the DECIDE team of the
 * Lab-STICC CNRS laboratory, see <http://www.lab-sticc.fr/>.
 *
 * The GIS group of the DECIDE team is located at :
 *
 * Laboratoire Lab-STICC – CNRS UMR 6285
 * Equipe DECIDE
 * UNIVERSITÉ DE BRETAGNE-SUD
 * Institut Universitaire de Technologie de Vannes
 * 8, Rue Montaigne - BP 561 56017 Vannes Cedex
 *
 * OSM is distributed under LGPL 3 license.
 *
 * Copyright (C) 2019-2020 CNRS (Lab-STICC UMR CNRS 6285)
 *
 *
 * OSM is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * OSM is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * OSM. If not, see <http://www.gnu.org/licenses/>.
 *
 * For more information, please consult: <http://www.orbisgis.org/>
 * or contact directly:
 * info_at_ orbisgis.org
 */
package org.orbisgis.orbisanalysis.osm.overpass

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Polygon
import org.orbisgis.orbisanalysis.osm.AbstractOSMTest
import org.orbisgis.orbisanalysis.osm.utils.NominatimUtils
import org.orbisgis.orbisanalysis.osm.utils.OverpassUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.regex.Pattern

import static org.junit.jupiter.api.Assertions.*

/**
 * Test class for {@link org.orbisgis.orbisanalysis.osm.utils.Utilities}
 *
 * @author Erwan Bocher (CNRS)
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019-2020)
 */
class UtilitiesTest extends AbstractOSMTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(UtilitiesTest)

    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def executeOverPassQuery
    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def getAreaFromPlace
    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def executeNominatimQuery

    @BeforeEach
    final void beforeEach(TestInfo testInfo){
        LOGGER.info("@ ${testInfo.testMethod.get().name}()")
    }

    @AfterEach
    final void afterEach(TestInfo testInfo){
        LOGGER.info("# ${testInfo.testMethod.get().name}()")
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#executeNominatimQuery(java.lang.Object, java.lang.Object)} method.
     * This test performs a web request to the Nominatim service.
     */
    @Test
    @Disabled
    void getExecuteNominatimQueryTest(){
        def path = RANDOM_PATH()
        def file = new File(path)
        assertTrue NominatimUtils.executeNominatimQuery("vannes", file)
        assertTrue file.exists()
        assertFalse file.text.isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#executeNominatimQuery(java.lang.Object, java.lang.Object)} method.
     */
    @Test
    @Disabled
    void badGetExecuteNominatimQueryTest(){
        def file = new File(RANDOM_PATH())
        assertFalse NominatimUtils.executeNominatimQuery(null, file)
        assertFalse NominatimUtils.executeNominatimQuery("", file)
        assertFalse NominatimUtils.executeNominatimQuery("query", file.getAbsolutePath())
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#toBBox(org.locationtech.jts.geom.Geometry)} method.
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

        assertEquals "(bbox:7.7,1.3,7.7,1.3)", OverpassUtils.toBBox(point)
        assertEquals "(bbox:2.0,2.0,4.0,4.0)", OverpassUtils.toBBox(ring)
        assertEquals "(bbox:2.0,2.0,4.0,4.0)", OverpassUtils.toBBox(polygon)
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#toBBox(org.locationtech.jts.geom.Geometry)} method with bad data.
     */
    @Test
    void badToBBoxTest(){
        assertNull OverpassUtils.toBBox(null)
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#toOSMPoly(org.locationtech.jts.geom.Geometry)} method.
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
        assertGStringEquals "(poly:\"2.0 2.0 2.0 4.0 4.0 4.0 4.0 2.0\")", OverpassUtils.toOSMPoly(poly)
        }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#toOSMPoly(org.locationtech.jts.geom.Geometry)} method with bad data.
     */
    @Test
    void badToPolyTest(){
        def factory = new GeometryFactory()
        assertNull OverpassUtils.toOSMPoly(null)
        assertNull OverpassUtils.toOSMPoly(factory.createPoint(new Coordinate(0.0, 0.0)))
        assertNull OverpassUtils.toOSMPoly(factory.createPolygon())
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#buildOSMQuery(org.locationtech.jts.geom.Envelope, java.lang.Object, org.orbisgis.orbisanalysis.osm.overpass.OSMElement[])}
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
                "out;", OverpassUtils.buildOSMQuery(enveloppe as Polygon, ["building", "water"], OSMElement.NODE, OSMElement.WAY)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OverpassUtils.buildOSMQuery(enveloppe as Polygon, ["building", "water"])
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OverpassUtils.buildOSMQuery(enveloppe as Polygon, ["building", "water"], null)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OverpassUtils.buildOSMQuery(enveloppe as Polygon, ["building", "water"], null)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                "\tnode;\n" +
                "\tway;\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OverpassUtils.buildOSMQuery(enveloppe as Polygon, [], OSMElement.NODE, OSMElement.WAY)
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#buildOSMQuery(org.locationtech.jts.geom.Envelope, java.lang.Object, org.orbisgis.orbisanalysis.osm.overpass.OSMElement[])}
     * method with bad data.
     */
    @Test
    void badBuildOSMQueryFromEnvelopeTest(){
        assertNull OverpassUtils.buildOSMQuery((Geometry)null, ["building"], OSMElement.NODE)
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#buildOSMQuery(org.locationtech.jts.geom.Polygon, java.lang.Object, org.orbisgis.orbisanalysis.osm.overpass.OSMElement[])}
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
                "out;", OverpassUtils.buildOSMQuery(polygon, ["building", "water"], OSMElement.NODE, OSMElement.WAY)
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OverpassUtils.buildOSMQuery(polygon, ["building", "water"])
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OverpassUtils.buildOSMQuery(polygon, ["building", "water"], null)
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OverpassUtils.buildOSMQuery(polygon, ["building", "water"], null)
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                "\tnode(poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                "\tway(poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                ");\n" +
                "out;", OverpassUtils.buildOSMQuery(polygon, [], OSMElement.NODE, OSMElement.WAY)
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#buildOSMQuery(org.locationtech.jts.geom.Polygon, java.lang.Object, org.orbisgis.orbisanalysis.osm.overpass.OSMElement[])}
     * method with bad data.
     */
    @Test
    void badBuildOSMQueryFromPolygonTest(){
        assertNull OverpassUtils.buildOSMQuery((Polygon)null, ["building"], OSMElement.NODE)
        assertNull OverpassUtils.buildOSMQuery(new GeometryFactory().createPolygon(), ["building"], OSMElement.NODE)
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#geometryFromNominatim(java.lang.Object)} method.
     */
    @Test
    void geometryFromNominatimTest(){
        assertEquals("POLYGON ((-3.29109 48.72223, -3.29109 48.83535, -2.80357 48.83535, -2.80357 48.72223, -3.29109 48.72223))",
                NominatimUtils.geometryFromNominatim([48.83535, -3.29109, 48.72223, -2.80357]).toString())
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#geometryFromNominatim(java.lang.Object)} method with bad data.
     */
    @Test
    void badGeometryFromNominatimTest(){
        assertNull NominatimUtils.geometryFromNominatim([-3.29109, 48.83535, -2.80357])
        assertNull NominatimUtils.geometryFromNominatim(null)
        assertNull NominatimUtils.geometryFromNominatim()
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#geometryFromOverpass(java.lang.Object)} method.
     */
    @Test
    void geometryFromOverpassTest(){
        assertEquals("POLYGON ((-3.29109 48.72223, -3.29109 48.83535, -2.80357 48.83535, -2.80357 48.72223, -3.29109 48.72223))",
                OverpassUtils.geometryFromOverpass([48.83535, -3.29109, 48.72223, -2.80357]).toString())
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#geometryFromOverpass(java.lang.Object)} method with bad data.
     */
    @Test
    void badGeometryFromOverpassTest(){
        assertNull OverpassUtils.geometryFromOverpass([-3.29109, 48.83535, -2.80357])
        assertNull OverpassUtils.geometryFromOverpass(null)
        assertNull OverpassUtils.geometryFromOverpass()
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#getArea(java.lang.Object)} method.
     */
    @Test
    @Disabled
    void getAreaFromPlaceTest(){
        def pattern = Pattern.compile("^POLYGON \\(\\((?>-?\\d+(?>\\.\\d+)? -?\\d+(?>\\.\\d+)?(?>, )?)*\\)\\)\$")
        assertTrue pattern.matcher(NominatimUtils.getArea("Paimpol").toString()).matches()
        assertTrue pattern.matcher(NominatimUtils.getArea("Boston").toString()).matches()
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.Utilities#getArea(java.lang.Object)} method with bad data.
     */
    @Test
    @Disabled
    void badGetAreaFromPlaceTest() {
        assertNull NominatimUtils.getArea(null)
    }

    /**
     * Test the {@link Utilities#getServerStatus()} method.
     */
    @Test
    void getServerStatusTest(){
        def status = OverpassUtils.getServerStatus()
        assertNotNull status
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.OSMTools#wait(int)} method.
     */
    @Test
    void waitTest(){
        assertTrue OverpassUtils.wait(500)
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.OSMTools#executeOverPassQuery(java.lang.Object, java.lang.Object)} method.
     */
    @Test
    @Disabled
    void executeOverPassQueryTest(){
        def file = new File("target/" + UUID.randomUUID().toString().replaceAll("-", "_"))
        assertTrue file.createNewFile()
        assertTrue OverpassUtils.executeAsOverPassQuery("(node(51.249,7.148,51.251,7.152);<;);out meta;", file)
        assertTrue file.exists()
        assertFalse file.text.isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.OSMTools#executeOverPassQuery(java.lang.Object, java.lang.Object)} method with bad data.
     */
    @Test
    @Disabled
    void badExecuteOverPassQueryTest(){
        def file = new File("target/" + UUID.randomUUID().toString().replaceAll("-", "_"))
        assertTrue file.createNewFile()
        assertFalse OverpassUtils.executeAsOverPassQuery(null, file)
        assertTrue file.text.isEmpty()
        assertFalse OverpassUtils.executeAsOverPassQuery("query", null)
        assertTrue file.text.isEmpty()
    }
}
