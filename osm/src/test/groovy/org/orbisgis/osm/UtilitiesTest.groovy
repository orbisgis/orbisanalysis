package org.orbisgis.osm

import org.junit.jupiter.api.Test
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.osm.Utilities

import static org.junit.jupiter.api.Assertions.assertArrayEquals
import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertNull
import static org.junit.jupiter.api.Assertions.assertTrue

/**
 * Test class for {@link Utilities}
 *
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */
class UtilitiesTest extends AbstractOSMTest{

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

    @Test
    void buildBoundingBox(){
        assertEquals("POLYGON ((-0.489 51.28, -0.489 51.686, 0.236 51.686, 0.236 51.28, -0.489 51.28))",
                OSMTools.Utilities.buildGeometry([-0.489,51.28,0.236,51.686]).toString())
        assertNull  OSMTools.Utilities.buildGeometry([20000, 2510, 10, 10])
        assertNull  OSMTools.Utilities.buildGeometry([])
        assertNull  OSMTools.Utilities.buildGeometry()
        assertNull  OSMTools.Utilities.buildGeometry([-0.489,51.28,0.236])
    }

    @Test
    void extractGeometryZone(){
        H2GIS ds = RANDOM_DS()
        Geometry geom =  OSMTools.Utilities.buildGeometry([-0.489, 51.28, 0.236, 51.686])
        assertTrue(geom.equals(OSMTools.Utilities.buildGeometryAndZone(geom,  0, ds).geom))
    }
}
