package org.orbisgis.osm

import org.h2gis.functions.spatial.crs.ST_Transform
import org.junit.jupiter.api.Test
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.WKTReader
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.datamanagerapi.dataset.ISpatialTable
import org.orbisgis.processmanagerapi.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.*

class OSMNoiseTests {

    private static final Logger logger = LoggerFactory.getLogger(OSMNoiseTests.class)

    @Test
    void downloadTest() {
        def h2GIS = H2GIS.open('./target/OSMNoise;AUTO_SERVER=TRUE')
        def process = OSMNoise.Data.download()
        assertTrue process.execute(datasource: h2GIS, placeName: "Bouguenais")
        assertTrue(h2GIS.hasTable(process.results.zoneTableName))
        ISpatialTable zoneTable = h2GIS.getSpatialTable(process.results.zoneTableName)
        assertTrue(zoneTable.rowCount==1)
        assertTrue(new File(process.results.osmFilePath).exists())
    }

    @Test
    void GISLayersTest() {
        def h2GIS = H2GIS.open('./target/OSMNoise;AUTO_SERVER=TRUE')
        def process = OSMNoise.Data.GISLayers()
        assertTrue process.execute(datasource: h2GIS, placeName: "Saint Jean La Poterie")
        assertTrue(h2GIS.hasTable(process.results.zoneTableName))
        ISpatialTable zoneTable = h2GIS.getSpatialTable(process.results.zoneTableName)
        assertTrue(zoneTable.rowCount==1)
        assertTrue(h2GIS.hasTable(process.results.buildingTableName))

    }

    @Test
    void GISLayersFromOSMFileTest() {
        def h2GIS = H2GIS.open('./target/OSMNoise;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())

        def process = OSMNoise.Data.createBuildingLayer()
        assertTrue process.execute(datasource: h2GIS, osmTablesPrefix: prefix,
                epsg: 2154, outputTablePrefix : "redon")
        assertTrue(h2GIS.hasTable(process.results.outputTableName))
        ISpatialTable ouputTable = h2GIS.getSpatialTable(process.results.outputTableName)
        assertTrue(ouputTable.rowCount>1)

        process = OSMNoise.Data.createRoadLayer()
        assertTrue process.execute(datasource: h2GIS, osmTablesPrefix: prefix,
                epsg: 2154, outputTablePrefix : "redon")
        assertTrue(h2GIS.hasTable(process.results.outputTableName))
        ouputTable = h2GIS.getSpatialTable(process.results.outputTableName)
        assertTrue(ouputTable.rowCount>1)
        ouputTable.save("/tmp/routes.shp")
    }

    @Test
    void test(){
        assertEquals (-1,OSMNoise.Data.getSpeedInKmh(null))
        assertEquals (-1,OSMNoise.Data.getSpeedInKmh(""))
        assertEquals 72,OSMNoise.Data.getSpeedInKmh("72")
        assertEquals 115.848,OSMNoise.Data.getSpeedInKmh("72 MPH")
        assertEquals 115.848,OSMNoise.Data.getSpeedInKmh("72 mph")
        assertEquals 115.848,OSMNoise.Data.getSpeedInKmh("72 MpH")
        assertEquals 72,OSMNoise.Data.getSpeedInKmh("72 KMH")
        assertEquals 72,OSMNoise.Data.getSpeedInKmh("72 KmH")
        assertEquals 72,OSMNoise.Data.getSpeedInKmh("72 kmh")
        assertEquals 72, OSMNoise.Data.getSpeedInKmh("72 kmh")
        assertEquals (-1, OSMNoise.Data.getSpeedInKmh("72 knots"))
        assertEquals (-1, OSMNoise.Data.getSpeedInKmh("25kmh"))
        assertEquals (-1, OSMNoise.Data.getSpeedInKmh("vbghfgh"))
    }

    @Test
    void buildTrafficWGAENData() {
        def h2GIS = H2GIS.open('./target/OSMNoise;AUTO_SERVER=TRUE')

        def process = OSMNoise.Traffic.WGAEN
        process.execute(datasource: h2GIS,roadTableName:null)
    }

}
