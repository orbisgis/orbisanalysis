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
        assertTrue process.execute(datasource: h2GIS, placeName: "Cliscouet, Vannes")
        assertTrue(h2GIS.hasTable(process.results.zoneTableName))
        ISpatialTable zoneTable = h2GIS.getSpatialTable(process.results.zoneTableName)
        assertTrue(zoneTable.rowCount==1)
        assertTrue(h2GIS.hasTable(process.results.buildingTableName))

    }

}
