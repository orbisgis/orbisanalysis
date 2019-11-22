package org.orbisgis.osm


import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.datamanagerapi.dataset.ISpatialTable
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.*

class OSMNoiseTests {

    private static final Logger logger = LoggerFactory.getLogger(OSMNoiseTests.class)

    @Test
    void downloadTest() {
        def h2GIS = H2GIS.open('./target/OSMNoise;AUTO_SERVER=TRUE')
        def process = OSMNoise.Data.download()
        assertTrue process.execute(datasource: h2GIS, placeName: "Saint Jean La Poterie")
        assertTrue(h2GIS.hasTable(process.results.zoneTableName))
        ISpatialTable zoneTable = h2GIS.getSpatialTable(process.results.zoneTableName)
        assertTrue(zoneTable.rowCount==1)
        assertTrue(new File(process.results.osmFilePath).exists())
    }

    @Test
    void GISLayersTestFromApi() {
        def h2GIS = H2GIS.open('./target/OSMNoise;AUTO_SERVER=TRUE')
        def process = OSMNoise.Data.GISLayers()
        assertTrue process.execute(datasource: h2GIS, placeName: "Saint Jean La Poterie")
        assertTrue(h2GIS.hasTable(process.results.zoneTableName))
        ISpatialTable zoneTable = h2GIS.getSpatialTable(process.results.zoneTableName)
        assertTrue(zoneTable.rowCount==1)
        assertTrue(h2GIS.hasTable(process.results.buildingTableName))
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.outputTableName} where NB_LEV is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.outputTableName} where HEIGHT_WALL is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.outputTableName} where HEIGHT_ROOF is null").count==0

        assertTrue(h2GIS.hasTable(process.results.roadTableName))
        ouputTable = h2GIS.getSpatialTable(process.results.roadTableName)
        assertTrue(ouputTable.rowCount>1)
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.roadTableName} where WGAEN_TYPE is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.roadTableName} where ONEWAY is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.roadTableName} where MAXSPEED is null").count==0

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
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.outputTableName} where NB_LEV is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.outputTableName} where HEIGHT_WALL is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.outputTableName} where HEIGHT_ROOF is null").count==0

        process = OSMNoise.Data.createRoadLayer()
        assertTrue process.execute(datasource: h2GIS, osmTablesPrefix: prefix,
                epsg: 2154, outputTablePrefix : "redon")
        assertTrue(h2GIS.hasTable(process.results.outputTableName))
        ouputTable = h2GIS.getSpatialTable(process.results.outputTableName)
        assertTrue(ouputTable.rowCount>1)
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.outputTableName} where WGAEN_TYPE is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.outputTableName} where ONEWAY is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.outputTableName} where MAXSPEED is null").count==0

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
    void buildTrafficWGAENDataFromTestFile() {
        def h2GIS = H2GIS.open('./target/OSMNoise;AUTO_SERVER=TRUE')
        def prefix = "OSM_FILE"
        def load = OSMTools.Loader.load()
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())
        def process = OSMNoise.Data.createRoadLayer()
        assertTrue process.execute(datasource: h2GIS, osmTablesPrefix: prefix,
                epsg: 2154, outputTablePrefix : "redon")
        assertTrue(h2GIS.hasTable(process.results.outputTableName))
        def ouputTable = h2GIS.getSpatialTable(process.results.outputTableName)
        assertTrue(ouputTable.rowCount>1)
        process = OSMNoise.Traffic.WGAEN_ROAD()
        process.execute(datasource: h2GIS,roadTableName:ouputTable.getName(), outputTablePrefix:"redon")
        ouputTable = h2GIS.getSpatialTable(process.results.outputTableName)
        assertTrue(ouputTable.rowCount>1)
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.outputTableName} where WGAEN_TYPE is null").count==0
        def columnsToCheck = ["day_lv_hour", "day_hv_hour","day_lv_speed", "day_hv_speed", "night_lv_hour", "night_hv_hour",
        "night_lv_speed", "night_hv_speed", "ev_lv_hour", "ev_hv_hour", "ev_lv_speed", "ev_hv_speed"]
        columnsToCheck.each {it ->
            assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.outputTableName} where ${it} is null and ${it}<=0").count==0
        }
    }

}
