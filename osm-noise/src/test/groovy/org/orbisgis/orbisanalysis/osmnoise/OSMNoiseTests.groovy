/*
 * Bundle OSM Noise is part of the OrbisGIS platform
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
 * OSM Noise is distributed under LGPL 3 license.
 *
 * Copyright (C) 2019 CNRS (Lab-STICC UMR CNRS 6285)
 *
 *
 * OSM Noise is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * OSM Noise is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * OSM Noise. If not, see <http://www.gnu.org/licenses/>.
 *
 * For more information, please consult: <http://www.orbisgis.org/>
 * or contact directly:
 * info_at_ orbisgis.org
 */
package org.orbisgis.orbisanalysis.osmnoise

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.orbisgis.orbisanalysis.osm.OSMTools
import org.orbisgis.orbisanalysis.osmnoise.OSMNoise
import org.orbisgis.orbisdata.datamanager.api.dataset.ISpatialTable
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessManager
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class OSMNoiseTests {

    private static final Logger logger = LoggerFactory.getLogger(OSMNoiseTests.class)

    @Disabled
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

    @Disabled
    @Test
    void GISLayersTestFromApi() {
        def placeName = "Lorient"
        def h2GIS = H2GIS.open('./target/OSMNoise;AUTO_SERVER=TRUE')
        def process = OSMNoise.Data.GISLayers()
        assertTrue process.execute(datasource: h2GIS, placeName: placeName)
        assertTrue(h2GIS.hasTable(process.results.zoneTableName))
        ISpatialTable zoneTable = h2GIS.getSpatialTable(process.results.zoneTableName)
        assertTrue(zoneTable.rowCount==1)
        assertTrue(h2GIS.hasTable(process.results.buildingTableName))
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.buildingTableName} where NB_LEV is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.buildingTableName} where HEIGHT_WALL is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.buildingTableName} where HEIGHT_ROOF is null").count==0

        def roadTableName = process.results.roadTableName
        assertTrue(h2GIS.hasTable(roadTableName))
        def ouputTable = h2GIS.getSpatialTable(roadTableName)
        assertTrue(ouputTable.rowCount>1)
        assertTrue h2GIS.firstRow("select count(*) as count from ${roadTableName} where WGAEN_TYPE is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${roadTableName} where ONEWAY is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${roadTableName} where MAXSPEED is null").count==0

        process = OSMNoise.Traffic.WGAEN_ROAD()
        process.execute(datasource: h2GIS,roadTableName: roadTableName)
        h2GIS.save(process.results.outputTableName, "./target/${placeName}_traffic.shp", true)

    }

    @Test
    void GISLayersFromOSMFileTest() {
        def h2GIS = H2GIS.open('./target/OSMNoise;AUTO_SERVER=TRUE')
        def load = OSMTools.Loader.load()
        def prefix = "OSM_FILE"
        assertTrue load.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())

        def process = OSMNoise.Data.createBuildingLayer()
        assertTrue process.execute(datasource: h2GIS, osmTablesPrefix: prefix,epsg: 2154, outputTablePrefix : "redon")
        assertTrue(h2GIS.hasTable(process.results.outputTableName))
        ISpatialTable ouputTable = h2GIS.getSpatialTable(process.results.outputTableName)
        assertTrue(ouputTable.rowCount>1)
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.outputTableName} where NB_LEV is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.outputTableName} where HEIGHT_WALL is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${process.results.outputTableName} where HEIGHT_ROOF is null").count==0

        process = OSMNoise.Data.createRoadLayer()
        assertTrue process.execute(datasource: h2GIS, osmTablesPrefix: prefix, epsg: 2154,outputTablePrefix : "redon")
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
