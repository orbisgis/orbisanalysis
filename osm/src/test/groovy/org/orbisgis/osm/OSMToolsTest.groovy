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
 * Copyright (C) 2019 CNRS (Lab-STICC UMR CNRS 6285)
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
package org.orbisgis.osm


import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.*

class OSMToolsTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(OSMToolsTest)

    @BeforeEach
    final void beforeEach(TestInfo testInfo){
        LOGGER.info("@ ${testInfo.testMethod.get().name}()")
    }

    @AfterEach
    final void afterEach(TestInfo testInfo){
        LOGGER.info("# ${testInfo.testMethod.get().name}()")
    }

    /**
     * Test the {@link OSMTools#getLogger(java.lang.Object)} method.
     */
    @Test
    void getLoggerTest(){
        assertNotNull OSMTools.getLogger(null)
        assertEquals "org.orbisgis.osm.OSMTools", OSMTools.getLogger(null).getName()
        assertNotNull OSMTools.getLogger("null")
        assertEquals "org.orbisgis.osm.OSMTools", OSMTools.getLogger("null").getName()
        assertNotNull OSMTools.getLogger(OSMTools.Utilities)
        assertEquals "org.orbisgis.osm.Utilities", OSMTools.getLogger(OSMTools.Utilities).getName()
    }

    /**
     * Test the {@link OSMTools#getServerStatus()} method.
     */
    @Test
    void getServerStatusTest(){
        def osmTools = new DummyOSMTools()
        def status = osmTools.getServerStatus()
        assertNotNull status
    }

    /**
     * Test the {@link OSMTools#wait(int)} method.
     */
    @Test
    void waitTest(){
        def osmTools = new DummyOSMTools()
        assertTrue osmTools.wait(500)
    }

    /**
     * Test the {@link OSMTools#executeOverPassQuery(java.lang.Object, java.lang.Object)} method.
     */
    @Test
    void executeOverPassQueryTest(){
        def osmTools = new DummyOSMTools()
        def file = new File("target/${OSMTools.uuid}")
        assertTrue file.createNewFile()
        assertTrue osmTools.executeOverPassQuery("(node(51.249,7.148,51.251,7.152);<;);out meta;", file)
        assertTrue file.exists()
        assertFalse file.text.isEmpty()
    }

    /**
     * Test the {@link OSMTools#executeOverPassQuery(java.lang.Object, java.lang.Object)} method with bad data.
     */
    @Test
    void badExecuteOverPassQueryTest(){
        def osmTools = new DummyOSMTools()
        def file = new File("target/${OSMTools.uuid}")
        assertTrue file.createNewFile()
        assertFalse osmTools.executeOverPassQuery(null, file)
        assertTrue file.text.isEmpty()
        assertFalse osmTools.executeOverPassQuery("query", null)
        assertTrue file.text.isEmpty()
    }

    /**
     * Dummy implementation of the {@link OSMTools} class.
     */
    private class DummyOSMTools extends OSMTools{
        @Override Object run() { return null }
    }
}