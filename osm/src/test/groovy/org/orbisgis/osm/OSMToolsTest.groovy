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