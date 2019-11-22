package org.orbisgis.osm

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.orbisgis.datamanager.h2gis.H2GIS
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertTrue

/**
 * Test class for the processes in {@link Transform}
 *
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */
class TransformTest extends AbstractOSMTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformTest)

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
     * Test the OSMTools.Transform.toPoints() process with bad data.
     */
    @Test
    void badToPointsTest(){
        def toPoints = OSMTools.Transform.toPoints()
        H2GIS ds = RANDOM_DS()
        def prefix = uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        LOGGER.warn("Ann error will be thrown next")
        assertFalse toPoints(datasource: null, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue toPoints.results.isEmpty()

        LOGGER.warn("Ann error will be thrown next")
        assertFalse toPoints(datasource: ds, osmTablesPrefix: prefix, epsgCode:-1, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue toPoints.results.isEmpty()

        LOGGER.warn("Ann error will be thrown next")
        assertFalse toPoints(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue toPoints.results.isEmpty()
    }

    /**
     * Test the OSMTools.Transform.toPoints() process.
     */
    @Test
    void toPointsTest(){
        def toPoints = OSMTools.Transform.toPoints()
        H2GIS ds = RANDOM_DS()
        def prefix = uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        //assertTrue toPoints(datasource: null, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        //assertFalse toPoints.results.isEmpty()
    }
}
