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
 * Test class for the processes in {@link Loader}
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

    @Test
    void createWhereFilterTest(){
        def tags = new HashMap<>()
        tags["material"] = ["concrete"]
        assertGStringEquals "(tag_key = 'material' AND tag_value IN ('concrete'))",
                OSMTools.Transform.createWhereFilter(tags)

        tags = new HashMap<>()
        tags[null] = ["tata", "tutu"]
        assertGStringEquals "(tag_value IN ('tata','tutu'))",
                OSMTools.Transform.createWhereFilter(tags)

        tags = new HashMap<>()
        tags[null] = "toto"
        assertGStringEquals "(tag_value IN ('toto'))",
                OSMTools.Transform.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["null"] = null
        assertGStringEquals "(tag_key = 'null')",
                OSMTools.Transform.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["empty"] = []
        assertGStringEquals "(tag_key = 'empty')",
                OSMTools.Transform.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["road"] = [null, "highway"]
        assertGStringEquals "(tag_key = 'road' AND tag_value IN ('highway'))",
                OSMTools.Transform.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["water"] = "pound"
        assertGStringEquals "(tag_key = 'water' AND tag_value IN ('pound'))",
                OSMTools.Transform.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["pound"] = ["emilie", "big", "large"]
        assertGStringEquals "(tag_key = 'pound' AND tag_value IN ('emilie','big','large'))",
                OSMTools.Transform.createWhereFilter(tags)

        tags = new HashMap<>()
        tags[["river", "song"]] = ["big", "large"]
        assertGStringEquals "(tag_key IN ('river','song') AND tag_value IN ('big','large'))",
                OSMTools.Transform.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["material"] = ["concrete"]
        tags[null] = ["tata", "tutu"]
        tags[null] = "toto"
        tags["null"] = null
        tags["empty"] = []
        tags["road"] = [null, "highway"]
        tags["water"] = "pound"
        tags["pound"] = ["emilie", "big", "large"]
        tags[["river", "song"]] = ["big", "large"]
        assertGStringEquals "(tag_value IN ('toto')) OR " +
                "(tag_key = 'pound' AND tag_value IN ('emilie','big','large')) OR " +
                "(tag_key = 'material' AND tag_value IN ('concrete')) OR " +
                "(tag_key = 'null') OR " +
                "(tag_key = 'road' AND tag_value IN ('highway')) OR " +
                "(tag_key IN ('river','song') AND tag_value IN ('big','large')) OR " +
                "(tag_key = 'water' AND tag_value IN ('pound')) OR " +
                "(tag_key = 'empty')", OSMTools.Transform.createWhereFilter(tags)

        assertGStringEquals "tag_key IN ('emilie','big','large')", OSMTools.Transform.createWhereFilter(["emilie", "big", "large"])
    }

    @Test
    void badCreateWhereFilterTest(){
        assertGStringEquals "", OSMTools.Transform.createWhereFilter(null)
        assertGStringEquals "", OSMTools.Transform.createWhereFilter(new HashMap())
    }

    @Test
    void badGetColumnSelector(){
        def validTableName = "tutu"
        def validTags = [toto:"tata"]
        def columnsToKeep = ["col1", "col2", "col5"]
        assertEquals null, OSMTools.Transform.getColumnSelector(null, validTags, columnsToKeep)
        assertEquals null, OSMTools.Transform.getColumnSelector("", validTags, columnsToKeep)
    }

    @Test
    void getColumnSelector(){
        def validTableName = "tutu"
        def validTags = [toto:"tata"]
        def columnsToKeep = ["col1", "col2", "col5"]
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto','col1','col2','col5')",
                OSMTools.Transform.getColumnSelector(validTableName, validTags, columnsToKeep)
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto')",
                OSMTools.Transform.getColumnSelector(validTableName, validTags, null)
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto')",
                OSMTools.Transform.getColumnSelector(validTableName, validTags, [])
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto')",
                OSMTools.Transform.getColumnSelector(validTableName, validTags, [null, null])
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto','tutu')",
                OSMTools.Transform.getColumnSelector(validTableName, validTags, "tutu")
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto','tutu')",
                OSMTools.Transform.getColumnSelector(validTableName, validTags, "tutu")
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('col1','col2','col5')",
                OSMTools.Transform.getColumnSelector(validTableName, null, columnsToKeep)
        assertGStringEquals "SELECT distinct tag_key FROM tutu", OSMTools.Transform.getColumnSelector(validTableName, null, null)
    }

    //@Test
    void getCountTagQueryTest(){

    }


    /**
     * Test the {@link Transform#extractNodesAsPoints(org.orbisgis.datamanager.JdbcDataSource, java.lang.String, int, java.lang.String, java.lang.Object, java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badExtractNodesAsPoints(){
        H2GIS ds = RANDOM_DS()
        def prefix = "prefix"+uuid()
        def epsgCode  = 2456
        def outTable = "output"
        def tags = [building:["toto", "house", null], material:["concrete"], road:null]
        tags.put(null, null)
        tags.put(null, ["value1", "value2"])
        def columnsToKeep = []

        ds.execute "CREATE TABLE ${prefix}_node (id_node int, the_geom geometry)"
        ds.execute "INSERT INTO ${prefix}_node VALUES (1, 'POINT(0 0)')"
        ds.execute "INSERT INTO ${prefix}_node VALUES (2, 'POINT(1 1)')"
        ds.execute "INSERT INTO ${prefix}_node VALUES (3, 'POINT(2 2)')"
        ds.execute "INSERT INTO ${prefix}_node VALUES (4, 'POINT(56.23 78.23)')"
        ds.execute "INSERT INTO ${prefix}_node VALUES (5, 'POINT(-5.3 -45.23)')"

        ds.execute "CREATE TABLE ${prefix}_node_tag (id_node int, tag_key varchar, tag_value varchar)"
        ds.execute "INSERT INTO ${prefix}_node_tag VALUES (1, 'building', ('house', 'garage'))"
        ds.execute "INSERT INTO ${prefix}_node_tag VALUES (2, 'water', ('pound'))"
        ds.execute "INSERT INTO ${prefix}_node_tag VALUES (3, 'material', ('concrete'))"
        ds.execute "INSERT INTO ${prefix}_node_tag VALUES (4, 'building', ('house', 'garage'))"
        ds.execute "INSERT INTO ${prefix}_node_tag VALUES (5, 'material', ('concrete', 'brick'))"

        LOGGER.warn("Ann error will be thrown next")
        assertFalse OSMTools.Transform.extractNodesAsPoints(null, prefix, epsgCode, outTable, tags, columnsToKeep)
        LOGGER.warn("Ann error will be thrown next")
        assertFalse OSMTools.Transform.extractNodesAsPoints(ds, null, epsgCode, outTable, tags, columnsToKeep)
        LOGGER.warn("Ann error will be thrown next")
        assertFalse OSMTools.Transform.extractNodesAsPoints(ds, prefix, -1, outTable, tags, columnsToKeep)
        LOGGER.warn("Ann error will be thrown next")
        assertFalse OSMTools.Transform.extractNodesAsPoints(ds, prefix, epsgCode, null, tags, columnsToKeep)
        assertTrue OSMTools.Transform.extractNodesAsPoints(ds, prefix, epsgCode, outTable, null, columnsToKeep)
        //assertFalse OSMTools.Transform.extractNodesAsPoints(ds, prefix, epsgCode, outTable, tags, null)
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
