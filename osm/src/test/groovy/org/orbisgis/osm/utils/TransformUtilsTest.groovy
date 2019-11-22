package org.orbisgis.osm.utils

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.osm.AbstractOSMTest
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertNull
import static org.junit.jupiter.api.Assertions.assertTrue

/**
 * Test class for the processes in {@link org.orbisgis.osm.Transform}
 *
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */
class TransformUtilsTest extends AbstractOSMTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformUtilsTest)

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
     * Test the {@link org.orbisgis.osm.utils.TransformUtils#createWhereFilter(java.lang.Object)}
     * method.
     */
    @Test
    void createWhereFilterTest(){
        def tags = new HashMap<>()
        tags["material"] = ["concrete"]
        assertGStringEquals "(tag_key = 'material' AND tag_value IN ('concrete'))",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags[null] = ["tata", "tutu"]
        assertGStringEquals "(tag_value IN ('tata','tutu'))",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags[null] = "toto"
        assertGStringEquals "(tag_value IN ('toto'))",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["null"] = null
        assertGStringEquals "(tag_key = 'null')",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["empty"] = []
        assertGStringEquals "(tag_key = 'empty')",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["road"] = [null, "highway"]
        assertGStringEquals "(tag_key = 'road' AND tag_value IN ('highway'))",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["water"] = "pound"
        assertGStringEquals "(tag_key = 'water' AND tag_value IN ('pound'))",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["pound"] = ["emilie", "big", "large"]
        assertGStringEquals "(tag_key = 'pound' AND tag_value IN ('emilie','big','large'))",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags[["river", "song"]] = ["big", "large"]
        assertGStringEquals "(tag_key IN ('river','song') AND tag_value IN ('big','large'))",
                TransformUtils.createWhereFilter(tags)

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
                "(tag_key = 'empty')", TransformUtils.createWhereFilter(tags)

        assertGStringEquals "tag_key IN ('emilie','big','large')", TransformUtils.createWhereFilter(["emilie", "big", "large", null])
    }

    /**
     * Test the {@link org.orbisgis.osm.utils.TransformUtils#createWhereFilter(java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badCreateWhereFilterTest(){
        assertGStringEquals "", TransformUtils.createWhereFilter(null)
        assertGStringEquals "", TransformUtils.createWhereFilter(new HashMap())
        assertGStringEquals "", TransformUtils.createWhereFilter([])
        assertGStringEquals "", TransformUtils.createWhereFilter([null])
    }

    /**
     * Test the {@link org.orbisgis.osm.utils.TransformUtils#getColumnSelector(java.lang.Object, java.lang.Object, java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badGetColumnSelectorTest(){
        def validTableName = "tutu"
        def validTags = [toto:"tata"]
        def columnsToKeep = ["col1", "col2", "col5"]
        assertNull TransformUtils.getColumnSelector(null, validTags, columnsToKeep)
        assertNull TransformUtils.getColumnSelector("", validTags, columnsToKeep)
    }

    /**
     * Test the {@link org.orbisgis.osm.utils.TransformUtils#getColumnSelector(java.lang.Object, java.lang.Object, java.lang.Object)}
     * method.
     */
    @Test
    void getColumnSelectorTest(){
        def validTableName = "tutu"
        def validTags = [toto:"tata"]
        def columnsToKeep = ["col1", "col2", "col5"]
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto','col1','col2','col5')",
                TransformUtils.getColumnSelector(validTableName, validTags, columnsToKeep)
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto')",
                TransformUtils.getColumnSelector(validTableName, validTags, null)
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto')",
                TransformUtils.getColumnSelector(validTableName, validTags, [])
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto')",
                TransformUtils.getColumnSelector(validTableName, validTags, [null, null])
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto','tutu')",
                TransformUtils.getColumnSelector(validTableName, validTags, "tutu")
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto','tutu')",
                TransformUtils.getColumnSelector(validTableName, validTags, "tutu")
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('col1','col2','col5')",
                TransformUtils.getColumnSelector(validTableName, null, columnsToKeep)
        assertGStringEquals "SELECT distinct tag_key FROM tutu", TransformUtils.getColumnSelector(validTableName, null, null)
    }

    /**
     * Test the {@link org.orbisgis.osm.utils.TransformUtils#getCountTagsQuery(java.lang.Object, java.lang.Object)}
     * method.
     */
    @Test
    void getCountTagQueryTest(){
        def osmTable = "tutu"
        assertGStringEquals "SELECT count(*) AS count FROM tutu WHERE tag_key IN ('titi','tata')",
                TransformUtils.getCountTagsQuery(osmTable, ["titi", "tata"])
        assertGStringEquals "SELECT count(*) AS count FROM tutu",
                TransformUtils.getCountTagsQuery(osmTable, null)
        assertGStringEquals "SELECT count(*) AS count FROM tutu",
                TransformUtils.getCountTagsQuery(osmTable, [])
        assertGStringEquals "SELECT count(*) AS count FROM tutu",
                TransformUtils.getCountTagsQuery(osmTable, [null])
        assertGStringEquals "SELECT count(*) AS count FROM tutu WHERE tag_key IN ('toto')",
                TransformUtils.getCountTagsQuery(osmTable, "toto")
    }

    /**
     * Test the {@link org.orbisgis.osm.utils.TransformUtils#getCountTagsQuery(java.lang.Object, java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badGetCountTagQueryTest(){
        assertNull TransformUtils.getCountTagsQuery(null, ["titi", "tata"])
        assertNull TransformUtils.getCountTagsQuery("", ["titi", "tata"])
    }


    /**
     * Test the {@link org.orbisgis.osm.utils.TransformUtils#extractNodesAsPoints(org.orbisgis.datamanager.JdbcDataSource, java.lang.String, int, java.lang.String, java.lang.Object, java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badExtractNodesAsPointsTest(){
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
        assertFalse TransformUtils.extractNodesAsPoints(null, prefix, epsgCode, outTable, tags, columnsToKeep)
        LOGGER.warn("Ann error will be thrown next")
        assertFalse TransformUtils.extractNodesAsPoints(ds, null, epsgCode, outTable, tags, columnsToKeep)
        LOGGER.warn("Ann error will be thrown next")
        assertFalse TransformUtils.extractNodesAsPoints(ds, prefix, -1, outTable, tags, columnsToKeep)
        LOGGER.warn("Ann error will be thrown next")
        assertFalse TransformUtils.extractNodesAsPoints(ds, prefix, epsgCode, null, tags, columnsToKeep)
        assertTrue TransformUtils.extractNodesAsPoints(ds, prefix, epsgCode, outTable, null, columnsToKeep)
        //assertFalse TransformUtils.extractNodesAsPoints(ds, prefix, epsgCode, outTable, tags, null)
    }
}
