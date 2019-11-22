package org.orbisgis.osm.utils

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.osm.AbstractOSMTest
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertArrayEquals
import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertNotNull
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
     * Test the {@link org.orbisgis.osm.utils.TransformUtils#createTagList(java.lang.Object, java.lang.Object)}
     * method.
     */
    @Test
    void createTagListTest(){
        def h2gis = RANDOM_DS()
        def osmTable = "toto"

        h2gis.execute("CREATE TABLE toto (id int, tag_key varchar, tag_value array[255])")
        h2gis.execute("INSERT INTO toto VALUES (0, 'material', ('concrete', 'brick'))")
        assertGStringEquals ", MAX(CASE WHEN b.tag_key = 'material' THEN b.tag_value END) AS \"MATERIAL\"",
                TransformUtils.createTagList(h2gis, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto")

        h2gis.execute("CREATE TABLE toto (id int, tag_key varchar, tag_value array[255])")
        h2gis.execute("INSERT INTO toto VALUES (1, 'water', null)")
        assertGStringEquals ", MAX(CASE WHEN b.tag_key = 'water' THEN b.tag_value END) AS \"WATER\"",
                TransformUtils.createTagList(h2gis, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto")

        h2gis.execute("CREATE TABLE toto (id int, tag_key varchar, tag_value array[255])")
        h2gis.execute("INSERT INTO toto VALUES (2, 'road', '{}')")
        assertGStringEquals ", MAX(CASE WHEN b.tag_key = 'road' THEN b.tag_value END) AS \"ROAD\"",
                TransformUtils.createTagList(h2gis, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto")

        h2gis.execute("CREATE TABLE toto (id int, tag_key varchar, tag_value array[255])")
        h2gis.execute("INSERT INTO toto VALUES (0, 'material', ('concrete', 'brick'))")
        assertNull  TransformUtils.createTagList(null, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto")
    }

    /**
     * Test the {@link org.orbisgis.osm.utils.TransformUtils#createTagList(java.lang.Object, java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badCreateTagListTest(){
        def h2gis = RANDOM_DS()
        def osmTable = "toto"

        h2gis.execute("CREATE TABLE toto (id int, tag_key varchar, tag_value array[255])")
        h2gis.execute("INSERT INTO toto VALUES (3, null, ('lake', 'pound'))")
        assertGStringEquals "", TransformUtils.createTagList(h2gis, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto")

        h2gis.execute("CREATE TABLE toto (id int, tag_key varchar, tag_value array[255])")
        h2gis.execute("INSERT INTO toto VALUES (4, null, null)")
        assertGStringEquals "", TransformUtils.createTagList(h2gis, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto")
    }

    /**
     * Test the {@link org.orbisgis.osm.utils.TransformUtils#buildIndexes(org.orbisgis.datamanager.JdbcDataSource, java.lang.String)}
     * method with bad data.
     */
    @Test
    void badBuildIndexesTest(){
        def h2gis = RANDOM_DS()
        def osmTable = "toto"

        LOGGER.warn("An error will be thrown next")
        assertFalse TransformUtils.buildIndexes(h2gis, null)
        LOGGER.warn("An error will be thrown next")
        assertFalse TransformUtils.buildIndexes(null, null)
        LOGGER.warn("An error will be thrown next")
        assertFalse TransformUtils.buildIndexes(null, osmTable)
    }

    /**
     * Test the {@link org.orbisgis.osm.utils.TransformUtils#buildIndexes(org.orbisgis.datamanager.JdbcDataSource, java.lang.String)}
     * method.
     */
    @Test
    void buildIndexesTest(){
        def h2gis = RANDOM_DS()
        def osmTablesPrefix = "toto"
        h2gis.execute """
            CREATE TABLE ${osmTablesPrefix}_node(id_node varchar);
            CREATE TABLE ${osmTablesPrefix}_way_node(id_node varchar, node_order varchar, id_way varchar);
            CREATE TABLE ${osmTablesPrefix}_way(id_way varchar, not_taken_into_account varchar);
            CREATE TABLE ${osmTablesPrefix}_way_tag(tag_key varchar,id_way varchar,tag_value varchar);
            CREATE TABLE ${osmTablesPrefix}_relation_tag(tag_key varchar,id_relation varchar,tag_value varchar);
            CREATE TABLE ${osmTablesPrefix}_relation(id_relation varchar);
            CREATE TABLE ${osmTablesPrefix}_way_member(id_relation varchar);
            CREATE TABLE ${osmTablesPrefix}_way_not_taken_into_account(id_way varchar);
            CREATE TABLE ${osmTablesPrefix}_relation_not_taken_into_account(id_relation varchar);
        """

        TransformUtils.buildIndexes(h2gis, osmTablesPrefix)

        assertNotNull h2gis.getTable("${osmTablesPrefix}_node")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_node")."id_node"
        assertTrue h2gis.getTable("${osmTablesPrefix}_node")."id_node".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_node")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_node")."id_node"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way_node")."id_node".indexed
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_node")."node_order"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way_node")."node_order".indexed
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_node")."id_way"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way_node")."id_way".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_way")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way")."id_way"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way")."id_way".indexed
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way")."not_taken_into_account"
        assertFalse h2gis.getTable("${osmTablesPrefix}_way")."not_taken_into_account".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_tag")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_tag")."tag_key"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way_tag")."tag_key".indexed
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_tag")."id_way"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way_tag")."id_way".indexed
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_tag")."tag_value"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way_tag")."tag_value".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation_tag")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation_tag")."tag_key"
        assertTrue h2gis.getTable("${osmTablesPrefix}_relation_tag")."tag_key".indexed
        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation_tag")."id_relation"
        assertTrue h2gis.getTable("${osmTablesPrefix}_relation_tag")."id_relation".indexed
        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation_tag")."tag_value"
        assertTrue h2gis.getTable("${osmTablesPrefix}_relation_tag")."tag_value".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation")."id_relation"
        assertTrue h2gis.getTable("${osmTablesPrefix}_relation")."id_relation".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_member")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_member")."id_relation"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way_member")."id_relation".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_not_taken_into_account")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_not_taken_into_account")."id_way"
        assertFalse h2gis.getTable("${osmTablesPrefix}_way_not_taken_into_account")."id_way".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation_not_taken_into_account")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation_not_taken_into_account")."id_relation"
        assertFalse h2gis.getTable("${osmTablesPrefix}_relation_not_taken_into_account")."id_relation".indexed
    }

    /**
     * Test the {@link org.orbisgis.osm.utils.TransformUtils#arrayUnion(boolean, java.util.Collection[])}
     * method with bad data.
     */
    @Test
    void badArrayUnionTest(){
        assertNotNull TransformUtils.arrayUnion(true, null)
        assertTrue TransformUtils.arrayUnion(true, null).isEmpty()
        assertNotNull TransformUtils.arrayUnion(true, [])
        assertTrue TransformUtils.arrayUnion(true, []).isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.osm.utils.TransformUtils#arrayUnion(boolean, java.util.Collection[])}
     * method.
     */
    @Test
    void arrayUnionTest(){
        def unique = TransformUtils.arrayUnion(true, ["tata", "titi", "tutu"], ["titi", "toto", "toto"], [null, "value"])
        assertNotNull unique
        assertEquals 6, unique.size
        assertEquals null, unique[0]
        assertEquals "tata", unique[1]
        assertEquals "titi", unique[2]
        assertEquals "toto", unique[3]
        assertEquals "tutu", unique[4]
        assertEquals "value", unique[5]

        def notUnique = TransformUtils.arrayUnion(false, ["tata", "titi", "tutu"], ["titi", "toto", "toto"], [null, "value"])
        assertNotNull notUnique
        assertEquals 8, notUnique.size
        assertEquals null, notUnique[0]
        assertEquals "tata", notUnique[1]
        assertEquals "titi", notUnique[2]
        assertEquals "titi", notUnique[3]
        assertEquals "toto", notUnique[4]
        assertEquals "toto", notUnique[5]
        assertEquals "tutu", notUnique[6]
        assertEquals "value", notUnique[7]
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

        LOGGER.warn("An error will be thrown next")
        assertFalse TransformUtils.extractNodesAsPoints(null, prefix, epsgCode, outTable, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertFalse TransformUtils.extractNodesAsPoints(ds, null, epsgCode, outTable, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertFalse TransformUtils.extractNodesAsPoints(ds, prefix, -1, outTable, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertFalse TransformUtils.extractNodesAsPoints(ds, prefix, epsgCode, null, tags, columnsToKeep)
        assertTrue TransformUtils.extractNodesAsPoints(ds, prefix, epsgCode, outTable, null, columnsToKeep)
        //assertFalse TransformUtils.extractNodesAsPoints(ds, prefix, epsgCode, outTable, tags, null)
    }
}
