
package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript OSMHelper osmHelper

/**
 * This script if used to extract all polygons from the OSM tables
 *
 * @return
 */
static IProcess toPolygons() {
    return processFactory.create(
            "Transform all OSM features as polygons",
            [datasource: JdbcDataSource,
             osmTablesPrefix: String,
             epsgCode: int
            ],
            [outputTableName : String],
            { datasource, osmTablesPrefix, epsgCode ->
                if (datasource != null) {
                    logger.info('Start polygon transformation')
                    logger.info("Indexing osm tables...")
                    buildIndexes(datasource,osmTablesPrefix)
                    boolean polygonWays = extractWaysAsPolygons(datasource,osmTablesPrefix, epsgCode)
                    boolean polygonRelations = extractRelationsAsPolygons(datasource,osmTablesPrefix,epsgCode)
                    if(polygonWays && polygonRelations){
                        //Merge ways and relations

                        logger.info('The polygons haven been builded.')
                    }
                    else if(polygonWays) {


                        logger.info('The polygons haven been builded.')

                    }
                    else if(polygonRelations){


                        logger.info('The polygons haven been builded.')

                    }
                    else{
                        logger.info('Cannot extract any polygons.')
                    }
                }
                else{
                    logger.info('Please set a valid database connection')
                }
                [ outputTableName : "blaba"]
            }
    )

}

/**
 * This function is used to extract ways as polygons
 *
 * @param dataSource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode epsg code to reproject the geometries
 * @return true if some polygons have been build
 */
static boolean extractWaysAsPolygons(JdbcDataSource dataSource, String osmTablesPrefix, int epsgCode){
    def rows = dataSource.firstRow("""select count(*) as count from ${osmTablesPrefix}_way_tag""")
    if(rows.count>0) {
        dataSource.execute "DROP TABLE IF EXISTS WAYS_POLYGONS_TMP;"
        //Create polygons from ways
        dataSource.execute """CREATE TABLE WAYS_POLYGONS_TMP AS 
           SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKEPOLYGON(ST_MAKELINE(the_geom)), 4326), ${epsgCode}) as the_geom, id_way
           FROM  (SELECT (SELECT ST_ACCUM(the_geom) as the_geom FROM  
           (SELECT n.id_node, n.the_geom, wn.id_way idway FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn WHERE n.id_node = wn.id_node ORDER BY wn.node_order)
            WHERE  idway = w.id_way) the_geom ,w.id_way  
            FROM ${osmTablesPrefix}_way w) geom_table 
            WHERE ST_GEOMETRYN(the_geom, 1) = ST_GEOMETRYN(the_geom, ST_NUMGEOMETRIES(the_geom)) AND ST_NUMGEOMETRIES(the_geom) > 3;"""

        dataSource.execute "CREATE INDEX ON WAYS_POLYGONS_TMP(ID_WAY);"

        def rowskeys = dataSource.rows("""select distinct a.tag_key as tag_key from ${osmTablesPrefix}_way_tag as a, WAYS_POLYGONS_TMP as b
            where a.id_way=b.id_way """)

        def list = []
        rowskeys.tag_key.each { it ->
            list << "MAX(CASE WHEN b.tag_key = '${it}' then b.tag_value END) as \"${it}\""
        }
        dataSource.execute """drop table if exists WAYS_POLYGONS; 
        CREATE TABLE WAYS_POLYGONS AS SELECT a.id_way, a.the_geom, ${list.join(",")} from WAYS_POLYGONS_TMP as a, ${osmTablesPrefix}_way_tag  b where a.id_way=b.id_way group by a.id_way;"""

        dataSource.execute("""DROP TABLE IF EXISTS WAYS_POLYGONS_TMP;""")

        dataSource.save("WAYS_POLYGONS", "/tmp/ways_polygons.geojson")
        return true
    }
    else {
        logger.info("No keys or values founded in the ways")
        return false
    }

}

/**
 * This function is used to extract relations as polygons
 * @param dataSource
 * @param osmTablesPrefix
 * @param epsgCode
 * @return true if some polygons have been build
 */
static  boolean extractRelationsAsPolygons(JdbcDataSource dataSource, String osmTablesPrefix, int epsgCode){
    def rows = dataSource.rows("select distinct TAG_KEY from ${osmTablesPrefix}_relation_tag")
    if(rows.size()>0){
        def list = []
        rows.tag_key.each { it ->
            list << "MAX(CASE WHEN tag_key = '${it}' then tag_value END) as \"${it}\""
        }
        dataSource.execute """DROP TABLE IF EXISTS flat_keys_tags_relations; 
                create table flat_keys_tags_relations as select id_relation, ${list.join(",")} 
                from ${osmTablesPrefix}_relation_tag group by id_relation;
                create index on flat_keys_tags_relations(id_relation);"""

        dataSource.execute "DROP TABLE IF EXISTS RELATIONS_POLYGONS_OUTER;"
        dataSource.execute "CREATE TABLE RELATIONS_POLYGONS_OUTER AS "+
                "SELECT ST_LINEMERGE(st_union(ST_ACCUM(the_geom))) the_geom, id_relation "+
                "   FROM "+
                "      (SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), ${epsgCode}) the_geom, id_relation, role, id_way "+
                "     FROM      "+
                "        (SELECT "+
                "           (SELECT ST_ACCUM(the_geom) the_geom "+
                "          FROM "+
                "             (SELECT n.id_node, n.the_geom, wn.id_way idway "+
                "            FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn "+
                "           WHERE n.id_node = wn.id_node ORDER BY wn.node_order) "+
                "      WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role "+
                " FROM ${osmTablesPrefix}_way w, ${osmTablesPrefix}_way_member br "+
                "WHERE w.id_way = br.id_way and br.role='outer') geom_table where st_numgeometries(the_geom)>=2) "+
                " GROUP BY id_relation; "

        dataSource.execute """DROP TABLE IF EXISTS RELATIONS_POLYGONS_OUTER_EXPLODED;
        CREATE TABLE RELATIONS_POLYGONS_OUTER_EXPLODED AS 
                SELECT ST_ACCUM(ST_MAKEPOLYGON(the_geom)) as the_geom, id_relation 
                from st_explode('RELATIONS_POLYGONS_OUTER') 
                WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom) GROUP BY id_relation;"""

        dataSource.execute """DROP TABLE IF EXISTS RELATIONS_POLYGONS_INNER ;
        CREATE TABLE RELATIONS_POLYGONS_INNER AS 
                SELECT ST_LINEMERGE(st_union(ST_ACCUM(the_geom))) the_geom, id_relation 
                   FROM 
                      (SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), ${epsgCode}) the_geom, id_relation, role, id_way 
                     FROM      
                        (SELECT 
                           (SELECT ST_ACCUM(the_geom) the_geom 
                          FROM 
                             (SELECT n.id_node, n.the_geom, wn.id_way idway 
                            FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
                           WHERE n.id_node = wn.id_node ORDER BY wn.node_order) s
                      WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role 
                 FROM ${osmTablesPrefix}_way w, ${osmTablesPrefix}_way_member br 
                WHERE w.id_way = br.id_way and br.role='inner') geom_table where st_numgeometries(the_geom)>=2) 
                GROUP BY id_relation; """

        dataSource.execute """DROP TABLE IF EXISTS RELATIONS_POLYGONS_INNER_EXPLODED;
                CREATE TABLE RELATIONS_POLYGONS_INNER_EXPLODED AS 
                SELECT st_ACCUM(ST_MAKEPOLYGON(the_geom)) as the_geom, id_relation 
                from st_explode('RELATIONS_POLYGONS_INNER') WHERE 
                ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom) GROUP BY id_relation;"""

        dataSource.execute """create index on RELATIONS_POLYGONS_OUTER_EXPLODED(id_relation);
                                create index on RELATIONS_POLYGONS_INNER_EXPLODED(id_relation)"""

        dataSource.execute """drop table if exists RELATIONS_POLYGONS_DIFF, RELATIONS_POLYGONS;
        create table RELATIONS_POLYGONS_DIFF as select ST_MakePolygon(a.the_geom, b.the_geom) as the_geom, 
        c.* from RELATIONS_POLYGONS_OUTER_EXPLODED as a , RELATIONS_POLYGONS_INNER_EXPLODED as b, FLAT_KEYS_TAGS_RELATIONS as c 
        where a.id_relation=b.id_relation and a.id_relation=c.id_relation;"""

        dataSource.execute """create table RELATIONS_POLYGONS as select * from RELATIONS_POLYGONS_DIFF union  
        select a.the_geom, c.* from RELATIONS_POLYGONS_OUTER_EXPLODED as a left JOIN  RELATIONS_POLYGONS_INNER_EXPLODED as b 
        on a.id_relation=b.id_relation left JOIN  FLAT_KEYS_TAGS_RELATIONS as c on a.id_relation=c.id_relation 
        WHERE b.id_relation IS NULL;"""

        //dataSource.save("RELATIONS_POLYGONS","/tmp/polygons_relation_osm.geojson")
        return true
    }
    else{
        logger.info("No keys or values founded in the relations")
        return false
    }
}


static IProcess toLines() {

}


static IProcess toPoints() {

}

/**
 * Build the indexes to perform analysis
 * @param dataSource
 * @param osmTablesPrefix
 * @return
 */
static  def buildIndexes(JdbcDataSource dataSource, String osmTablesPrefix){
    dataSource.execute "CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_node_index on ${osmTablesPrefix}_node(id_node);"+
            "CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_node_id_node_index on ${osmTablesPrefix}_way_node(id_node);"+
            "CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_node_order_index on ${osmTablesPrefix}_way_node(node_order);"+
            "CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_node_id_way_index ON ${osmTablesPrefix}_way_node(id_way);"+
            "CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_index on ${osmTablesPrefix}_way(id_way);"+
            "CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_tag_key_tag_index on ${osmTablesPrefix}_way_tag(tag_key);"+
            "CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_tag_id_way_index on ${osmTablesPrefix}_way_tag(id_way);"+
            "CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_tag_value_index on ${osmTablesPrefix}_way_tag(tag_value);"+
            "CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_relation_tag_key_tag_index ON ${osmTablesPrefix}_relation_tag(tag_key);"+
            "CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_relation_tag_id_relation_index ON ${osmTablesPrefix}_relation_tag(id_relation);"+
            "CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_relation_tag_tag_value_index ON ${osmTablesPrefix}_relation_tag(tag_value);"+
            "CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_relation_id_relation_index ON ${osmTablesPrefix}_relation(id_relation);"+
            "CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_member_id_relation_index ON ${osmTablesPrefix}_way_member(id_relation);" +
            "CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_id_way on ${osmTablesPrefix}_way(id_way);"
}