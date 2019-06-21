
package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript OSMHelper osmHelper

/**
 * This script if used to extract all polygons from the OSM tables
 * @return
 */
static IProcess toPolygons() {
    return processFactory.create(
            "Transform all OSM features as polygons",
            [datasource: JdbcDataSource,
             osmTablesPrefix: String
            ],
            [outputTableName : JdbcDataSource],
            { datasource, osmTablesPrefix  ->
                if (datasource != null) {
                    logger.info('Start polygon transformation')
                    outputTableName = "";

                }
                else{
                    logger.info('Please set a valid database connection')
                }
                [ outputTableName : outputTableName]
            }
    )

}


/**
 * This function is used to extract ways as polygons
 */
def extractWaysAsPolygons(JdbcDataSource dataSource){

    def wherequery = ""
    if (!listofkeys.isEmpty()){
        wherequery+= "t.tag_key in('${listofkeys.join("','")}')"
    }

    if (!listoftags.isEmpty()){
        if(!wherequery.isEmpty()){
            wherequery += " or  "
        }
        wherequery += "wt.value in('${listoftags.join("','")}')"
    }

    def query = "create table tag_ways as SELECT id_way,  t.tag_key as key , wt.value as tag  FROM map_way_tag wt left join  map_tag t on wt.id_tag = t.id_tag "

    if(!wherequery.isEmpty()){
        query+=" where "+ wherequery
    }


    dataSource.execute "drop table if exists tag_ways; $query;".toString()
    dataSource.execute "create index on tag_ways(id_way)"

//Limit the number of ways
    dataSource.execute "drop table if exists distinct_ways; create table distinct_ways as select distinct id_way from tag_ways"
    dataSource.execute "create index on distinct_ways(id_way)"

    def rows = dataSource.rows("select distinct key from tag_ways")

    if(rows.size()>0){

        def caseWhenList = rows.inject([]) { list, it ->
            list << "MAX(case when key='$it.key' then tag end) as \"${it.key.replace(":","_")}\""
        }
        caseWhen = caseWhenList.join(",")


        dataSource.execute "DROP TABLE IF EXISTS flat_keys_tags_ways; create table flat_keys_tags_ways as select id_way, $caseWhen from tag_ways group by id_way".toString()
        dataSource.execute "create index on flat_keys_tags_ways(id_way)"

        dataSource.execute "DROP TABLE IF EXISTS WAYS_POLYGONS;"
//Create buildings from ways
        dataSource.execute "CREATE TABLE WAYS_POLYGONS AS"+
                " SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKEPOLYGON(ST_MAKELINE(the_geom)), 4326), 2154) the_geom, flat_keys_tags_ways.* FROM "+
                " (SELECT "+
                "   (SELECT ST_ACCUM(the_geom) the_geom"+
                " FROM"+
                " (SELECT n.id_node, n.the_geom, wn.id_way idway "+
                " FROM map_node n, map_way_node wn "+
                " WHERE n.id_node = wn.id_node "+
                " ORDER BY wn.node_order) "+
                " WHERE  idway = w.id_way) the_geom ,w.id_way "+
                " FROM map_way w, distinct_ways b"+
                " WHERE w.id_way = b.id_way) geom_table, flat_keys_tags_ways "+
                " WHERE ST_GEOMETRYN(the_geom, 1) = ST_GEOMETRYN(the_geom, ST_NUMGEOMETRIES(the_geom))"+
                " AND ST_NUMGEOMETRIES(the_geom) > 3 and geom_table.id_way=flat_keys_tags_ways.id_way;"

    }
    else{
        logger.info("No tag or key founded for in the ways")
    }
}


/**
 * Extract relations as polygons
 */
def extractRelationsAsPolygons(JdbcDataSource dataSource){
    def wherequery = ""
    if (!listofkeys.isEmpty()){
        wherequery+= "t.tag_key in('${listofkeys.join("','")}')"
    }

    if (!listoftags.isEmpty()){
        if(!wherequery.isEmpty()){
            wherequery += " or  "
        }
        wherequery += "rt.tag_value in('${listoftags.join("','")}')"
    }

    def query = "create table tag_relations_tmp as SELECT rt.id_relation,  t.tag_key as key , rt.tag_value as tag  FROM map_relation_tag rt left join map_tag t on rt.id_tag = t.id_tag "

    if(!wherequery.isEmpty()){
        query+=" where "+ wherequery
    }

    dataSource.execute "drop table if exists tag_relations, tag_relations_tmp; $query;".toString()
    dataSource.execute "create index on tag_relations_tmp(id_relation);"
    dataSource.execute "create table tag_relations as select a.id_relation, b.id_way,  a.key , a.tag, b.role from tag_relations_tmp as a left join map_way_member as b on a.id_relation = b.id_relation;"
    dataSource.execute "create index on tag_relations(id_relation)"

//Limit the number of relations
    dataSource.execute "drop table if exists distinct_relations; create table distinct_relations as select distinct id_relation from tag_relations"
    dataSource.execute "create index on distinct_relations(id_relation)"

    def rows = dataSource.rows("select distinct key from tag_relations")

    if(rows.size()>0){

        def caseWhenList = rows.inject([]) { list, it ->
            list << "MAX(case when key='$it.key' then tag end) as \"${it.key.replace(":","_")}\""
        }
        caseWhen = caseWhenList.join(",")

        dataSource.execute "DROP TABLE IF EXISTS flat_keys_tags_relations; create table flat_keys_tags_relations as select id_relation, $caseWhen from tag_relations group by id_relation".toString()
        dataSource.execute "create index on flat_keys_tags_relations(id_relation)"

        dataSource.execute "DROP TABLE IF EXISTS RELATIONS_POLYGONS_OUTER;"
        dataSource.execute "CREATE TABLE RELATIONS_POLYGONS_OUTER AS "+
                "SELECT ST_LINEMERGE(st_union(ST_ACCUM(the_geom))) the_geom, id_relation "+
                "   FROM "+
                "      (SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), 2154) the_geom, id_relation, role, id_way "+
                "     FROM      "+
                "        (SELECT "+
                "           (SELECT ST_ACCUM(the_geom) the_geom "+
                "          FROM "+
                "             (SELECT n.id_node, n.the_geom, wn.id_way idway "+
                "            FROM map_node n, map_way_node wn "+
                "           WHERE n.id_node = wn.id_node ORDER BY wn.node_order) "+
                "      WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role "+
                " FROM map_way w, tag_relations br "+
                "WHERE w.id_way = br.id_way and br.role='outer') geom_table where st_numgeometries(the_geom)>=2) "+
                " GROUP BY id_relation; "

        dataSource.execute "DROP TABLE IF EXISTS RELATIONS_POLYGONS_OUTER_EXPLODED;"
        dataSource.execute "CREATE TABLE RELATIONS_POLYGONS_OUTER_EXPLODED AS SELECT ST_ACCUM(ST_MAKEPOLYGON(the_geom)) as the_geom, id_relation from st_explode('RELATIONS_POLYGONS_OUTER') WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom) GROUP BY id_relation;"

        dataSource.execute "DROP TABLE IF EXISTS RELATIONS_POLYGONS_INNER ;"
        dataSource.execute "CREATE TABLE RELATIONS_POLYGONS_INNER AS "+
                "SELECT ST_LINEMERGE(st_union(ST_ACCUM(the_geom))) the_geom, id_relation "+
                "   FROM "+
                "      (SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), 2154) the_geom, id_relation, role, id_way "+
                "     FROM      "+
                "        (SELECT "+
                "           (SELECT ST_ACCUM(the_geom) the_geom "+
                "          FROM "+
                "             (SELECT n.id_node, n.the_geom, wn.id_way idway "+
                "            FROM map_node n, map_way_node wn "+
                "           WHERE n.id_node = wn.id_node ORDER BY wn.node_order) "+
                "      WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role "+
                " FROM map_way w, tag_relations br "+
                "WHERE w.id_way = br.id_way and br.role='inner') geom_table where st_numgeometries(the_geom)>=2) "+
                "GROUP BY id_relation; "

        dataSource.execute "DROP TABLE IF EXISTS RELATIONS_POLYGONS_INNER_EXPLODED;"
        dataSource.execute "CREATE TABLE RELATIONS_POLYGONS_INNER_EXPLODED AS SELECT st_ACCUM(ST_MAKEPOLYGON(the_geom)) as the_geom, id_relation from st_explode('RELATIONS_POLYGONS_INNER') WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom) GROUP BY id_relation;"

        dataSource.execute "create index on RELATIONS_POLYGONS_OUTER_EXPLODED(id_relation);create index on RELATIONS_POLYGONS_INNER_EXPLODED(id_relation)"

        dataSource.execute "drop table if exists RELATIONS_POLYGONS_DIFF, RELATIONS_POLYGONS;"
        dataSource.execute "create table RELATIONS_POLYGONS_DIFF as select ST_difference(a.the_geom, b.the_geom) as the_geom, c.* from RELATIONS_POLYGONS_OUTER_EXPLODED as a , RELATIONS_POLYGONS_INNER_EXPLODED as b, FLAT_KEYS_TAGS_RELATIONS as c where a.id_relation=b.id_relation and a.id_relation=c.id_relation;"

        dataSource.execute "create table RELATIONS_POLYGONS as select * from RELATIONS_POLYGONS_DIFF union  select a.the_geom, c.* from RELATIONS_POLYGONS_OUTER_EXPLODED as a left JOIN  RELATIONS_POLYGONS_INNER_EXPLODED as b on a.id_relation=b.id_relation left JOIN  FLAT_KEYS_TAGS_RELATIONS as c on a.id_relation=c.id_relation WHERE b.id_relation IS NULL;"


    }
}


static IProcess toLines() {

}


static IProcess toPoints() {

}