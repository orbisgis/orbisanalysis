
package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript OSMHelper osmHelper


/**
 * This script if used to extract all points from the OSM tables
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode epsg code to reproject the geometries
 * @param tag_keys list ok keys to be filtered
 * @return a name for the table that contains all points
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Lesaux UBS LAB-STICC
 */
static IProcess toPoints() {
    return processFactory.create(
        "Transform all OSM features as points",
        [datasource     : JdbcDataSource, osmTablesPrefix: String, epsgCode  : int, tag_keys : []],
        [ outputTableName: String],
         { datasource, osmTablesPrefix, epsgCode, tag_keys ->
            String outputTableName = "OSM_POINTS_${uuid()}"
            if (datasource != null) {
                logger.info('Start points transformation')
                logger.info("Indexing osm tables...")
                buildIndexes(datasource, osmTablesPrefix)
                boolean pointsNodes = extractNodesAsPoints(datasource, osmTablesPrefix, epsgCode, outputTableName, tag_keys)
                if (pointsNodes) {
                    logger.info('The points have been built.')
                } else {
                    logger.info('Cannot extract any points.')
                    return
                }
            } else {
                logger.error('Please set a valid database connection')
            }
            [outputTableName: outputTableName]
        }
    )
}

/**
 * This script if used to extract all lines from the OSM tables
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode epsg code to reproject the geometries
 * @param tag_keys list ok keys to be filtered
 * @return a name for the table that contains all lines
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 */
static IProcess toLines() {
    return processFactory.create(
             "Transform all OSM features as lines",
            [ datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: int,tag_keys : []],
            [ outputTableName : String],
             { datasource, osmTablesPrefix, epsgCode,tag_keys ->
                String outputTableName = "OSM_LINES_${uuid()}"
                if (datasource != null) {
                    logger.info('Start lines transformation')
                    logger.info("Indexing osm tables...")
                    buildIndexes(datasource,osmTablesPrefix)
                    boolean lineWays = extractWaysAsLines(datasource,osmTablesPrefix, epsgCode, outputTableName, tag_keys)
                    if(lineWays){
                        logger.info('The lines have been built.')
                    }
                    else{
                        logger.info('Cannot extract any lines.')
                        return
                    }
                }
                else{
                    logger.error('Please set a valid database connection')
                }
                [ outputTableName : outputTableName]
            }
    )
}



/**
 * This script if used to extract all polygons from the OSM tables
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode epsg code to reproject the geometries
 * @param tag_keys list ok keys to be filtered
 * @return a name for the table that contains all polygons
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 */
static IProcess toPolygons() {
    return processFactory.create(
         "Transform all OSM features as polygons",
        [datasource : JdbcDataSource, osmTablesPrefix: String, epsgCode : int, tag_keys : []],
        [ outputTableName: String],
         { datasource, osmTablesPrefix, epsgCode, tag_keys ->
            String outputTableName = "OSM_POLYGONS_${uuid()}"
            if (datasource != null) {
                logger.info('Start polygon transformation')
                logger.info("Indexing osm tables...")
                buildIndexes(datasource, osmTablesPrefix)
                String ouputWayPolygons = "WAYS_POLYGONS_${uuid()}"
                boolean polygonWays = extractWaysAsPolygons(datasource, osmTablesPrefix, epsgCode, ouputWayPolygons, tag_keys)
                String ouputRelationPolygons = "RELATION_POLYGONS_${uuid()}"
                boolean polygonRelations = extractRelationsAsPolygons(datasource, osmTablesPrefix, epsgCode, ouputRelationPolygons, tag_keys)
                if (polygonWays && polygonRelations) {
                    //Merge ways and relations
                    def columnsWays = datasource.getTable(ouputWayPolygons).columnNames
                    def columnsRelations = datasource.getTable(ouputRelationPolygons).columnNames
                    def allColumns = []
                    allColumns.addAll(columnsWays)
                    allColumns.removeAll(columnsRelations)
                    allColumns.addAll(columnsRelations)
                    allColumns.sort()
                    def leftSelect = '';
                    def rigthSelect = '';
                    allColumns.each { iter ->
                        if (columnsWays.contains(iter)) {
                            leftSelect += "\"${iter}\","
                        } else {
                            leftSelect += "null as \"${iter}\","
                        }

                        if (columnsRelations.contains(iter)) {
                            rigthSelect += "\"${iter}\","

                        } else {
                            rigthSelect += "null as \"${iter}\","
                        }
                    }
                    leftSelect = leftSelect[0..-2]
                    rigthSelect = rigthSelect[0..-2]

                    datasource.execute """DROP TABLE IF EXISTS ${outputTableName};
                        CREATE TABLE ${outputTableName} AS 
                        SELECT  ${leftSelect} from ${ouputWayPolygons} 
                        union all 
                        select  ${rigthSelect} from ${ouputRelationPolygons};
                        DROP TABLE IF EXISTS ${ouputWayPolygons}, ${ouputRelationPolygons};"""
                    logger.info('The polygons have been built.')
                } else if (polygonWays) {
                    datasource.execute """ALTER TABLE ${ouputWayPolygons} RENAME TO ${outputTableName};"""
                    logger.info('The polygons have been built.')
                } else if (polygonRelations) {
                    datasource.execute """ALTER TABLE ${ouputRelationPolygons} RENAME TO ${outputTableName};"""
                    logger.info('The polygons have been built.')
                } else {
                    logger.info('Cannot extract any polygons.')
                    return
                }
            } else {
                logger.error('Please set a valid database connection')
            }
            [outputTableName: outputTableName]
        }
    )

}

/**
 * This function is used to extract ways as polygons
 *
 * @param dataSource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode epsg code to reproject the geometries
 * @param ouputWayPolygons the name of the way polygons table
 * @param tag_keys list ok keys to be filtered
 * @return true if some polygons have been build
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 */
static boolean extractWaysAsPolygons(JdbcDataSource dataSource, String osmTablesPrefix, int epsgCode, String ouputWayPolygons, def tag_keys){
    def countTagKeysQuery = "select count(*) as count from ${osmTablesPrefix}_way_tag"
    boolean filterByKeys = false
    def whereKeysFilter
    if(tag_keys!=null && !tag_keys.isEmpty()){
        whereKeysFilter = "tag_key in ('${tag_keys.join("','")}')"
        countTagKeysQuery+= " where ${whereKeysFilter}"
        filterByKeys=true
    }
    def rows = dataSource.firstRow(countTagKeysQuery)
    if(rows.count>0) {
        logger.info("Build way polygons")
        String WAYS_POLYGONS_TMP = "WAYS_POLYGONS_TMP${uuid()}"
        dataSource.execute "DROP TABLE IF EXISTS ${WAYS_POLYGONS_TMP};"
        //Create polygons from ways
        if(filterByKeys){
            dataSource.execute """CREATE TABLE ${WAYS_POLYGONS_TMP} AS 
           SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKEPOLYGON(ST_MAKELINE(the_geom)), 4326), ${epsgCode}) as the_geom, id_way
           FROM  (SELECT (SELECT ST_ACCUM(the_geom) as the_geom FROM  
           (SELECT n.id_node, n.the_geom, wn.id_way idway FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn WHERE n.id_node = wn.id_node ORDER BY wn.node_order)
            WHERE  idway = w.id_way) the_geom ,w.id_way  
            FROM ${osmTablesPrefix}_way w, (SELECT DISTINCT id_way 
            FROM ${osmTablesPrefix}_way_tag wt
            WHERE ${whereKeysFilter}) b 
            WHERE w.id_way = b.id_way) geom_table
            WHERE ST_GEOMETRYN(the_geom, 1) = ST_GEOMETRYN(the_geom, ST_NUMGEOMETRIES(the_geom)) AND ST_NUMGEOMETRIES(the_geom) > 3
             """
        }
        else{
            dataSource.execute """CREATE TABLE ${WAYS_POLYGONS_TMP} AS 
           SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKEPOLYGON(ST_MAKELINE(the_geom)), 4326), ${epsgCode}) as the_geom, id_way
           FROM  (SELECT (SELECT ST_ACCUM(the_geom) as the_geom FROM  
           (SELECT n.id_node, n.the_geom, wn.id_way idway FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn WHERE n.id_node = wn.id_node ORDER BY wn.node_order)
            WHERE  idway = w.id_way) the_geom ,w.id_way  
            FROM ${osmTablesPrefix}_way w) geom_table
            WHERE ST_GEOMETRYN(the_geom, 1) = ST_GEOMETRYN(the_geom, ST_NUMGEOMETRIES(the_geom)) AND ST_NUMGEOMETRIES(the_geom) > 3
         """
        }
        def caseWhenFilter = """select distinct a.tag_key as tag_key from ${osmTablesPrefix}_way_tag as a, ${WAYS_POLYGONS_TMP} as b
            where a.id_way=b.id_way """
        if(filterByKeys){
            caseWhenFilter+= "and ${whereKeysFilter}"
        }
        def rowskeys = dataSource.rows(caseWhenFilter)

        def list = []
        rowskeys.tag_key.each { it ->
            list << "MAX(CASE WHEN b.tag_key = '${it}' then b.tag_value END) as \"${it}\""
        }
        if(list.isEmpty()){
            dataSource.execute """drop table if exists ${ouputWayPolygons}; 
        CREATE TABLE ${ouputWayPolygons} AS SELECT 'w'||a.id_way as id, a.the_geom from 
        ${WAYS_POLYGONS_TMP} as a, ${osmTablesPrefix}_way_tag  b where a.id_way=b.id_way group by a.id_way;"""
        }else {
            dataSource.execute """drop table if exists ${ouputWayPolygons}; 
        CREATE TABLE ${ouputWayPolygons} AS SELECT 'w'||a.id_way as id, a.the_geom, ${list.join(",")} from 
        ${WAYS_POLYGONS_TMP} as a, ${osmTablesPrefix}_way_tag  b where a.id_way=b.id_way group by a.id_way;"""
        }
        dataSource.execute("""DROP TABLE IF EXISTS ${WAYS_POLYGONS_TMP};""")
        return true
    }
    else {
        logger.error("No keys or values founded in the ways")
        return false
    }

}

/**
 * This function is used to extract relations as polygons
 * @param dataSource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode epsg code to reproject the geometries
 * @param ouputRelationPolygons the name of the relation polygons table
 * @param tag_keys list ok keys to be filtered
 * @return true if some polygons have been build
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 */
static  boolean extractRelationsAsPolygons(JdbcDataSource dataSource, String osmTablesPrefix, int epsgCode, String ouputRelationPolygons, def tag_keys){

    def countTagKeysQuery = "select count(*) as count from ${osmTablesPrefix}_relation_tag"
    boolean filterByKeys = false
    def whereKeysFilter
    if(tag_keys!=null && !tag_keys.isEmpty()){
        whereKeysFilter = "tag_key in ('${tag_keys.join("','")}')"
        countTagKeysQuery+= " where tag_key in ('${tag_keys.join("','")}')"
        filterByKeys=true
    }
    def rows = dataSource.firstRow(countTagKeysQuery)
    if(rows.size()>0){
        logger.info("Build outer polygons")
        String RELATIONS_POLYGONS_OUTER = "RELATIONS_POLYGONS_OUTER_${uuid()}"
        String RELATION_FILTERED_KEYS = "RELATION_FILTERED_KEYS_${uuid()}"
        if(filterByKeys){
            dataSource.execute """ DROP TABLE IF EXISTS ${RELATION_FILTERED_KEYS};
                     CREATE TABLE ${RELATION_FILTERED_KEYS} as SELECT DISTINCT id_relation  FROM ${osmTablesPrefix}_relation_tag wt 
                          WHERE ${whereKeysFilter};
                CREATE INDEX ON ${RELATION_FILTERED_KEYS}(id_relation);"""
            dataSource.execute """DROP TABLE IF EXISTS ${RELATIONS_POLYGONS_OUTER};
        CREATE TABLE ${RELATIONS_POLYGONS_OUTER} AS 
                SELECT ST_LINEMERGE(st_union(ST_ACCUM(the_geom))) the_geom, id_relation 
                   FROM 
                      (SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), ${epsgCode}) the_geom, id_relation, role, id_way 
                     FROM      
                        (SELECT 
                           (SELECT ST_ACCUM(the_geom) the_geom 
                          FROM 
                             (SELECT n.id_node, n.the_geom, wn.id_way idway 
                            FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
                           WHERE n.id_node = wn.id_node ORDER BY wn.node_order) 
                      WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role 
                 FROM ${osmTablesPrefix}_way w, ${osmTablesPrefix}_way_member br, 
                ${RELATION_FILTERED_KEYS} g
                WHERE w.id_way = br.id_way and br.role='outer' and br.id_relation=g.id_relation) geom_table
                 where st_numgeometries(the_geom)>=2) 
                 GROUP BY id_relation;"""
        }else {
            dataSource.execute """DROP TABLE IF EXISTS ${RELATIONS_POLYGONS_OUTER};
        CREATE TABLE ${RELATIONS_POLYGONS_OUTER} AS 
                SELECT ST_LINEMERGE(st_union(ST_ACCUM(the_geom))) the_geom, id_relation 
                   FROM 
                      (SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), ${epsgCode}) the_geom, id_relation, role, id_way 
                     FROM      
                        (SELECT 
                           (SELECT ST_ACCUM(the_geom) the_geom 
                          FROM 
                             (SELECT n.id_node, n.the_geom, wn.id_way idway 
                            FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
                           WHERE n.id_node = wn.id_node ORDER BY wn.node_order) 
                      WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role 
                 FROM ${osmTablesPrefix}_way w, ${osmTablesPrefix}_way_member br 
                WHERE w.id_way = br.id_way and br.role='outer') geom_table where st_numgeometries(the_geom)>=2) 
                 GROUP BY id_relation;"""
        }
        logger.info("Build inner polygons")
        String RELATIONS_POLYGONS_INNER = "RELATIONS_POLYGONS_INNER_${uuid()}"
        if(filterByKeys){
            dataSource.execute """DROP TABLE IF EXISTS ${RELATIONS_POLYGONS_INNER};
        CREATE TABLE ${RELATIONS_POLYGONS_INNER} AS 
                SELECT ST_LINEMERGE(st_union(ST_ACCUM(the_geom))) the_geom, id_relation 
                   FROM 
                      (SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), ${epsgCode}) the_geom, id_relation, role, id_way 
                     FROM      
                        (SELECT 
                           (SELECT ST_ACCUM(the_geom) the_geom 
                          FROM 
                             (SELECT n.id_node, n.the_geom, wn.id_way idway 
                            FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
                           WHERE n.id_node = wn.id_node ORDER BY wn.node_order) 
                      WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role 
                 FROM ${osmTablesPrefix}_way w, ${osmTablesPrefix}_way_member br, ${RELATION_FILTERED_KEYS} g
                WHERE w.id_way = br.id_way and br.role='inner' and br.id_relation=g.id_relation) geom_table where st_numgeometries(the_geom)>=2) 
                 GROUP BY id_relation;"""
        }
        else {
            dataSource.execute """DROP TABLE IF EXISTS ${RELATIONS_POLYGONS_INNER};
        CREATE TABLE ${RELATIONS_POLYGONS_INNER} AS 
                SELECT ST_LINEMERGE(st_union(ST_ACCUM(the_geom))) the_geom, id_relation 
                   FROM 
                      (SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), ${epsgCode}) the_geom, id_relation, role, id_way 
                     FROM      
                        (SELECT 
                           (SELECT ST_ACCUM(the_geom) the_geom 
                          FROM 
                             (SELECT n.id_node, n.the_geom, wn.id_way idway 
                            FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
                           WHERE n.id_node = wn.id_node ORDER BY wn.node_order) 
                      WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role 
                 FROM ${osmTablesPrefix}_way w, ${osmTablesPrefix}_way_member br 
                WHERE w.id_way = br.id_way and br.role='inner') geom_table where st_numgeometries(the_geom)>=2) 
                 GROUP BY id_relation;"""
        }

        logger.info("Explode outer polygons")
        String RELATIONS_POLYGONS_OUTER_EXPLODED = "RELATIONS_POLYGONS_OUTER_EXPLODED_${uuid()}"
        dataSource.execute  """
        DROP TABLE IF EXISTS ${RELATIONS_POLYGONS_OUTER_EXPLODED};
        CREATE TABLE ${RELATIONS_POLYGONS_OUTER_EXPLODED} AS 
                SELECT ST_MAKEPOLYGON(the_geom) as the_geom, id_relation 
                from st_explode('${RELATIONS_POLYGONS_OUTER}') 
                WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom); """

        logger.info("Explode inner polygons")
        String RELATIONS_POLYGONS_INNER_EXPLODED = "RELATIONS_POLYGONS_INNER_EXPLODED_${uuid()}"
        dataSource.execute """ DROP TABLE IF EXISTS ${RELATIONS_POLYGONS_INNER_EXPLODED};
        CREATE TABLE ${RELATIONS_POLYGONS_INNER_EXPLODED} AS SELECT the_geom as the_geom, id_relation 
        from st_explode('${RELATIONS_POLYGONS_INNER}') WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom); """

        logger.info("Build all polygon relations")
        String RELATIONS_MP_HOLES = "RELATIONS_MP_HOLES_${uuid()}"
        dataSource.execute """CREATE INDEX ON ${RELATIONS_POLYGONS_OUTER_EXPLODED} (the_geom) USING RTREE;
       CREATE INDEX ON ${RELATIONS_POLYGONS_INNER_EXPLODED} (the_geom) USING RTREE;
       create index on ${RELATIONS_POLYGONS_OUTER_EXPLODED}(id_relation);
       create index on ${RELATIONS_POLYGONS_INNER_EXPLODED}(id_relation);       
       DROP TABLE IF EXISTS ${RELATIONS_MP_HOLES};
       CREATE TABLE ${RELATIONS_MP_HOLES} AS (SELECT ST_MAKEPOLYGON(ST_EXTERIORRING(a.the_geom), ST_ACCUM(b.the_geom)) AS the_geom, a.ID_RELATION FROM
        ${RELATIONS_POLYGONS_OUTER_EXPLODED} AS a LEFT JOIN ${RELATIONS_POLYGONS_INNER_EXPLODED} AS b on (a.the_geom && b.the_geom AND 
        st_contains(a.the_geom, b.THE_GEOM) AND a.ID_RELATION=b.ID_RELATION) GROUP BY a.the_geom, a.id_relation) union  
        (select a.the_geom, a.ID_RELATION from ${RELATIONS_POLYGONS_OUTER_EXPLODED} as a left JOIN  ${RELATIONS_POLYGONS_INNER_EXPLODED} as b 
        on a.id_relation=b.id_relation WHERE b.id_relation IS NULL);"""


        def caseWhenQuery ="""select distinct a.tag_key as tag_key from ${osmTablesPrefix}_relation_tag as a, ${RELATIONS_MP_HOLES} as b 
                 where a.id_relation=b.id_relation """

        if(filterByKeys){
            caseWhenQuery+=" and ${whereKeysFilter}"
        }

        def rowskeys = dataSource.rows(caseWhenQuery)

        def list = []
        rowskeys.tag_key.each { it ->
            list << "MAX(CASE WHEN b.tag_key = '${it}' then b.tag_value END) as \"${it}\""
        }
        if(list.isEmpty()){
            dataSource.execute """drop table if exists ${ouputRelationPolygons};     
            CREATE TABLE ${ouputRelationPolygons} AS SELECT 'r'||a.id_relation as id, a.the_geom from ${RELATIONS_MP_HOLES} as a, ${osmTablesPrefix}_relation_tag  b where a.id_relation=b.id_relation group by a.the_geom, a.id_relation;"""
        }
        else{
        dataSource.execute """drop table if exists ${ouputRelationPolygons};     
        CREATE TABLE ${ouputRelationPolygons} AS SELECT 'r'||a.id_relation as id, a.the_geom, ${list.join(",")} from ${RELATIONS_MP_HOLES} as a, ${osmTablesPrefix}_relation_tag  b where a.id_relation=b.id_relation group by a.the_geom, a.id_relation;"""
        }
        dataSource.execute"""DROP TABLE IF EXISTS ${RELATIONS_POLYGONS_OUTER}, ${RELATIONS_POLYGONS_INNER},
           ${RELATIONS_POLYGONS_OUTER_EXPLODED}, ${RELATIONS_POLYGONS_INNER_EXPLODED}, ${RELATIONS_MP_HOLES};"""

        return true
    }
    else{
        logger.error("No keys or values founded in the relations")
        return false
    }
}


/**
 * This function is used to extract ways as lines
 *
 * @param dataSource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode epsg code to reproject the geometries
 * @param outputWaysLines the name of the way lines table
 * @param tag_keys list ok keys to be filtered
 * @return true if some polygons have been build
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Lesaux UBS LAB-STICC
 */
static boolean extractWaysAsLines(JdbcDataSource dataSource, String osmTablesPrefix, int epsgCode, String outputWaysLines, def tag_keys) {
    def countTagKeysQuery = "select count(*) as count from ${osmTablesPrefix}_way_tag"
    boolean filterByKeys = false
    def whereKeysFilter
    if(tag_keys!=null && !tag_keys.isEmpty()){
        whereKeysFilter = "tag_key in ('${tag_keys.join("','")}')"
        countTagKeysQuery+= " where ${whereKeysFilter}"
        filterByKeys=true
    }
    def rows = dataSource.firstRow(countTagKeysQuery)
    if(rows.count>0) {
        logger.info("Build ways as lines")
        String WAYS_LINES_TMP = "WAYS_LINES_TMP_${uuid()}"

        if(filterByKeys) {
            dataSource.execute """DROP TABLE IF EXISTS ${WAYS_LINES_TMP}; 
            CREATE TABLE  ${WAYS_LINES_TMP} AS SELECT id_way,ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(THE_GEOM), 4326), 
        ${epsgCode}) the_geom FROM 
             (SELECT (SELECT ST_ACCUM(the_geom) the_geom FROM (SELECT n.id_node, n.the_geom, wn.id_way idway FROM 
             ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
             WHERE n.id_node = wn.id_node ORDER BY wn.node_order) 
             WHERE idway = w.id_way) the_geom, w.id_way  
             FROM ${osmTablesPrefix}_way w, (SELECT DISTINCT id_way 
            FROM ${osmTablesPrefix}_way_tag wt
            WHERE ${whereKeysFilter}) b WHERE w.id_way = b.id_way) geom_table 
             WHERE ST_NUMGEOMETRIES(the_geom) >= 2;"""
            dataSource.execute "CREATE INDEX ON ${WAYS_LINES_TMP}(ID_WAY);"
        }
        else{
            dataSource.execute """DROP TABLE IF EXISTS ${WAYS_LINES_TMP}; 
            CREATE TABLE  ${WAYS_LINES_TMP} AS SELECT id_way,ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(THE_GEOM), 4326), 
        ${epsgCode}) the_geom FROM 
             (SELECT (SELECT ST_ACCUM(the_geom) the_geom FROM (SELECT n.id_node, n.the_geom, wn.id_way idway FROM 
             ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
             WHERE n.id_node = wn.id_node ORDER BY wn.node_order) 
             WHERE idway = w.id_way) the_geom, w.id_way  
             FROM ${osmTablesPrefix}_way w) geom_table 
             WHERE ST_NUMGEOMETRIES(the_geom) >= 2;"""
            dataSource.execute "CREATE INDEX ON ${WAYS_LINES_TMP}(ID_WAY);"
        }

        def caseWhenFilter = """select distinct a.tag_key as tag_key from ${osmTablesPrefix}_way_tag as a, ${WAYS_LINES_TMP} as b
            where a.id_way=b.id_way """
        if(filterByKeys){
            caseWhenFilter+= "and ${whereKeysFilter}"
        }
        def rowskeys = dataSource.rows(caseWhenFilter)

        def list = []
        rowskeys.tag_key.each { it ->
            list << "MAX(CASE WHEN b.tag_key = '${it}' then b.tag_value END) as \"${it}\""
        }
        if(list.isEmpty()){
            dataSource.execute """drop table if exists ${outputWaysLines};
            CREATE TABLE ${outputWaysLines} AS SELECT 'w'||a.id_way as id, a.the_geom from ${WAYS_LINES_TMP} as a, 
            ${osmTablesPrefix}_way_tag  b where a.id_way=b.id_way group by a.id_way;"""
        }
        else {
            dataSource.execute """drop table if exists ${outputWaysLines};
            CREATE TABLE ${outputWaysLines} AS SELECT 'w'||a.id_way as id, a.the_geom, ${list.join(",")} from ${WAYS_LINES_TMP} as a, 
        ${osmTablesPrefix}_way_tag  b where a.id_way=b.id_way group by a.id_way;"""
        }
        dataSource.execute("""DROP TABLE IF EXISTS ${WAYS_LINES_TMP};""")

        return true
    }else{
            logger.error("No keys or values founded in the ways")
            return false

    }
}


/**
 * This function is used to extract nodes as points
 *
 * @param dataSource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode epsg code to reproject the geometries
 * @param ouputWayPolygons the name of the way points table
 * @return true if some points have been build
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Lesaux UBS LAB-STICC
 */
static boolean extractNodesAsPoints(JdbcDataSource dataSource, String osmTablesPrefix, int epsgCode, String ouputNodesPoints, def tag_keys) {
    def countTagKeysQuery = "select count(*) as count from ${osmTablesPrefix}_node_tag"
    boolean filterByKeys = false
    def whereKeysFilter
    if(tag_keys!=null && !tag_keys.isEmpty()){
        whereKeysFilter = "tag_key in ('${tag_keys.join("','")}')"
        countTagKeysQuery+= " where ${whereKeysFilter}"
        filterByKeys=true
    }
    def rows = dataSource.firstRow(countTagKeysQuery)
    if(rows.count>0) {
        logger.info("Build nodes as points")

        def caseWhenFilter = """select distinct tag_key as tag_key from ${osmTablesPrefix}_node_tag """
        if(filterByKeys){
            caseWhenFilter+= "where ${whereKeysFilter}"
        }
        def rowskeys = dataSource.rows(caseWhenFilter)

        def list = []
        rowskeys.tag_key.each { it ->
            list << "MAX(CASE WHEN b.tag_key = '${it}' then b.tag_value END) as \"${it}\""
        }
        if(list.isEmpty()){
            if(filterByKeys){
                dataSource.execute """drop table if exists ${ouputNodesPoints}; 
        CREATE TABLE ${ouputNodesPoints} AS SELECT a.id_node,ST_TRANSFORM(ST_SETSRID(a.THE_GEOM, 4326), 
        ${epsgCode}) as the_geom, ${list.join(",")} from ${osmTablesPrefix}_node as a, ${osmTablesPrefix}_node_tag  b where a.id_node=b.id_node and b.${whereKeysFilter} group by a.id_node;"""
            }
            else {
                dataSource.execute """drop table if exists ${ouputNodesPoints}; 
        CREATE TABLE ${ouputNodesPoints} AS SELECT a.id_node,ST_TRANSFORM(ST_SETSRID(a.THE_GEOM, 4326), 
        ${epsgCode}) as the_geom, ${list.join(",")} from ${osmTablesPrefix}_node as a, ${osmTablesPrefix}_node_tag  b where a.id_node=b.id_node group by a.id_node;"""
            }
        }
        else{
            if(filterByKeys){
                dataSource.execute """drop table if exists ${ouputNodesPoints}; 
            CREATE TABLE ${ouputNodesPoints} AS SELECT a.id_node,ST_TRANSFORM(ST_SETSRID(a.THE_GEOM, 4326), 
            ${epsgCode}) as the_geom, ${list.join(",")} from ${osmTablesPrefix}_node as a, ${osmTablesPrefix}_node_tag  b where a.id_node=b.id_node and b.${whereKeysFilter} group by a.id_node;"""

            }else {
                dataSource.execute """drop table if exists ${ouputNodesPoints}; 
            CREATE TABLE ${ouputNodesPoints} AS SELECT a.id_node,ST_TRANSFORM(ST_SETSRID(a.THE_GEOM, 4326), 
            ${epsgCode}) as the_geom, ${list.join(",")} from ${osmTablesPrefix}_node as a, ${osmTablesPrefix}_node_tag  b where a.id_node=b.id_node group by a.id_node;"""
            }
        }

        return true
    }else{
        logger.error("No keys or values founded in the nodes")
        return false
    }
}

/**
 * Build the indexes to perform analysis
 * @param dataSource
 * @param osmTablesPrefix
 * @return
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Lesaux UBS LAB-STICC
 */
static def buildIndexes(JdbcDataSource dataSource, String osmTablesPrefix){
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