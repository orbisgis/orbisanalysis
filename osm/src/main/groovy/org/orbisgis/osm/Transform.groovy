package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript OSMHelper osmHelper


/**
 * This process is used to extract all the points from the OSM tables
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tagKeys list ok keys to be filtered
 *
 * @return outputTableName a name for the table that contains all points
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
IProcess toPoints() {
    return create({
            title "Transform all OSM features as points"
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: -1, tagKeys: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tagKeys ->
                String outputTableName = "OSM_POINTS_$uuid"
                if (datasource != null) {
                    if(epsgCode!=-1) {
                        info "Start points transformation"
                        info "Indexing osm tables..."
                        buildIndexes(datasource, osmTablesPrefix)
                        def pointsNodes = extractNodesAsPoints(datasource, osmTablesPrefix, epsgCode, outputTableName, tagKeys)
                        if (pointsNodes) {
                            info "The points have been built."
                        } else {
                            info "Cannot extract any point."
                            outputTableName = null
                        }
                    }
                    else{
                        error "Invalid EPSG code : $epsg"
                        return
                    }
                } else {
                    error "Please set a valid database connection"
                }
                [outputTableName: outputTableName]
            }
        })
}

/**
 * This process is used to extract all the lines from the OSM tables
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tagKeys list ok keys to be filtered
 *
 * @return outputTableName a name for the table that contains all lines
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess toLines() {
    return create({
            title "Transform all OSM features as lines"
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: -1, tagKeys: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tagKeys ->
                String outputTableName = "OSM_LINES_$uuid"
                if (datasource != null) {
                    if(epsgCode!=-1){
                        info "Start lines transformation"
                        info "Indexing osm tables..."
                        buildIndexes(datasource, osmTablesPrefix)
                        IProcess lineWaysProcess = extractWaysAsLines()
                        lineWaysProcess.execute(datasource:datasource, osmTablesPrefix:osmTablesPrefix, epsgCode:epsgCode, tagKeys:tagKeys)
                        def outputWayLines = lineWaysProcess.getResults().outputTableName

                        IProcess lineRelationsProcess = extractRelationsAsLines()
                        lineRelationsProcess.execute(datasource:datasource, osmTablesPrefix:osmTablesPrefix, epsgCode:epsgCode, tagKeys:tagKeys)
                        def outputRelationLines = lineRelationsProcess.getResults().outputTableName
                        if (outputWayLines!=null && outputRelationLines!=null) {
                            //Merge ways and relations
                            def columnsWays = datasource.getTable(outputWayLines).columnNames
                            def columnsRelations = datasource.getTable(outputRelationLines).columnNames
                            def allColumns = []
                            allColumns.addAll(columnsWays)
                            allColumns.removeAll(columnsRelations)
                            allColumns.addAll(columnsRelations)
                            allColumns.sort()
                            def leftSelect = ""
                            def rightSelect = ""
                            allColumns.each { iter ->
                                if (columnsWays.contains(iter)) {
                                    leftSelect += "\"${iter}\","
                                } else {
                                    leftSelect += "null as \"${iter}\","
                                }

                                if (columnsRelations.contains(iter)) {
                                    rightSelect += "\"${iter}\","

                                } else {
                                    rightSelect += "null as \"${iter}\","
                                }
                            }
                            leftSelect = leftSelect[0..-2]
                            rightSelect = rightSelect[0..-2]

                            datasource.execute """DROP TABLE IF EXISTS ${outputTableName};
                        CREATE TABLE ${outputTableName} AS 
                        SELECT  ${leftSelect} from ${outputWayLines} 
                        union all 
                        select  ${rightSelect} from ${outputRelationLines};
                        DROP TABLE IF EXISTS ${outputWayLines}, ${outputRelationLines};"""
                            info "The way and relation lines have been built."
                        } else if (outputWayLines!=null) {
                            datasource.execute "ALTER TABLE $outputWayLines RENAME TO $outputTableName"
                            info "The way lines have been built."
                        } else if (outputRelationLines) {
                            datasource.execute "ALTER TABLE $outputRelationLines RENAME TO $outputTableName"
                            info "The relation lines have been built."
                        } else {
                            info "Cannot extract any line."
                            outputTableName = null
                        }
                    }
                    else{
                        error "Invalid EPSG code : $epsg"
                        return
                    }
                } else {
                    error "Please set a valid database connection"
                }
                [outputTableName: outputTableName]
            }
        })
}


/**
 * This process is used to extract all the polygons from the OSM tables
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tagKeys list ok keys to be filtered
 * @return outputTableName a name for the table that contains all polygons
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 */
IProcess toPolygons() {
    return create({
            title "Transform all OSM features as polygons"
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: -1, tagKeys: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tagKeys ->
                String outputTableName = "OSM_POLYGONS_$uuid"
                if (datasource != null) {
                    if(epsgCode!=-1){
                        info "Start polygon transformation"
                        info "Indexing osm tables..."
                        buildIndexes(datasource, osmTablesPrefix)
                        IProcess polygonWaysProcess = extractWaysAsPolygons()
                        polygonWaysProcess.execute(datasource:datasource, osmTablesPrefix:osmTablesPrefix, epsgCode:epsgCode, tagKeys :tagKeys)
                        outputWayPolygons = polygonWaysProcess.getResults().outputTableName

                        IProcess polygonRelationsProcess = extractRelationsAsPolygons()
                        polygonRelationsProcess.execute(datasource:datasource, osmTablesPrefix:osmTablesPrefix, epsgCode:epsgCode, tagKeys :tagKeys)
                        outputRelationsPolygons = polygonRelationsProcess.getResults().outputTableName

                        if (outputWayPolygons!=null && outputRelationsPolygons!=null) {
                            //Merge ways and relations
                            def columnsWays = datasource.getTable(outputWayPolygons).columnNames
                            def columnsRelations = datasource.getTable(outputRelationPolygons).columnNames
                            def allColumns = []
                            allColumns.addAll(columnsWays)
                            allColumns.removeAll(columnsRelations)
                            allColumns.addAll(columnsRelations)
                            allColumns.sort()
                            def leftSelect = ""
                            def rightSelect = ""
                            allColumns.each { iter ->
                                if (columnsWays.contains(iter)) {
                                    leftSelect += "\"${iter}\","
                                } else {
                                    leftSelect += "null as \"${iter}\","
                                }

                                if (columnsRelations.contains(iter)) {
                                    rightSelect += "\"${iter}\","

                                } else {
                                    rightSelect += "null as \"${iter}\","
                                }
                            }
                            leftSelect = leftSelect[0..-2]
                            rightSelect = rightSelect[0..-2]

                            datasource.execute """DROP TABLE IF EXISTS ${outputTableName};
                        CREATE TABLE ${outputTableName} AS 
                        SELECT  ${leftSelect} from ${outputWayPolygons} 
                        union all 
                        select  ${rightSelect} from ${outputRelationPolygons};
                        DROP TABLE IF EXISTS ${outputWayPolygons}, ${outputRelationPolygons};"""
                            info "The way and relation polygons have been built."
                        } else if (outputWayPolygons!=null ){
                            datasource.execute "ALTER TABLE $outputWayPolygons RENAME TO $outputTableName"
                            info "The way polygons have been built."
                        } else if (outputRelationsPolygons!=null) {
                            datasource.execute "ALTER TABLE $outputRelationPolygons RENAME TO $outputTableName"
                            info "The relation polygons have been built."
                        } else {
                            info "Cannot extract any polygon."
                            outputTableName = null
                        }
                    }else{
                        error "Invalid EPSG code : $epsg"
                        return
                    }
                } else {
                    error "Please set a valid database connection"
                    return
                }
                [outputTableName: outputTableName]
            }
        })
}

/**
 * This process is used to extract ways as polygons
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tagKeys list ok keys to be filtered
 *
 * @return outputTableName a name for the table that contains all ways transformed as polygons
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess extractWaysAsPolygons() {
    return create({
            title "Transform all OSM ways as polygons"
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: -1, tagKeys: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tagKeys ->
                if (datasource != null) {
                    if(epsgCode!=-1) {
                        outputTableName = "WAYS_POLYGONS_$uuid"
                        def counttagKeysQuery = "select count(*) as count from ${osmTablesPrefix}_way_tag"
                        def filterByKeys = false
                        def whereKeysFilter = ""
                        if (tagKeys != null && !tagKeys.isEmpty()) {
                            whereKeysFilter = "tag_key in ('${tagKeys.join("','")}')"
                            counttagKeysQuery += " where ${whereKeysFilter}"
                            filterByKeys = true
                        }
                        def rows = datasource.firstRow(counttagKeysQuery)
                        if (rows.count > 0) {
                            info "Build way polygons"
                            def WAYS_POLYGONS_TMP = "WAYS_POLYGONS_TMP$uuid"
                            datasource.execute "DROP TABLE IF EXISTS ${WAYS_POLYGONS_TMP};"
                            //Create polygons from ways
                            def filter = ""
                            def caseWhenFilter = """select distinct a.tag_key as tag_key from ${osmTablesPrefix}_way_tag as a, ${WAYS_POLYGONS_TMP} as b
            where a.id_way=b.id_way """

                            if (filterByKeys) {
                                filter = """, (SELECT DISTINCT id_way FROM ${osmTablesPrefix}_way_tag wt WHERE ${whereKeysFilter}) b WHERE w.id_way = b.id_way"""
                                caseWhenFilter += "and ${whereKeysFilter}"
                            }

                            datasource.execute """CREATE TABLE ${WAYS_POLYGONS_TMP} AS 
        SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKEPOLYGON(ST_MAKELINE(the_geom)), 4326), ${epsgCode}) as the_geom, id_way
        FROM  (SELECT (SELECT ST_ACCUM(the_geom) as the_geom FROM  
        (SELECT n.id_node, n.the_geom, wn.id_way idway FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn WHERE n.id_node = wn.id_node ORDER BY wn.node_order)
        WHERE  idway = w.id_way) the_geom ,w.id_way  
        FROM ${osmTablesPrefix}_way w ${filter}) geom_table
        WHERE ST_GEOMETRYN(the_geom, 1) = ST_GEOMETRYN(the_geom, ST_NUMGEOMETRIES(the_geom)) AND ST_NUMGEOMETRIES(the_geom) > 3;
        create index on ${WAYS_POLYGONS_TMP}(id_way);
        """

                            def rowskeys = datasource.rows(caseWhenFilter)

                            def list = []
                            rowskeys.tag_key.each { iter ->
                                list << "MAX(CASE WHEN b.tag_key = '${iter}' then b.tag_value END) as \"${iter}\""
                            }
                            def tagList = ""
                            if (!list.isEmpty()) {
                                tagList = ", ${list.join(",")}"
                            }

                            datasource.execute """drop table if exists ${outputTableName}; 
        CREATE TABLE ${outputTableName} AS SELECT 'w'||a.id_way as id, a.the_geom ${tagList} from 
        ${WAYS_POLYGONS_TMP} as a, ${osmTablesPrefix}_way_tag  b where a.id_way=b.id_way group by a.id_way;"""

                            datasource.execute "DROP TABLE IF EXISTS ${WAYS_POLYGONS_TMP};"

                        }
                        else {
                            info "No keys or values found to extract ways."
                            outputTableName = null
                        }
                    }else{
                        error "Invalid EPSG code : $epsg"
                        return
                    }
                } else {
                    error "Please set a valid database connection"
                    return
                }

                [outputTableName: outputTableName]
            }
        })
}

/**
 * This process is used to extract relations as polygons
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tagKeys list ok keys to be filtered
 *
 * @return outputTableName a name for the table that contains all relations transformed as polygons
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess extractRelationsAsPolygons() {
    create({
            title "Transform all OSM ways as polygons"
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: -1, tagKeys: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tagKeys ->
                if (datasource != null) {
                    if(epsgCode!=-1){
                        outputTableName = "RELATION_POLYGONS_$uuid"

                        def counttagKeysQuery = "select count(*) as count from ${osmTablesPrefix}_relation_tag"
                        def filterByKeys = false
                        def whereKeysFilter =''
                        if (tagKeys != null && !tagKeys.isEmpty()) {
                            whereKeysFilter = "tag_key in ('${tagKeys.join("','")}')"
                            counttagKeysQuery += " where " + whereKeysFilter
                            filterByKeys = true
                        }
                        def rows = datasource.firstRow(counttagKeysQuery)
                        if (rows.count > 0) {
                            info "Build outer polygons"
                            String RELATIONS_POLYGONS_OUTER = "RELATIONS_POLYGONS_OUTER_$uuid"
                            String RELATION_FILTERED_KEYS = "RELATION_FILTERED_KEYS_$uuid"
                            def outer_condition
                            def inner_condition
                            if (filterByKeys) {
                                datasource.execute """ DROP TABLE IF EXISTS ${RELATION_FILTERED_KEYS};
                CREATE TABLE ${RELATION_FILTERED_KEYS} as SELECT DISTINCT id_relation  FROM ${osmTablesPrefix}_relation_tag wt 
                WHERE ${whereKeysFilter};
                CREATE INDEX ON ${RELATION_FILTERED_KEYS}(id_relation);"""
                                outer_condition = """, ${RELATION_FILTERED_KEYS} g
                    WHERE br.id_relation=g.id_relation AND w.id_way = br.id_way AND br.role='outer'"""
                                inner_condition = """, ${RELATION_FILTERED_KEYS} g
                        WHERE br.id_relation=g.id_relation AND w.id_way = br.id_way and br.role='inner'"""
                            } else {
                                outer_condition = "WHERE w.id_way = br.id_way and br.role='outer'"
                                inner_condition = "WHERE w.id_way = br.id_way and br.role='inner'"
                            }
                            datasource.execute """DROP TABLE IF EXISTS ${RELATIONS_POLYGONS_OUTER};
            CREATE TABLE ${RELATIONS_POLYGONS_OUTER} AS 
            SELECT ST_LINEMERGE(st_union(ST_ACCUM(the_geom))) the_geom, id_relation 
            FROM(
                SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), ${epsgCode}) the_geom, id_relation, role, id_way 
                FROM(
                    SELECT(
                        SELECT ST_ACCUM(the_geom) the_geom 
                        FROM(
                            SELECT n.id_node, n.the_geom, wn.id_way idway 
                            FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
                            WHERE n.id_node = wn.id_node ORDER BY wn.node_order) 
                        WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role 
                    FROM ${osmTablesPrefix}_way w, ${osmTablesPrefix}_way_member br ${outer_condition}) geom_table
                    WHERE st_numgeometries(the_geom)>=2) 
            GROUP BY id_relation;"""

                            info "Build inner polygons"
                            def RELATIONS_POLYGONS_INNER = "RELATIONS_POLYGONS_INNER_$uuid"
                            datasource.execute """DROP TABLE IF EXISTS ${RELATIONS_POLYGONS_INNER};
            CREATE TABLE ${RELATIONS_POLYGONS_INNER} AS 
            SELECT ST_LINEMERGE(st_union(ST_ACCUM(the_geom))) the_geom, id_relation 
            FROM(
                SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), ${epsgCode}) the_geom, id_relation, role, id_way 
                FROM(     
                    SELECT(
                        SELECT ST_ACCUM(the_geom) the_geom 
                        FROM(
                            SELECT n.id_node, n.the_geom, wn.id_way idway 
                            FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
                            WHERE n.id_node = wn.id_node ORDER BY wn.node_order) 
                        WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role 
                    FROM ${osmTablesPrefix}_way w, ${osmTablesPrefix}_way_member br ${inner_condition}) geom_table 
                    where st_numgeometries(the_geom)>=2) 
            GROUP BY id_relation;"""

                            info "Explode outer polygons"
                            def RELATIONS_POLYGONS_OUTER_EXPLODED = "RELATIONS_POLYGONS_OUTER_EXPLODED_$uuid"
                            datasource.execute """
            DROP TABLE IF EXISTS ${RELATIONS_POLYGONS_OUTER_EXPLODED};
            CREATE TABLE ${RELATIONS_POLYGONS_OUTER_EXPLODED} AS 
                SELECT ST_MAKEPOLYGON(the_geom) as the_geom, id_relation 
                FROM st_explode('${RELATIONS_POLYGONS_OUTER}') 
                WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom); """

                            info "Explode inner polygons"
                            def RELATIONS_POLYGONS_INNER_EXPLODED = "RELATIONS_POLYGONS_INNER_EXPLODED_$uuid"
                            datasource.execute """ DROP TABLE IF EXISTS ${RELATIONS_POLYGONS_INNER_EXPLODED};
            CREATE TABLE ${RELATIONS_POLYGONS_INNER_EXPLODED} AS 
            SELECT the_geom as the_geom, id_relation 
            FROM st_explode('${RELATIONS_POLYGONS_INNER}') 
            WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom); """

                            info "Build all polygon relations"
                            String RELATIONS_MP_HOLES = "RELATIONS_MP_HOLES_$uuid"
                            datasource.execute """CREATE INDEX ON ${RELATIONS_POLYGONS_OUTER_EXPLODED} (the_geom) USING RTREE;
            CREATE INDEX ON ${RELATIONS_POLYGONS_INNER_EXPLODED} (the_geom) USING RTREE;
            CREATE INDEX ON ${RELATIONS_POLYGONS_OUTER_EXPLODED}(id_relation);
            CREATE INDEX ON ${RELATIONS_POLYGONS_INNER_EXPLODED}(id_relation);       
            DROP TABLE IF EXISTS ${RELATIONS_MP_HOLES};
            CREATE TABLE ${RELATIONS_MP_HOLES} AS (SELECT ST_MAKEPOLYGON(ST_EXTERIORRING(a.the_geom), ST_ACCUM(b.the_geom)) AS the_geom, a.ID_RELATION FROM
            ${RELATIONS_POLYGONS_OUTER_EXPLODED} AS a LEFT JOIN ${RELATIONS_POLYGONS_INNER_EXPLODED} AS b on (a.the_geom && b.the_geom AND 
            st_contains(a.the_geom, b.THE_GEOM) AND a.ID_RELATION=b.ID_RELATION) GROUP BY a.the_geom, a.id_relation) union  
            (select a.the_geom, a.ID_RELATION from ${RELATIONS_POLYGONS_OUTER_EXPLODED} as a left JOIN  ${RELATIONS_POLYGONS_INNER_EXPLODED} as b 
            on a.id_relation=b.id_relation WHERE b.id_relation IS NULL);
            CREATE INDEX ON ${RELATIONS_MP_HOLES}(id_relation);"""


                            def caseWhenQuery ="""select distinct a.tag_key as tag_key from ${osmTablesPrefix}_relation_tag as a, ${
                                RELATIONS_MP_HOLES
                            } as b 
                 where a.id_relation=b.id_relation """

                            if (filterByKeys) {
                                caseWhenQuery += " and ${whereKeysFilter}"
                            }

                            def rowskeys = datasource.rows(caseWhenQuery)

                            def list = []
                            rowskeys.tag_key.each { iter ->
                                list << "MAX(CASE WHEN b.tag_key = '${iter}' then b.tag_value END) as \"${iter}\""
                            }
                            def tagList = ""
                            if (!list.isEmpty()) {
                                tagList = ", ${list.join(",")}"
                            }
                            datasource.execute """
            DROP TABLE IF EXISTS ${outputRelationPolygons};     
            CREATE TABLE ${outputRelationPolygons} AS SELECT 'r'||a.id_relation as id, a.the_geom ${tagList}
            from ${RELATIONS_MP_HOLES} as a, ${osmTablesPrefix}_relation_tag  b 
            where a.id_relation=b.id_relation group by a.the_geom, a.id_relation;
            """

                            datasource.execute """DROP TABLE IF EXISTS ${RELATIONS_POLYGONS_OUTER}, ${RELATIONS_POLYGONS_INNER},
           ${RELATIONS_POLYGONS_OUTER_EXPLODED}, ${RELATIONS_POLYGONS_INNER_EXPLODED}, ${RELATIONS_MP_HOLES}, ${RELATION_FILTERED_KEYS};"""

                        } else {
                            info "No keys or values found in the relations."
                            outputTableName = null
                        }
                    }else{
                        error "Invalid EPSG code : $epsg"
                        return
                    }
                } else {
                    error "Please set a valid database connection"
                    return
                }

                [outputTableName: outputTableName]
            }
        })
}





/**
 * This process is used to extract ways as lines
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tagKeys list ok keys to be filtered
 *
 * @return outputTableName a name for the table that contains all ways transformed as lines
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
IProcess extractWaysAsLines() {
    return create({
            title "Transform all OSM ways as lines"
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: -1, tagKeys: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tagKeys ->
                if (datasource != null) {
                    if(epsgCode!=-1){
                        outputTableName = "WAYS_LINES_$uuid"
                        def countTagKeysQuery = "select count(*) as count from ${osmTablesPrefix}_way_tag"
                        def filterByKeys = false
                        def whereKeysFilter = ""
                        if (tagKeys != null && !tagKeys.isEmpty()) {
                            whereKeysFilter = "tag_key in ('${tagKeys.join("','")}')"
                            countTagKeysQuery += " where ${whereKeysFilter}"
                            filterByKeys = true
                        }
                        def rows = datasource.firstRow(countTagKeysQuery)
                        if (rows.count > 0) {
                            info "Build ways as lines"
                            String WAYS_LINES_TMP = "WAYS_LINES_TMP_$uuid"

                            def caseWhenFilter = """select distinct a.tag_key as tag_key from ${osmTablesPrefix}_way_tag as a,
            ${WAYS_LINES_TMP} as b where a.id_way=b.id_way """
                            def filter = ""

                            if (filterByKeys) {
                                caseWhenFilter += "and ${whereKeysFilter}"
                                filter = "WHERE ${whereKeysFilter}"
                            }
                            datasource.execute """DROP TABLE IF EXISTS ${WAYS_LINES_TMP}; 
        CREATE TABLE  ${WAYS_LINES_TMP} AS SELECT id_way,ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(THE_GEOM), 4326), 
        ${epsgCode}) the_geom FROM 
        (SELECT (SELECT ST_ACCUM(the_geom) the_geom FROM (SELECT n.id_node, n.the_geom, wn.id_way idway FROM 
        ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
        WHERE n.id_node = wn.id_node ORDER BY wn.node_order) 
        WHERE idway = w.id_way) the_geom, w.id_way  
        FROM ${osmTablesPrefix}_way w, (SELECT DISTINCT id_way 
        FROM ${osmTablesPrefix}_way_tag wt
        ${filter}) b WHERE w.id_way = b.id_way) geom_table 
        WHERE ST_NUMGEOMETRIES(the_geom) >= 2;
        CREATE INDEX ON ${WAYS_LINES_TMP}(ID_WAY);"""

                            def rowskeys = datasource.rows(caseWhenFilter)

                            def list = []
                            rowskeys.tag_key.each { iter ->
                                list << "MAX(CASE WHEN b.tag_key = '${iter}' then b.tag_value END) as \"${iter}\""
                            }
                            def tagList = ""
                            if (!list.isEmpty()) {
                                tagList = ", ${list.join(",")}"
                            }
                            datasource.execute """drop table if exists ${outputTableName};
            CREATE TABLE ${outputTableName} AS SELECT 'w'||a.id_way as id, a.the_geom ${tagList} 
            from ${WAYS_LINES_TMP} as a, ${osmTablesPrefix}_way_tag  b where a.id_way=b.id_way group by a.id_way;
            DROP TABLE IF EXISTS ${WAYS_LINES_TMP};"""
                        } else {
                            info "No keys or values found in the ways."
                            outputTableName = null
                        }
                    }
                    else{
                        error "Invalid EPSG code : $epsg"
                        return
                    }
                } else {
                    error "Please set a valid database connection"
                    return
                }
                [outputTableName: outputTableName]
            }
        })
}


/**
 * This process is used to extract relations as lines
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tagKeys list ok keys to be filtered
 *
 * @return outputTableName a name for the table that contains all relations transformed as lines
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
IProcess extractRelationsAsLines() {
    return create({
            title "Transform all OSM ways as lines"
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: -1, tagKeys: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tagKeys ->
                if (datasource != null) {
                    if(epsgCode!=-1){
                        outputTableName = "RELATIONS_LINES_$uuid"
                        def counttagKeysQuery = "select count(*) as count from ${osmTablesPrefix}_relation_tag"
                        def filterByKeys = false
                        def whereKeysFilter =''
                        if (tagKeys != null && !tagKeys.isEmpty()) {
                            whereKeysFilter = "tag_key in ('${tagKeys.join("','")}')"
                            counttagKeysQuery += " where " + whereKeysFilter
                            filterByKeys = true
                        }
                        def rows = datasource.firstRow(counttagKeysQuery)
                        if (rows.count> 0) {
                            String RELATIONS_LINES_TMP = "RELATIONS_LINES_TMP_$uuid"
                            String RELATION_FILTERED_KEYS = "RELATION_FILTERED_KEYS_$uuid"

                            def caseWhenFilter = """select distinct a.tag_key as tag_key from ${osmTablesPrefix}_relation_tag as a,
            ${RELATIONS_LINES_TMP} as b where a.id_relation=b.id_relation """

                            if (filterByKeys) {
                                datasource.execute """ DROP TABLE IF EXISTS ${RELATION_FILTERED_KEYS};
                CREATE TABLE ${RELATION_FILTERED_KEYS} as SELECT DISTINCT id_relation  FROM ${osmTablesPrefix}_relation_tag wt 
                WHERE ${whereKeysFilter};
                CREATE INDEX ON ${RELATION_FILTERED_KEYS}(id_relation);"""
                                caseWhenFilter += "and ${whereKeysFilter}"
                            }
                            else{
                                RELATION_FILTERED_KEYS = "${osmTablesPrefix}_relation"
                            }

                            datasource.execute """ DROP TABLE IF EXISTS ${RELATIONS_LINES_TMP};
        CREATE TABLE ${RELATIONS_LINES_TMP} AS SELECT ST_ACCUM(THE_GEOM) AS the_geom, id_relation from
        (SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), ${epsgCode}) the_geom, id_relation, id_way
        FROM(
                SELECT(
                        SELECT ST_ACCUM(the_geom) the_geom
                        FROM(
                                SELECT n.id_node, n.the_geom, wn.id_way idway
                                FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn
                                WHERE n.id_node = wn.id_node ORDER BY wn.node_order)
                        WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation
                FROM ${osmTablesPrefix}_way w, (SELECT br.id_way, g.ID_RELATION FROM  ${osmTablesPrefix}_way_member br , ${RELATION_FILTERED_KEYS} g 
           WHERE br.id_relation=g.id_relation) br
                WHERE w.id_way = br.id_way) geom_table
        where st_numgeometries(the_geom)>=2) GROUP BY id_relation;
        CREATE INDEX on ${RELATIONS_LINES_TMP}(id_relation);"""


                            def rowskeys = datasource.rows(caseWhenFilter)

                            def list = []
                            rowskeys.tag_key.each { iter ->
                                list << "MAX(CASE WHEN b.tag_key = '${iter}' then b.tag_value END) as \"${iter}\""
                            }
                            def tagList = ""
                            if (!list.isEmpty()) {
                                tagList = ", ${list.join(",")}"
                            }
                            datasource.execute """drop table if exists ${outputTableName};
            CREATE TABLE ${outputTableName} AS SELECT 'r'||a.id_relation as id, a.the_geom ${tagList} 
            from ${RELATIONS_LINES_TMP} as a, ${osmTablesPrefix}_relation_tag  b where a.id_relation=b.id_relation group by a.id_relation;
            DROP TABLE IF EXISTS ${RELATIONS_LINES_TMP}, ${RELATION_FILTERED_KEYS};"""

                        }
                        else {
                            info "No keys or values found in the relations."
                            outputTableName = null
                        }
                    }
                    else{
                        error "Invalid EPSG code : $epsg"
                        return
                    }
                } else {
                    error "Please set a valid database connection"
                    return
                }
                [outputTableName: outputTableName]
            }
        })
}

/**
 * This function is used to extract relation id and way id plus there tags
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param outputRelationsLines the name of the relation lines table
 * @param tagKeys list ok keys to be filtered
 *
 * @return true if some lines have been built
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
static boolean extractRelationsIdWays(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode,
                                      String outputRelationsLines, def tagKeys) {

    def counttagKeysQuery = "select count(*) as count from ${osmTablesPrefix}_relation_tag"
    def filterByKeys = false
    def whereKeysFilter =''
    if (tagKeys != null && !tagKeys.isEmpty()) {
        whereKeysFilter = "tag_key in ('${tagKeys.join("','")}')"
        counttagKeysQuery += " where " + whereKeysFilter
        filterByKeys = true
    }
    def rows = datasource.firstRow(counttagKeysQuery)
    if (rows.count> 0) {
        String RELATIONS_IDWAYS_TMP = "RELATIONS_LINES_TMP_$uuid"

        def caseWhenFilter = """select distinct a.tag_key as tag_key from ${osmTablesPrefix}_relation_tag as a,
            ${RELATIONS_IDWAYS_TMP} as b where a.id_relation=b.id_relation """


        if (filterByKeys) {
            datasource.execute """ DROP TABLE IF EXISTS ${RELATIONS_IDWAYS_TMP};
        CREATE TABLE ${RELATIONS_IDWAYS_TMP} AS 
        SELECT br.id_way, g.ID_RELATION FROM  ${osmTablesPrefix}_way_member br , (SELECT DISTINCT id_relation  FROM ${osmTablesPrefix}_relation_tag wt 
                WHERE ${whereKeysFilter}) g 
           WHERE br.id_relation=g.id_relation;
           CREATE INDEX ON ${RELATIONS_IDWAYS_TMP}(id_way); """
            caseWhenFilter += "and ${whereKeysFilter}"
        }
        else{
            datasource.execute """ DROP TABLE IF EXISTS ${RELATIONS_IDWAYS_TMP};
        CREATE TABLE ${RELATIONS_IDWAYS_TMP} AS 
        SELECT br.id_way, g.ID_RELATION FROM  ${osmTablesPrefix}_way_member br , ${osmTablesPrefix}_relation g 
           WHERE br.id_relation=g.id_relation;
           CREATE INDEX ON ${RELATIONS_IDWAYS_TMP}(id_way); """
        }
        def rowskeys = datasource.rows(caseWhenFilter)

        def list = []
        rowskeys.tag_key.each { it ->
            list << "MAX(CASE WHEN b.tag_key = '${it}' then b.tag_value END) as \"${it}\""
        }
        def tagList = ""
        if (!list.isEmpty()) {
            tagList = ", ${list.join(",")}"
        }
        datasource.execute """drop table if exists ${outputRelationsLines};
            CREATE TABLE ${outputRelationsLines} AS SELECT a.id_relation, a.id_way ${tagList} 
            from ${RELATIONS_IDWAYS_TMP} as a, ${osmTablesPrefix}_relation_tag  b where a.id_relation=b.id_relation group by a.id_way;
            DROP TABLE IF EXISTS ${RELATIONS_IDWAYS_TMP};"""
        return true

    }
    else {
        info "No keys or values found in the relations"
        return false
    }
}


/**
 * This function is used to extract nodes as points
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param outputNodesPoints the name of the nodes points table
 *
 * @return true if some points have been built
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
static boolean extractNodesAsPoints(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode, String outputNodesPoints, def tagKeys) {
    def countTagKeysQuery = "select count(*) as count from ${osmTablesPrefix}_node_tag"
    def filterByKeys = false
    def whereKeysFilter =""
    if (tagKeys != null && !tagKeys.isEmpty()) {
        whereKeysFilter = "tag_key in ('${tagKeys.join("','")}')"
        countTagKeysQuery += " where ${whereKeysFilter}"
        filterByKeys = true
    }
    def rows = datasource.firstRow(countTagKeysQuery)
    if (rows.count > 0) {
        info "Build nodes as points"

        def caseWhenFilter = """select distinct tag_key as tag_key from ${osmTablesPrefix}_node_tag """
        def filter =""
        if (filterByKeys) {
            caseWhenFilter += "where ${whereKeysFilter}"
            filter = "and b.${whereKeysFilter}"
        }
        def rowskeys = datasource.rows(caseWhenFilter)

        def list = []
        rowskeys.tag_key.each { it ->
            list << "MAX(CASE WHEN b.tag_key = '${it}' then b.tag_value END) as \"${it}\""
        }
        def tagList =""
        if (!list.isEmpty()) {
            tagList = ", ${list.join(",")}"
        }

        datasource.execute """drop table if exists ${outputNodesPoints}; 
                    CREATE TABLE ${outputNodesPoints} AS SELECT a.id_node,ST_TRANSFORM(ST_SETSRID(a.THE_GEOM, 4326), 
                    ${epsgCode}) as the_geom ${tagList} from ${osmTablesPrefix}_node as a, 
                    ${osmTablesPrefix}_node_tag  b where a.id_node=b.id_node ${filter} group by a.id_node;"""
        return true
    } else {
        info("No keys or values found in the nodes")
        return false
    }
}


/**
 * Build the indexes to perform analysis quicker
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 *
 * @return
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
static def buildIndexes(JdbcDataSource datasource, String osmTablesPrefix){
    datasource.execute "CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_node_index on ${osmTablesPrefix}_node(id_node);"+
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