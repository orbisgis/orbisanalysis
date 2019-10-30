package org.orbisgis.osm

import groovy.transform.BaseScript
import org.h2gis.utilities.SFSUtilities
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Polygon
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

import static org.orbisgis.osm.utils.OSMElement.NODE
import static org.orbisgis.osm.utils.OSMElement.RELATION
import static org.orbisgis.osm.utils.OSMElement.WAY


@BaseScript OSMTools osmTools


/**
 * This process is used to extract all the points from the OSM tables
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all points
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
IProcess toPoints() {
    return create({
            title "Transform all OSM features as points"
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: -1, tags: [], columnsToKeep: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep ->
                String outputTableName = "OSM_POINTS_$uuid"
                if (datasource != null) {
                    if(epsgCode!=-1) {
                        info "Start points transformation"
                        info "Indexing osm tables..."
                        buildIndexes(datasource, osmTablesPrefix)
                        def pointsNodes = extractNodesAsPoints(datasource, osmTablesPrefix, epsgCode, outputTableName, tags, columnsToKeep)
                        if (pointsNodes) {
                            info "The points have been built."
                        } else {
                            info "Cannot extract any point."
                            return null
                        }
                    }
                    else{
                        error "Invalid EPSG code : $epsgCode"
                        return null
                    }
                } else {
                    error "Please set a valid database connection"
                    return null
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
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all lines
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess toLines() {
    return create({
            title "Transform all OSM features as lines"
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: -1, tags : [], columnsToKeep:[]
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep ->
                String outputTableName = "OSM_LINES_$uuid"
                if (datasource != null) {
                    if(epsgCode!=-1){
                        info "Start lines transformation"
                        info "Indexing osm tables..."
                        buildIndexes(datasource, osmTablesPrefix)
                        IProcess lineWaysProcess = extractWaysAsLines()
                        lineWaysProcess.execute(datasource:datasource, osmTablesPrefix:osmTablesPrefix, epsgCode:epsgCode, tags : tags, columnsToKeep:columnsToKeep)
                        def outputWayLines = lineWaysProcess.getResults().outputTableName

                        IProcess lineRelationsProcess = extractRelationsAsLines()
                        lineRelationsProcess.execute(datasource:datasource, osmTablesPrefix:osmTablesPrefix, epsgCode:epsgCode, tags : tags, columnsToKeep:columnsToKeep)
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
                        } else if (outputRelationLines!=null) {
                            datasource.execute "ALTER TABLE $outputRelationLines RENAME TO $outputTableName"
                            info "The relation lines have been built."
                        } else {
                            info "Cannot extract any line."
                            return null
                        }
                    }
                    else{
                        error "Invalid EPSG code : $epsgCode"
                        return null
                    }
                } else {
                    error "Please set a valid database connection"
                    return null
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
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all polygons
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 */
IProcess toPolygons() {
    return create({
            title "Transform all OSM features as polygons"
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: -1, tags: [], columnsToKeep: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep ->
                String outputTableName = "OSM_POLYGONS_$uuid"
                if (datasource != null) {
                    if(epsgCode!=-1){
                        info "Start polygon transformation"
                        info "Indexing osm tables..."
                        buildIndexes(datasource, osmTablesPrefix)
                        IProcess polygonWaysProcess = extractWaysAsPolygons()
                        polygonWaysProcess.execute(datasource:datasource, osmTablesPrefix:osmTablesPrefix, epsgCode:epsgCode, tags : tags, columnsToKeep:columnsToKeep)
                        def outputWayPolygons = polygonWaysProcess.getResults().outputTableName

                        IProcess polygonRelationsProcess = extractRelationsAsPolygons()
                        polygonRelationsProcess.execute(datasource:datasource, osmTablesPrefix:osmTablesPrefix, epsgCode:epsgCode, tags : tags, columnsToKeep:columnsToKeep)
                        def outputRelationPolygons = polygonRelationsProcess.getResults().outputTableName

                        if (outputWayPolygons!=null && outputRelationPolygons!=null) {
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
                        } else if (outputRelationPolygons!=null) {
                            datasource.execute "ALTER TABLE $outputRelationPolygons RENAME TO $outputTableName"
                            info "The relation polygons have been built."
                        } else {
                            info "Cannot extract any polygon."
                            return null
                        }
                    }else{
                        error "Invalid EPSG code : $epsgCode"
                        return null
                    }
                } else {
                    error "Please set a valid database connection"
                    return null
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
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all ways transformed as polygons
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess extractWaysAsPolygons() {
    return create({
            title "Transform all OSM ways as polygons"
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: -1, tags: [], columnsToKeep: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep ->
                if (datasource != null) {
                    if(epsgCode!=-1) {
                        outputTableName = "WAYS_POLYGONS_$uuid"
                        def IDWAYS_TABLE = "ID_WAYS_POLYGONS_$uuid"
                        def countTagsQuery = "select count(*) as count from ${osmTablesPrefix}_way_tag"
                        def columnsSelector = """select distinct tag_key as tag_key from ${osmTablesPrefix}_way_tag """
                        def tagsFilter =''
                        if (tags != null && !tags.isEmpty()) {
                            def tagKeysList = []
                            if(tags in Map) {
                                tagKeysList = tags.keySet().findResults { it != "null" && !it.isEmpty() ? it : null }
                            }
                            else{
                                tagKeysList = tags.findResults { it != "null" && !it.isEmpty() ? it : null }
                            }
                            tagsFilter = createWhereFilter(tags)
                            countTagsQuery += " where ${tagsFilter}"
                            if(columnsToKeep!=null){
                                columnsSelector += " where tag_key in ('${(tagKeysList + columnsToKeep).unique().join("','")}')"
                            }
                        }
                        else if(columnsToKeep!=null&&!columnsToKeep.isEmpty()){
                            columnsSelector += "tag_key in ('${columnsToKeep.unique().join("','")}')"
                        }
                        if (datasource.firstRow(countTagsQuery).count>0) {
                            info "Build way polygons"
                            def WAYS_POLYGONS_TMP = "WAYS_POLYGONS_TMP$uuid"
                            datasource.execute "DROP TABLE IF EXISTS ${WAYS_POLYGONS_TMP};"

                            if(!tagsFilter.isEmpty()){
                                datasource.execute """ DROP TABLE IF EXISTS $IDWAYS_TABLE;
                    CREATE TABLE $IDWAYS_TABLE AS SELECT DISTINCT id_way FROM ${osmTablesPrefix}_way_tag WHERE ${tagsFilter};
                    CREATE INDEX ON $IDWAYS_TABLE(id_way);"""
                            }
                            else{
                                IDWAYS_TABLE = "${osmTablesPrefix}_way_tag"
                            }

                            datasource.execute """CREATE TABLE ${WAYS_POLYGONS_TMP} 
        AS  SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKEPOLYGON(ST_MAKELINE(the_geom)), 4326), ${epsgCode}) as the_geom, id_way
        FROM  (SELECT (SELECT ST_ACCUM(the_geom) as the_geom FROM  
        (SELECT n.id_node, n.the_geom, wn.id_way idway FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn WHERE n.id_node = wn.id_node ORDER BY wn.node_order)
        WHERE  idway = w.id_way) the_geom ,w.id_way  
        FROM ${osmTablesPrefix}_way w, ${IDWAYS_TABLE} b WHERE w.id_way = b.id_way) geom_table
        WHERE ST_GEOMETRYN(the_geom, 1) = ST_GEOMETRYN(the_geom, ST_NUMGEOMETRIES(the_geom)) AND ST_NUMGEOMETRIES(the_geom) > 3;
        create index on ${WAYS_POLYGONS_TMP}(id_way);        """


                            datasource.execute """drop table if exists ${outputTableName}; 
        CREATE TABLE ${outputTableName} AS SELECT 'w'||a.id_way as id, a.the_geom ${createTagList(datasource,columnsSelector)} from 
        ${WAYS_POLYGONS_TMP} as a, ${osmTablesPrefix}_way_tag  b where a.id_way=b.id_way group by a.id_way;"""

                            datasource.execute "DROP TABLE IF EXISTS ${WAYS_POLYGONS_TMP};"

                        }
                        else {
                            info "No keys or values found to extract ways."
                            return null
                        }
                    }else{
                        error "Invalid EPSG code : $epsgCode"
                        return null
                    }
                } else {
                    error "Please set a valid database connection"
                    return null
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
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all relations transformed as polygons
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess extractRelationsAsPolygons() {
    create({
            title "Transform all OSM ways as polygons"
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: -1, tags :[], columnsToKeep: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep ->
                if (datasource != null) {
                    if(epsgCode!=-1){
                        outputTableName = "RELATION_POLYGONS_$uuid"
                        def countTagsQuery = "select count(*) as count from ${osmTablesPrefix}_relation_tag"
                        def columnsSelector = """select distinct tag_key as tag_key from ${osmTablesPrefix}_relation_tag """
                        def tagsFilter =''
                        if (tags != null && !tags.isEmpty()) {
                            def tagKeysList = []
                            if(tags in Map) {
                                tagKeysList = tags.keySet().findResults { it != "null" && !it.isEmpty() ? it : null }
                            }
                            else{
                                tagKeysList = tags.findResults { it != "null" && !it.isEmpty() ? it : null }
                            }
                            tagsFilter = createWhereFilter(tags)
                            countTagsQuery += " where ${tagsFilter}"
                            if(columnsToKeep!=null){
                                columnsSelector += " where tag_key in ('${(tagKeysList + columnsToKeep).unique().join("','")}')"
                            }
                        }
                        else if(columnsToKeep!=null&&!columnsToKeep.isEmpty()){
                            columnsSelector += "tag_key in ('${columnsToKeep.unique().join("','")}')"
                        }

                        if (datasource.firstRow(countTagsQuery).count>0) {
                            info "Build outer polygons"
                            String RELATIONS_POLYGONS_OUTER = "RELATIONS_POLYGONS_OUTER_$uuid"
                            String RELATION_FILTERED_KEYS = "RELATION_FILTERED_KEYS_$uuid"
                            def outer_condition
                            def inner_condition
                            if(tagsFilter.isEmpty()){
                                outer_condition = "WHERE w.id_way = br.id_way and br.role='outer'"
                                inner_condition = "WHERE w.id_way = br.id_way and br.role='inner'"
                            }
                            else{
                                datasource.execute """ DROP TABLE IF EXISTS ${RELATION_FILTERED_KEYS};
                CREATE TABLE ${RELATION_FILTERED_KEYS} as SELECT DISTINCT id_relation  FROM ${osmTablesPrefix}_relation_tag wt 
                WHERE ${tagsFilter};
                CREATE INDEX ON ${RELATION_FILTERED_KEYS}(id_relation);"""
                                outer_condition = """, ${RELATION_FILTERED_KEYS} g
                    WHERE br.id_relation=g.id_relation AND w.id_way = br.id_way AND br.role='outer'"""
                                inner_condition = """, ${RELATION_FILTERED_KEYS} g
                        WHERE br.id_relation=g.id_relation AND w.id_way = br.id_way and br.role='inner'"""
                            }

            datasource.execute """DROP TABLE IF EXISTS ${RELATIONS_POLYGONS_OUTER};
            CREATE TABLE ${RELATIONS_POLYGONS_OUTER} AS 
            SELECT ST_LINEMERGE(ST_ACCUM(the_geom)) the_geom, id_relation 
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
            SELECT ST_LINEMERGE(ST_ACCUM(the_geom)) the_geom, id_relation 
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
                WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom) and ST_NPoints(the_geom)>=4; """

                            info "Explode inner polygons"
                            def RELATIONS_POLYGONS_INNER_EXPLODED = "RELATIONS_POLYGONS_INNER_EXPLODED_$uuid"
                            datasource.execute """ DROP TABLE IF EXISTS ${RELATIONS_POLYGONS_INNER_EXPLODED};
            CREATE TABLE ${RELATIONS_POLYGONS_INNER_EXPLODED} AS 
            SELECT the_geom as the_geom, id_relation 
            FROM st_explode('${RELATIONS_POLYGONS_INNER}') 
            WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom) and ST_NPoints(the_geom)>=4; """

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


                            datasource.execute """
            DROP TABLE IF EXISTS ${outputTableName};     
            CREATE TABLE ${outputTableName} AS SELECT 'r'||a.id_relation as id, a.the_geom ${createTagList(datasource,columnsSelector)}
            from ${RELATIONS_MP_HOLES} as a, ${osmTablesPrefix}_relation_tag  b 
            where a.id_relation=b.id_relation group by a.the_geom, a.id_relation;
            """

                            datasource.execute """DROP TABLE IF EXISTS ${RELATIONS_POLYGONS_OUTER}, ${RELATIONS_POLYGONS_INNER},
           ${RELATIONS_POLYGONS_OUTER_EXPLODED}, ${RELATIONS_POLYGONS_INNER_EXPLODED}, ${RELATIONS_MP_HOLES}, ${RELATION_FILTERED_KEYS};"""

                        } else {
                            warn "No keys or values found in the relations."
                            return null
                        }
                    }else{
                        error "Invalid EPSG code : $epsgCode"
                        return null
                    }
                } else {
                    error "Please set a valid database connection"
                    return null
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
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all ways transformed as lines
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
IProcess extractWaysAsLines() {
    return create({
            title "Transform all OSM ways as lines"
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: -1, tags: [], columnsToKeep: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep ->
                if (datasource != null) {
                    if(epsgCode!=-1){
                        outputTableName = "WAYS_LINES_$uuid"
                        def IDWAYS_TABLE = "ID_WAYS_$uuid"

                        def countTagsQuery = "select count(*) as count from ${osmTablesPrefix}_way_tag"
                        def columnsSelector = """select distinct tag_key as tag_key from ${osmTablesPrefix}_way_tag """
                        def tagsFilter =''
                        if (tags != null && !tags.isEmpty()) {
                            def tagKeysList = []
                            if(tags in Map) {
                                tagKeysList = tags.keySet().findResults { it != "null" && !it.isEmpty() ? it : null }
                            }
                            else{
                                tagKeysList = tags.findResults { it != "null" && !it.isEmpty() ? it : null }
                            }
                            tagsFilter = createWhereFilter(tags)
                            countTagsQuery += " where ${tagsFilter}"
                            if(columnsToKeep!=null){
                                columnsSelector += " where tag_key in ('${(tagKeysList + columnsToKeep).unique().join("','")}')"
                            }
                        }
                        else if(columnsToKeep!=null&&!columnsToKeep.isEmpty()){
                            columnsSelector += "tag_key in ('${columnsToKeep.unique().join("','")}')"
                        }

                        if (datasource.firstRow(countTagsQuery).count>0) {
                            info "Build ways as lines"
                            String WAYS_LINES_TMP = "WAYS_LINES_TMP_$uuid"

                            if(tagsFilter.isEmpty()){
                                IDWAYS_TABLE="${osmTablesPrefix}_way_tag"
                            }
                            else{
                                datasource.execute """ DROP TABLE IF EXISTS $IDWAYS_TABLE;
                    CREATE TABLE $IDWAYS_TABLE AS SELECT DISTINCT id_way FROM ${osmTablesPrefix}_way_tag WHERE ${tagsFilter};
                    CREATE INDEX ON $IDWAYS_TABLE(id_way);"""
                            }



          datasource.execute """DROP TABLE IF EXISTS ${WAYS_LINES_TMP}; 
        CREATE TABLE  ${WAYS_LINES_TMP} AS SELECT id_way,ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(THE_GEOM), 4326), 
        ${epsgCode}) the_geom FROM 
        (SELECT (SELECT ST_ACCUM(the_geom) the_geom FROM (SELECT n.id_node, n.the_geom, wn.id_way idway FROM 
        ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
        WHERE n.id_node = wn.id_node ORDER BY wn.node_order) 
        WHERE idway = w.id_way) the_geom, w.id_way  
        FROM ${osmTablesPrefix}_way w, ${IDWAYS_TABLE} b WHERE w.id_way = b.id_way) geom_table 
        WHERE ST_NUMGEOMETRIES(the_geom) >= 2;
        CREATE INDEX ON ${WAYS_LINES_TMP}(ID_WAY);"""

            datasource.execute """drop table if exists ${outputTableName};
            CREATE TABLE ${outputTableName} AS SELECT 'w'||a.id_way as id, a.the_geom ${createTagList(datasource,columnsSelector)} 
            from ${WAYS_LINES_TMP} as a, ${osmTablesPrefix}_way_tag  b where a.id_way=b.id_way group by a.id_way;
            DROP TABLE IF EXISTS ${WAYS_LINES_TMP}, $IDWAYS_TABLE ;"""
                        } else {
                            info "No keys or values found in the ways."
                            return null
                        }
                    }
                    else{
                        error "Invalid EPSG code : $epsgCode"
                        return null
                    }
                } else {
                    error "Please set a valid database connection"
                    return null
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
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all relations transformed as lines
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
IProcess extractRelationsAsLines() {
    return create({
            title "Transform all OSM ways as lines"
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: -1, tags: [], columnsToKeep: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tags,columnsToKeep ->
                if (datasource != null) {
                    if(epsgCode!=-1){
                        outputTableName = "RELATIONS_LINES_$uuid"
                        def countTagsQuery = "select count(*) as count from ${osmTablesPrefix}_relation_tag"
                        def columnsSelector = """select distinct tag_key as tag_key from ${osmTablesPrefix}_relation_tag """
                        def tagsFilter =''
                        if (tags != null && !tags.isEmpty()) {
                            def tagKeysList = []
                            if(tags in Map) {
                                tagKeysList = tags.keySet().findResults { it != "null" && !it.isEmpty() ? it : null }
                            }
                            else{
                                tagKeysList = tags.findResults { it != "null" && !it.isEmpty() ? it : null }
                            }
                            tagsFilter = createWhereFilter(tags)
                            countTagsQuery += " where ${tagsFilter}"
                            if(columnsToKeep!=null){
                                columnsSelector += " where tag_key in ('${(tagKeysList + columnsToKeep).unique().join("','")}')"
                            }
                        }
                        else if(columnsToKeep!=null&&!columnsToKeep.isEmpty()){
                            columnsSelector += "tag_key in ('${columnsToKeep.unique().join("','")}')"
                        }

                        if (datasource.firstRow(countTagsQuery).count>0) {
                            String RELATIONS_LINES_TMP = "RELATIONS_LINES_TMP_$uuid"
                            String RELATION_FILTERED_KEYS = "RELATION_FILTERED_KEYS_$uuid"

                            if(tagsFilter.isEmpty()){
                                RELATION_FILTERED_KEYS = "${osmTablesPrefix}_relation"
                            }
                            else{
                                datasource.execute """ DROP TABLE IF EXISTS ${RELATION_FILTERED_KEYS};
                CREATE TABLE ${RELATION_FILTERED_KEYS} as SELECT DISTINCT id_relation  FROM ${osmTablesPrefix}_relation_tag wt 
                WHERE ${tagsFilter};
                CREATE INDEX ON ${RELATION_FILTERED_KEYS}(id_relation);"""
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

                            datasource.execute """drop table if exists ${outputTableName};
            CREATE TABLE ${outputTableName} AS SELECT 'r'||a.id_relation as id, a.the_geom ${createTagList(datasource,columnsSelector)} 
            from ${RELATIONS_LINES_TMP} as a, ${osmTablesPrefix}_relation_tag  b where a.id_relation=b.id_relation group by a.id_relation;
            DROP TABLE IF EXISTS ${RELATIONS_LINES_TMP}, ${RELATION_FILTERED_KEYS};"""

                        }
                        else {
                            warn "No keys or values found in the relations."
                            return null
                        }
                    }
                    else{
                        error "Invalid EPSG code : $epsgCode"
                        return null
                    }
                } else {
                    error "Please set a valid database connection"
                    return null
                }
                [outputTableName: outputTableName]
            }
        })
}


/**
 * Perform the OSM data transformation from relation model to GIS layers.
 *
 * @param datasource Datasource to use for the extraction
 * @param filterArea Area to extract. Must be specificied
 * @param epsgCode as integer value
 * Default value is -1. If the default value is used the process will find the best UTM projection
 * according the interior point of the filterArea
 * @param dataDim Dimension of the data to extract. It should be an array with the value 0, 1, 2.
 * 0 = extract points
 * 1 = extract lines
 * 2 = extract polygons
 * @param tags Array of tags to extract.
 *
 * @return Map containing the name of the output polygon table with the key
 * 'outputPolygonsTableName', the name of the output line table with the key 'outputLinesTableName', the name of the
 * output point table with the key 'outputPointsTableName'
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess stackingTransform(datasource, filterArea, epsgCode, dataDim, tags) {
    if (datasource == null) {
        error "The datasource cannot be null"
    }
    if (filterArea == null) {
        error "Filter area not defined"
    }
    if (dataDim == null) {
        dataDim = [0,1,2]
    }

    def query =""
    Coordinate interiorPoint
    if(filterArea instanceof Envelope ) {
        query = OSMHelper.Utilities.buildOSMQuery(filterArea, tags, NODE, WAY, RELATION)
        interiorPoint = filterArea.centre()
        epsgCode = SFSUtilities.getSRID(datasource.getConnection(), interiorPoint.y as float, interiorPoint.x as float)
    }
    else if( filterArea instanceof Polygon ) {
        query = OSMHelper.Utilities.buildOSMQuery(filterArea, tags, NODE, WAY, RELATION)
        interiorPoint= filterArea.getCentroid().getCoordinate()
        epsgCode = SFSUtilities.getSRID(datasource.getConnection(), interiorPoint.y as float, interiorPoint.x as float)
    }
    else {
        error "The filter area must be an Envelope or a Polygon"
        return null
    }
    if(epsgCode!=-1){
        def extract = OSMHelper.Loader.extract()
        if (!query.isEmpty()) {
            if (extract.execute(overpassQuery: query)) {
                def prefix = "OSM_FILE_$uuid"
                def load = OSMHelper.Loader.load()
                info "Loading"
                if (load(datasource: datasource, osmTablesPrefix: prefix, osmFilePath: extract.results.outputFilePath)) {
                    def outputPointsTableName = null
                    def outputPolygonsTableName = null
                    def outputLinesTableName = null
                    if (dataDim.contains(0)) {
                        def transform = OSMHelper.Transform.toPoints()
                        info "Transforming points"
                        assert transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsgCode, tags:tags)
                        outputPointsTableName = transform.results.outputTableName
                    }
                    if (dataDim.contains(1)) {
                        def transform = OSMHelper.Transform.extractWaysAsLines()
                        info "Transforming lines"
                        assert transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsgCode,tags:tags)
                        outputLinesTableName = transform.results.outputTableName
                    }
                    if (dataDim.contains(2)) {
                        def transform = OSMHelper.Transform.toPolygons()
                        info "Transforming polygons"
                        assert transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsgCode,tags:tags)
                        outputPolygonsTableName = transform.results.outputTableName
                    }
                    return [outputPolygonsTableName: outputPolygonsTableName,
                            outputPointsTableName  : outputPointsTableName,
                            outputLinesTableName   : outputLinesTableName]
                }
            }
        }
        else{
            error "Invalid EPSG code : $epsgCode"
            return null
        }
    }
    return null
}

/**
 * This function is used to extract nodes as points
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param outputNodesPoints the name of the nodes points table
 * @param tagKeys list ok keys to be filtered
 * @param columnsToKeep a list of columns to keep.
 * The name of a column corresponds to a key name
 *
 * @return true if some points have been built
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
static boolean extractNodesAsPoints(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode, String outputNodesPoints, def tags, def columnsToKeep) {
    def countTagsQuery = "select count(*) as count from ${osmTablesPrefix}_node_tag"
    def columnsSelector = """select distinct tag_key as tag_key from ${osmTablesPrefix}_node_tag """
    def tagsFilter =''
    if (tags != null && !tags.isEmpty()) {
        def tagKeysList = []
        if(tags in Map) {
             tagKeysList = tags.keySet().findResults { it != "null" && !it.isEmpty() ? it : null }
        }
        else{
             tagKeysList = tags.findResults { it != "null" && !it.isEmpty() ? it : null }
        }
        tagsFilter = createWhereFilter(tags)
        countTagsQuery += " where ${tagsFilter}"
        if(columnsToKeep!=null){
            columnsSelector += " where tag_key in ('${(tagKeysList + columnsToKeep).unique().join("','")}')"
        }
    }
    else if(columnsToKeep!=null && !columnsToKeep.isEmpty()){
        columnsSelector += "tag_key in ('${columnsToKeep.unique().join("','")}')"
    }

    if (datasource.firstRow(countTagsQuery).count>0) {
        info "Build nodes as points"
        if(tagsFilter.isEmpty()){
            datasource.execute """        
        drop table if exists ${outputNodesPoints}; 
         CREATE TABLE ${outputNodesPoints} AS SELECT a.id_node,ST_TRANSFORM(ST_SETSRID(a.THE_GEOM, 4326), 
            ${epsgCode}) as the_geom ${createTagList(datasource, columnsSelector)} from ${osmTablesPrefix}_node as a, 
          ${osmTablesPrefix}_node_tag  b where a.id_node=b.id_node group by a.id_node;"""
        }else {
            def FILTERED_NODES = "FILTERED_NODES_${OSMTools.uuid}"
            datasource.execute """create table $FILTERED_NODES as
        SELECT DISTINCT id_node from ${osmTablesPrefix}_node_tag where ${tagsFilter};
        create index on $FILTERED_NODES(id_node);        
        drop table if exists ${outputNodesPoints}; 
         CREATE TABLE ${outputNodesPoints} AS SELECT a.id_node,ST_TRANSFORM(ST_SETSRID(a.THE_GEOM, 4326), 
            ${epsgCode}) as the_geom ${createTagList(datasource, columnsSelector)} from ${osmTablesPrefix}_node as a, 
          ${osmTablesPrefix}_node_tag  b, $FILTERED_NODES c where a.id_node=b.id_node and a.id_node=c.id_node group by a.id_node;"""
        }

        return true
    } else {
        info("No keys or values found in the nodes")
        return false
    }

}


/**
 * Method to build a where filter based on a list of key, values
 *
 * @param tags the input Map of key and values with the following signature
 * ["building", "landcover"] or
 * ["building": ["yes"], "landcover":["grass", "forest"]]
 * @return a where filter as
 * tag_key in '(building', 'landcover') or
 * (tag_key = 'building' and tag_value in ('yes')) or (tag_key = 'landcover' and tag_value in ('grass','forest')))
 */
static def createWhereFilter(def tags){
    def whereKeysValuesFilter = ""
    if(tags in Map){
        def whereQuery = []
        tags.each{entry ->
            def keyIn = ''
            def valueIn = ''
            def key  = entry.key
            if(key!="null" && !key.isEmpty()){
                keyIn+= "tag_key = '${key}'"
            }

            def valuesList = entry.value.flatten().findResults{it!=null && !it.isEmpty()?it:null}
            if(valuesList!=null && !valuesList.isEmpty()){
                valueIn += "tag_value in ('${valuesList.join("','")}')"
            }

            if(!keyIn.isEmpty()&& !valueIn.isEmpty()){
                whereQuery+= "$keyIn and $valueIn"
            }
            else if(!keyIn.isEmpty()){
                whereQuery+= "$keyIn"
            }
            else if(!valueIn.isEmpty()){
                whereQuery+= "$valueIn"
            }
        }
        whereKeysValuesFilter = "(${whereQuery.join(') or (')})"
    }
    else {
        whereKeysValuesFilter = "tag_key in ('${tags.join("','")}')"
    }
    return whereKeysValuesFilter
}

/**
 * Build a case when expression to pivot keys
 * @param datasource a connection to a database
 * @param selectTableQuery the table that contains the keys and values to pivot
 * @return the case when expression
 */
static def createTagList(datasource, selectTableQuery){
    def rowskeys = datasource.rows(selectTableQuery)
    def list = []
    rowskeys.tag_key.each { it ->
        list << "MAX(CASE WHEN b.tag_key = '${it}' then b.tag_value END) as \"${it.toUpperCase()}\""
    }
    def tagList =""
    if (!list.isEmpty()) {
        tagList = ", ${list.join(",")}"
    }
    return tagList
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