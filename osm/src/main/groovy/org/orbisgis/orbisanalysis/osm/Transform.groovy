/*
 * Bundle OSM is part of the OrbisGIS platform
 *
 * OrbisGIS is a java GIS application dedicated to research in GIScience.
 * OrbisGIS is developed by the GIS group of the DECIDE team of the
 * Lab-STICC CNRS laboratory, see <http://www.lab-sticc.fr/>.
 *
 * The GIS group of the DECIDE team is located at :
 *
 * Laboratoire Lab-STICC – CNRS UMR 6285
 * Equipe DECIDE
 * UNIVERSITÉ DE BRETAGNE-SUD
 * Institut Universitaire de Technologie de Vannes
 * 8, Rue Montaigne - BP 561 56017 Vannes Cedex
 *
 * OSM is distributed under LGPL 3 license.
 *
 * Copyright (C) 2019 CNRS (Lab-STICC UMR CNRS 6285)
 *
 *
 * OSM is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * OSM is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * OSM. If not, see <http://www.gnu.org/licenses/>.
 *
 * For more information, please consult: <http://www.orbisgis.org/>
 * or contact directly:
 * info_at_ orbisgis.org
 */
package org.orbisgis.orbisanalysis.osm

import groovy.transform.BaseScript
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess

import static org.orbisgis.orbisanalysis.osm.utils.TransformUtils.buildIndexes
import static org.orbisgis.orbisanalysis.osm.utils.TransformUtils.createTagList
import static org.orbisgis.orbisanalysis.osm.utils.TransformUtils.createWhereFilter
import static org.orbisgis.orbisanalysis.osm.utils.TransformUtils.extractNodesAsPoints
import static org.orbisgis.orbisanalysis.osm.utils.TransformUtils.getColumnSelector
import static org.orbisgis.orbisanalysis.osm.utils.TransformUtils.getCountTagsQuery
import static org.orbisgis.orbisanalysis.osm.utils.TransformUtils.toPolygonOrLine

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
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: 4326, tags: [], columnsToKeep: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep ->
                String outputTableName = "OSM_POINTS_$uuid"
                if(!datasource){
                    error "Please set a valid database connection"
                    return
                }
                if(epsgCode == -1){
                    error "Invalid EPSG code : $epsgCode"
                    return
                }
                info "Start points transformation"
                info "Indexing osm tables..."
                buildIndexes(datasource, osmTablesPrefix)
                def pointsNodes = extractNodesAsPoints(datasource, osmTablesPrefix, epsgCode, outputTableName, tags, columnsToKeep)
                if (pointsNodes) {
                    info "The points have been built."
                } else {
                    warn "Cannot extract any point."
                    return
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
 * @param columnsToKeep a list of columns to keep. The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all lines
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess toLines() {
    return create({
            title "Transform all OSM features as lines"
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: 4326, tags : [], columnsToKeep:[]
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep ->
                return toPolygonOrLine("LINES", datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep)
            }
    })
}

/**
 * This process is used to extract all the polygons from the OSM tables
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep. The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all polygons
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 */
IProcess toPolygons() {
    return create({
        title "Transform all OSM features as polygons"
        inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: 4326, tags: [], columnsToKeep: []
        outputs outputTableName: String
        run { datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep ->
            return toPolygonOrLine("POLYGONS", datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep)
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
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: 4326, tags: [], columnsToKeep: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep ->
                if(!datasource){
                    error "Please set a valid database connection"
                    return
                }
                if(epsgCode == -1){
                    error "Invalid EPSG code : $epsgCode"
                    return
                }
                def outputTableName = "WAYS_POLYGONS_$uuid"
                def idWaysPolygons = "ID_WAYS_POLYGONS_$uuid"
                def osmTableTag = "${osmTablesPrefix}_way_tag"
                def countTagsQuery = getCountTagsQuery(osmTableTag, tags)
                def columnsSelector = getColumnSelector(osmTableTag, tags, columnsToKeep)
                def tagsFilter = createWhereFilter(tags)

                if (datasource.firstRow(countTagsQuery).count <= 0) {
                    warn "No keys or values found to extract ways."
                    return
                }

                info "Build way polygons"
                def waysPolygonTmp = "WAYS_POLYGONS_TMP$uuid"
                datasource.execute "DROP TABLE IF EXISTS $waysPolygonTmp;"

                if(tagsFilter){
                    datasource.execute """
                            DROP TABLE IF EXISTS $idWaysPolygons;
                            CREATE TABLE $idWaysPolygons AS
                                SELECT DISTINCT id_way
                                FROM $osmTableTag
                                WHERE $tagsFilter;
                            CREATE INDEX ON $idWaysPolygons(id_way);
                    """
                }
                else{
                    idWaysPolygons = osmTableTag
                }

                datasource.execute """
                        CREATE TABLE $waysPolygonTmp AS
                            SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKEPOLYGON(ST_MAKELINE(the_geom)), 4326), $epsgCode) AS the_geom, id_way
                            FROM(
                                SELECT(
                                    SELECT ST_ACCUM(the_geom) AS the_geom
                                    FROM(
                                        SELECT n.id_node, n.the_geom, wn.id_way idway
                                        FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn
                                        WHERE n.id_node = wn.id_node
                                        ORDER BY wn.node_order)
                                    WHERE  idway = w.id_way
                                ) the_geom ,w.id_way  
                                FROM ${osmTablesPrefix}_way w, $idWaysPolygons b
                                WHERE w.id_way = b.id_way
                            ) geom_table
                            WHERE ST_GEOMETRYN(the_geom, 1) = ST_GEOMETRYN(the_geom, ST_NUMGEOMETRIES(the_geom)) 
                            AND ST_NUMGEOMETRIES(the_geom) > 3;
                        CREATE INDEX ON $waysPolygonTmp(id_way);
                """

                datasource.execute """
                        DROP TABLE IF EXISTS $outputTableName; 
                        CREATE TABLE $outputTableName AS 
                            SELECT 'w'||a.id_way AS id, a.the_geom ${createTagList(datasource,columnsSelector)} 
                            FROM $waysPolygonTmp AS a, $osmTableTag b
                            WHERE a.id_way=b.id_way
                            GROUP BY a.id_way;
                """

                datasource.execute "DROP TABLE IF EXISTS $waysPolygonTmp;"

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
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: 4326, tags :[], columnsToKeep: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep ->
                if(!datasource){
                    error "Please set a valid database connection"
                    return
                }
                if(epsgCode == -1){
                    error "Invalid EPSG code : $epsgCode"
                    return
                }
                def outputTableName = "RELATION_POLYGONS_$uuid"
                def osmTableTag = "${osmTablesPrefix}_relation_tag"
                def countTagsQuery = getCountTagsQuery(osmTableTag, tags)
                def columnsSelector = getColumnSelector(osmTableTag, tags, columnsToKeep)
                def tagsFilter = createWhereFilter(tags)

                if (datasource.firstRow(countTagsQuery).count <= 0) {
                    warn "No keys or values found in the relations."
                    return
                }
                info "Build outer polygons"
                def relationsPolygonsOuter = "RELATIONS_POLYGONS_OUTER_$uuid"
                def relationFilteredKeys = "RELATION_FILTERED_KEYS_$uuid"
                def outer_condition
                def inner_condition
                if(tagsFilter.isEmpty()){
                    outer_condition = "WHERE w.id_way = br.id_way AND br.role='outer'"
                    inner_condition = "WHERE w.id_way = br.id_way AND br.role='inner'"
                }
                else{
                    datasource.execute """
                            DROP TABLE IF EXISTS $relationFilteredKeys;
                            CREATE TABLE $relationFilteredKeys AS 
                                SELECT DISTINCT id_relation
                                FROM ${osmTablesPrefix}_relation_tag wt 
                                WHERE $tagsFilter;
                            CREATE INDEX ON $relationFilteredKeys(id_relation);
                    """
                    outer_condition = """, $relationFilteredKeys g 
                            WHERE br.id_relation=g.id_relation
                            AND w.id_way = br.id_way
                            AND br.role='outer'
                    """
                    inner_condition = """, $relationFilteredKeys g
                            WHERE br.id_relation=g.id_relation
                            AND w.id_way = br.id_way
                            AND br.role='inner'
                    """
                }

                datasource.execute """
                        DROP TABLE IF EXISTS $relationsPolygonsOuter;
                        CREATE TABLE $relationsPolygonsOuter AS 
                        SELECT ST_LINEMERGE(ST_ACCUM(the_geom)) as the_geom, id_relation 
                        FROM(
                            SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), $epsgCode) AS  the_geom, id_relation, role, id_way 
                            FROM(
                                SELECT(
                                    SELECT ST_ACCUM(the_geom) the_geom 
                                    FROM(
                                        SELECT n.id_node, n.the_geom, wn.id_way idway 
                                        FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
                                        WHERE n.id_node = wn.id_node ORDER BY wn.node_order) 
                                    WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role 
                                FROM ${osmTablesPrefix}_way w, ${osmTablesPrefix}_way_member br $outer_condition) geom_table
                                WHERE st_numgeometries(the_geom)>=2) 
                        GROUP BY id_relation;
                """

                info "Build inner polygons"
                def relationsPolygonsInner = "RELATIONS_POLYGONS_INNER_$uuid"
                datasource.execute """
                        DROP TABLE IF EXISTS $relationsPolygonsInner;
                        CREATE TABLE $relationsPolygonsInner AS 
                        SELECT ST_LINEMERGE(ST_ACCUM(the_geom)) the_geom, id_relation 
                        FROM(
                            SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), $epsgCode) AS the_geom, id_relation, role, id_way 
                            FROM(     
                                SELECT(
                                    SELECT ST_ACCUM(the_geom) the_geom 
                                    FROM(
                                        SELECT n.id_node, n.the_geom, wn.id_way idway 
                                        FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
                                        WHERE n.id_node = wn.id_node ORDER BY wn.node_order) 
                                    WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role 
                                FROM ${osmTablesPrefix}_way w, ${osmTablesPrefix}_way_member br ${inner_condition}) geom_table 
                                WHERE st_numgeometries(the_geom)>=2) 
                        GROUP BY id_relation;
                """

                info "Explode outer polygons"
                def relationsPolygonsOuterExploded = "RELATIONS_POLYGONS_OUTER_EXPLODED_$uuid"
                datasource.execute """
                        DROP TABLE IF EXISTS $relationsPolygonsOuterExploded;
                        CREATE TABLE $relationsPolygonsOuterExploded AS 
                            SELECT ST_MAKEPOLYGON(the_geom) AS the_geom, id_relation 
                            FROM st_explode('$relationsPolygonsOuter') 
                            WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom)
                            AND ST_NPoints(the_geom)>=4;
                """

                info "Explode inner polygons"
                def relationsPolygonsInnerExploded = "RELATIONS_POLYGONS_INNER_EXPLODED_$uuid"
                datasource.execute """
                        DROP TABLE IF EXISTS $relationsPolygonsInnerExploded;
                        CREATE TABLE $relationsPolygonsInnerExploded AS 
                            SELECT the_geom AS the_geom, id_relation 
                            FROM st_explode('$relationsPolygonsInner') 
                            WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom)
                            AND ST_NPoints(the_geom)>=4; 
                """

                info "Build all polygon relations"
                def relationsMpHoles = "RELATIONS_MP_HOLES_$uuid"
                datasource.execute """
                        CREATE INDEX ON $relationsPolygonsOuterExploded USING RTREE (the_geom);
                        CREATE INDEX ON $relationsPolygonsInnerExploded USING RTREE (the_geom);
                        CREATE INDEX ON $relationsPolygonsOuterExploded(id_relation);
                        CREATE INDEX ON $relationsPolygonsInnerExploded(id_relation);       
                        DROP TABLE IF EXISTS $relationsMpHoles;
                        CREATE TABLE $relationsMpHoles AS (
                            SELECT ST_MAKEPOLYGON(ST_EXTERIORRING(a.the_geom), ST_ACCUM(b.the_geom)) AS the_geom, a.ID_RELATION
                            FROM $relationsPolygonsOuterExploded AS a 
                            LEFT JOIN $relationsPolygonsInnerExploded AS b 
                            ON(
                                a.the_geom && b.the_geom 
                                AND st_contains(a.the_geom, b.THE_GEOM) 
                                AND a.ID_RELATION=b.ID_RELATION)
                            GROUP BY a.the_geom, a.id_relation)
                        UNION(
                            SELECT a.the_geom, a.ID_RELATION 
                            FROM $relationsPolygonsOuterExploded AS a 
                            LEFT JOIN  $relationsPolygonsInnerExploded AS b 
                            ON a.id_relation=b.id_relation 
                            WHERE b.id_relation IS NULL);
                        CREATE INDEX ON $relationsMpHoles(id_relation);
                """

                datasource.execute """
                        DROP TABLE IF EXISTS $outputTableName;     
                        CREATE TABLE $outputTableName AS 
                            SELECT 'r'||a.id_relation AS id, a.the_geom ${createTagList(datasource,columnsSelector)}
                            FROM $relationsMpHoles AS a, ${osmTablesPrefix}_relation_tag  b 
                            WHERE a.id_relation=b.id_relation 
                            GROUP BY a.the_geom, a.id_relation;
                """

                datasource.execute """
                        DROP TABLE IF EXISTS    $relationsPolygonsOuter, 
                                                $relationsPolygonsInner,
                                                $relationsPolygonsOuterExploded, 
                                                $relationsPolygonsInnerExploded, 
                                                $relationsMpHoles, 
                                                $relationFilteredKeys;
                """
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
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: 4326, tags: [], columnsToKeep: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep ->
                if(!datasource){
                    error "Please set a valid database connection"
                    return
                }
                if(epsgCode == -1){
                    error "Invalid EPSG code : $epsgCode"
                    return
                }
                def outputTableName = "WAYS_LINES_$uuid"
                def idWaysTable = "ID_WAYS_$uuid"
                def osmTableTag = "${osmTablesPrefix}_way_tag"
                def countTagsQuery = getCountTagsQuery(osmTableTag, tags)
                def columnsSelector = getColumnSelector(osmTableTag, tags, columnsToKeep)
                def tagsFilter = createWhereFilter(tags)

                if (datasource.firstRow(countTagsQuery).count <= 0) {
                    info "No keys or values found in the ways."
                    return
                }
                info "Build ways as lines"
                def waysLinesTmp = "WAYS_LINES_TMP_$uuid"

                if(tagsFilter.isEmpty()){
                    idWaysTable="${osmTablesPrefix}_way_tag"
                }
                else{
                    datasource.execute """
                            DROP TABLE IF EXISTS $idWaysTable;
                            CREATE TABLE $idWaysTable AS
                                SELECT DISTINCT id_way
                                FROM ${osmTablesPrefix}_way_tag
                                WHERE $tagsFilter;
                            CREATE INDEX ON $idWaysTable(id_way);
                    """
                }

                datasource.execute """
                        DROP TABLE IF EXISTS $waysLinesTmp; 
                        CREATE TABLE  $waysLinesTmp AS 
                            SELECT id_way,ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(THE_GEOM), 4326), $epsgCode) the_geom 
                            FROM(
                                SELECT(
                                    SELECT ST_ACCUM(the_geom) the_geom 
                                    FROM(
                                        SELECT n.id_node, n.the_geom, wn.id_way idway 
                                        FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
                                        WHERE n.id_node = wn.id_node
                                        ORDER BY wn.node_order) 
                                    WHERE idway = w.id_way
                                ) the_geom, w.id_way  
                                FROM ${osmTablesPrefix}_way w, $idWaysTable b 
                                WHERE w.id_way = b.id_way) geom_table 
                            WHERE ST_NUMGEOMETRIES(the_geom) >= 2;
                        CREATE INDEX ON $waysLinesTmp(ID_WAY);
                """

                datasource.execute """
                        DROP TABLE IF EXISTS $outputTableName;
                        CREATE TABLE $outputTableName AS 
                            SELECT 'w'||a.id_way AS id, a.the_geom ${createTagList(datasource,columnsSelector)} 
                            FROM $waysLinesTmp AS a, ${osmTablesPrefix}_way_tag b 
                            WHERE a.id_way=b.id_way 
                            GROUP BY a.id_way;
                        DROP TABLE IF EXISTS $waysLinesTmp, $idWaysTable;
                """
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
            inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsgCode: 4326, tags: [], columnsToKeep: []
            outputs outputTableName: String
            run { datasource, osmTablesPrefix, epsgCode, tags,columnsToKeep ->
                if(!datasource){
                    error "Please set a valid database connection"
                    return
                }
                if(epsgCode == -1){
                    error "Invalid EPSG code : $epsgCode"
                    return
                }
                def outputTableName = "RELATIONS_LINES_$uuid"
                def osmTableTag = "${osmTablesPrefix}_relation_tag"
                def countTagsQuery = getCountTagsQuery(osmTableTag, tags)
                def columnsSelector = getColumnSelector(osmTableTag, tags, columnsToKeep)
                def tagsFilter = createWhereFilter(tags)

                if (datasource.firstRow(countTagsQuery).count <= 0) {
                    warn "No keys or values found in the relations."
                    return
                }
                def relationsLinesTmp = "RELATIONS_LINES_TMP_$uuid"
                def RelationsFilteredKeys = "RELATION_FILTERED_KEYS_$uuid"

                if(tagsFilter.isEmpty()){
                    RelationsFilteredKeys = "${osmTablesPrefix}_relation"
                }
                else{
                    datasource.execute """
                            DROP TABLE IF EXISTS $RelationsFilteredKeys;
                            CREATE TABLE $RelationsFilteredKeys AS
                                SELECT DISTINCT id_relation
                                FROM ${osmTablesPrefix}_relation_tag wt
                                WHERE $tagsFilter;
                            CREATE INDEX ON $RelationsFilteredKeys(id_relation);
                    """
                }

                datasource.execute """
                        DROP TABLE IF EXISTS $relationsLinesTmp;
                        CREATE TABLE $relationsLinesTmp AS
                            SELECT ST_ACCUM(THE_GEOM) AS the_geom, id_relation
                            FROM(
                                SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), $epsgCode) the_geom, id_relation, id_way
                                FROM(
                                    SELECT(
                                        SELECT ST_ACCUM(the_geom) the_geom
                                        FROM(
                                            SELECT n.id_node, n.the_geom, wn.id_way idway
                                            FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn
                                            WHERE n.id_node = wn.id_node ORDER BY wn.node_order)
                                        WHERE idway = w.id_way
                                    ) the_geom, w.id_way, br.id_relation
                                    FROM ${osmTablesPrefix}_way w, (
                                        SELECT br.id_way, g.ID_RELATION
                                        FROM  ${osmTablesPrefix}_way_member br , $RelationsFilteredKeys g
                                        WHERE br.id_relation=g.id_relation
                                    ) br
                                    WHERE w.id_way = br.id_way
                                ) geom_table
                                WHERE st_numgeometries(the_geom)>=2)
                            GROUP BY id_relation;
                        CREATE INDEX ON $relationsLinesTmp(id_relation);
                """

                datasource.execute """
                        DROP TABLE IF EXISTS $outputTableName;
                        CREATE TABLE $outputTableName AS
                            SELECT 'r'||a.id_relation AS id, a.the_geom ${createTagList(datasource,columnsSelector)}
                            FROM $relationsLinesTmp AS a, ${osmTablesPrefix}_relation_tag  b
                            WHERE a.id_relation=b.id_relation
                            GROUP BY a.id_relation;
                        DROP TABLE IF EXISTS $relationsLinesTmp, $RelationsFilteredKeys;
                """
                [outputTableName: outputTableName]
            }
    })
}
