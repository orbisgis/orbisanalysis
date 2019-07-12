package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanager.inoutput.Input
import org.orbisgis.processmanagerapi.IProcess


@BaseScript OSMHelper osmHelper


    /**
     * Extract building as polygons
     * With zindex
     * @return
     * @author Erwan Bocher
     * @author Elisabeth Le Saux
     */
    IProcess BUILDING() {
        return create({
            title "Building extraction for a specified place/area"
            inputs bbox: '', poly: '', datasource: JdbcDataSource,
                    tags: ['height','building:height','roof:height','building:roof:height',
                           'building:levels','roof:levels','building:roof:levels','building',
                           'amenity','layer','aeroway','historic','leisure','monument',
                           'place_of_worship','military','railway','public_transport',
                           'barrier','government','historic:building','grandstand',
                           'apartments','ruins','agricultural','barn', 'healthcare',
                           'education','restaurant','sustenance','office']
            outputs datasource: JdbcDataSource, outputTableName: String
            run { bbox, poly, datasource, tags ->
                if (datasource == null) {
                    datasource = H2GIS.open(File.createTempFile("osm","db").path)
                }
                def extract = OSMHelper.Loader.extract()
                String query = defineQueryArea(bbox, poly)
                if (query != '') {
                    query += "((node;way;relation;);>;);out;"
                    if (extract.execute(overpassQuery: query)) {
                        def load = OSMHelper.Loader.load()
                        def uuid = UUID.randomUUID().toString().replaceAll("-", "_")
                        def prefix = "OSM_FILE_$uuid"
                        logger.info("Loading")
                        if (load.execute(datasource: datasource, osmTablesPrefix: prefix, osmFilePath: extract.results.outputFilePath)) {
                            def transform = OSMHelper.Transform.toPolygons()
                            logger.info("Transforming")
                            assert transform.execute(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tag_keys: tags)
                            [datasource: datasource, outputTableName: transform.results.outputTableName]
                        }
                    }
                } else {
                    logger.error('Query area not defined')
                }
            }

        })

    }

    /**
     * Extract landcover as polygons
     * With zindex
     * @param closure
     * @return
     * @author Erwan Bocher
     * @author Elisabeth Le Saux
     */
public IProcess LANDCOVER() {
    return create({
        title "Landcover extraction for a specified place/area"
        description "Process to extract the landcover of a given bbox, area or zone defined with a code and an admin level"
        keywords "landcover", "osm to gis"
        inputs bbox: '', poly: '', datasource: JdbcDataSource, tags: ['landcover', 'natural', 'landuse', 'water', 'waterway', 'leisure', 'aeroway', 'amenity']
        outputs datasource: JdbcDataSource, outputTableName: String
        run { bbox, poly, datasource, tags ->
            if (datasource == null) {
                datasource = H2GIS.open(File.createTempFile("osm","db").path)
            }
            def extract = OSMHelper.Loader.extract()
            String query = defineQueryArea(bbox, poly)
            if (query != '') {
                query += "((node;way;relation;);>;);out;"
                if (extract.execute(overpassQuery: query)) {
                    def load = OSMHelper.Loader.load()
                    def uuid = UUID.randomUUID().toString().replaceAll("-", "_")
                    def prefix = "OSM_FILE_$uuid"
                    logger.info("Loading")
                    if (load.execute(datasource: datasource, osmTablesPrefix: prefix, osmFilePath: extract.results.outputFilePath)) {
                        def transform = OSMHelper.Transform.toPolygons()
                        logger.info("Transforming")
                        assert transform.execute(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tag_keys: tags)
                        [datasource: datasource, outputTableName: transform.results.outputTableName]
                    }
                }
            } else {
                logger.error('Query area not defined')
            }
        }
    })
}

    /**
     * Extract water as polygons
     * With zindex
     * @param closure
     * @return
     * @author Erwan Bocher
     * @author Elisabeth Le Saux
     */
public IProcess WATER() {
    return create({
        title "Water extraction for a specified place/area"
        description "Process to extract the water of a given bbox, area or zone defined with a code and an admin level"
        keywords "water", "osm to gis"
        inputs bbox: '', poly: '', datasource: JdbcDataSource, tags: ['water', 'waterway', 'amenity']
        outputs datasource: JdbcDataSource, outputPolygonsTableName: String, outputLinesTableName: String
        run { bbox, poly, datasource, tags ->
            if (datasource == null) {
                datasource = H2GIS.open(File.createTempFile("osm","db").path)
            }
            def extract = OSMHelper.Loader.extract()
            String query = defineQueryArea(bbox, poly)
            if (query != '') {
                query += "((node;way;relation;);>;);out;"
                if (extract.execute(overpassQuery: query)) {
                    def load = OSMHelper.Loader.load()
                    def uuid = UUID.randomUUID().toString().replaceAll("-", "_")
                    def prefix = "OSM_FILE_$uuid"
                    logger.info("Loading")
                    if (load.execute(datasource: datasource, osmTablesPrefix: prefix, osmFilePath: extract.results.outputFilePath)) {
                        def transformL = OSMHelper.Transform.toLines()
                        logger.info("Transforming lines")
                        assert transformL.execute(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tag_keys: tags)
                        def transformP = OSMHelper.Transform.toPolygons()
                        logger.info("Transforming polygons")
                        assert transformP.execute(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tag_keys: tags)
                        [datasource: datasource,
                         outputPolygonsTableName: transformP.results.outputTableName,
                         outputLinesTableName: transformL.results.outputTableName
                        ]
                    }
                }
            } else {
                logger.error('Query area not defined')
            }
        }
    })
}


/**
 * Compute the area on which to make the query
 * @param bbox the bbox on which the area should be computed
 * @param poly the polygon on which the area should be computed
 * @return query the begin of the overpass query, corresponding to the defined area
 * @author Elisabeth Le Saux
 */

static String defineQueryArea(String bbox, String poly) {
    def query = ""
    if (bbox != "" && poly != "") {
        logger.error ('You should provide bbox or poly, not both !')
    } else {
        query = (bbox != "")?"[bbox:$bbox];":"[poly:$poly];"
    }
    return query
}