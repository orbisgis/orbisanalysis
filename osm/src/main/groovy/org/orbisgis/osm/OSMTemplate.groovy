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
            inputs filterArea:Map, datasource: JdbcDataSource,dataDim : [2]
            outputs datasource: JdbcDataSource, outputPolygonsTableName: String, outputPointsTableName :String, outputLinesTableName: String
            run { filterArea, datasource,dataDim ->
                if (datasource == null) {
                    datasource = H2GIS.open(File.createTempFile("osm","db").path)
                }
                if(filterArea!=null|| dataDim!=null){
                    String query = OSMHelper.Utilities.defineFilterArea(filterArea)
                    def tags = ['building',
                                'amenity','layer','aeroway','historic','leisure','monument',
                                'place_of_worship','military','railway','public_transport',
                                'barrier','government','historic:building','grandstand',
                                'apartments','ruins','agricultural','barn', 'healthcare',
                                'education','restaurant','sustenance','office']
                    def extract = OSMHelper.Loader.extract()
                if (query != '') {
                    query += ";"+OSMHelper.Utilities.defineKeysFilter(tags, OSMElement.NODE,OSMElement.WAY,OSMElement.RELATION)
                    if (extract.execute(overpassQuery: query)) {
                        def load = OSMHelper.Loader.load()
                        def uuid = UUID.randomUUID().toString().replaceAll("-", "_")
                        def prefix = "OSM_FILE_$uuid"
                        logger.info("Loading")
                        if (load.execute(datasource: datasource, osmTablesPrefix: prefix, osmFilePath: extract.results.outputFilePath)) {
                            def outputPointsTableName=null
                            def outputPolygonsTableName=null
                            def outputLinesTableName=null
                            if(dataDim.contains(0)){
                                def transform = OSMHelper.Transform.toPoints()
                                logger.info("Transforming points")
                                assert transform.execute(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tag_keys: tags)
                                outputPointsTableName = transform.results.outputTableName
                            }
                            if(dataDim.contains(1)){
                                def transform = OSMHelper.Transform.toLines()
                                logger.info("Transforming lines")
                                assert transform.execute(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tag_keys: tags)
                                outputLinesTableName = transform.results.outputTableName
                            }
                            if(dataDim.contains(2)){
                                def transform = OSMHelper.Transform.toPolygons()
                                logger.info("Transforming polygons")
                                assert transform.execute(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tag_keys: tags)
                                outputPolygonsTableName = transform.results.outputTableName
                            }
                            [datasource: datasource,
                             outputPolygonsTableName: outputPolygonsTableName,
                             outputPointsTableName:outputPointsTableName,
                             outputLinesTableName :outputLinesTableName]
                        }
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
        inputs filterArea:Map, datasource: JdbcDataSource, dataDim : [2]
        outputs datasource: JdbcDataSource, outputPolygonsTableName: String, outputPointsTableName :String, outputLinesTableName: String
        run { filterArea, datasource,dataDim ->
            if (datasource == null) {
                datasource = H2GIS.open(File.createTempFile("osm","db").path)
            }
            if(filterArea!=null|| dataDim!=null){
                String query = OSMHelper.Utilities.defineFilterArea(filterArea)
                def tags = ['landcover', 'natural', 'landuse', 'water', 'waterway', 'leisure', 'aeroway', 'amenity']
                def extract = OSMHelper.Loader.extract()
            if (query != '') {
                query += ";"+OSMHelper.Utilities.defineKeysFilter(tags, OSMElement.NODE,OSMElement.WAY,OSMElement.RELATION)
                if (extract.execute(overpassQuery: query)) {
                    def load = OSMHelper.Loader.load()
                    def uuid = UUID.randomUUID().toString().replaceAll("-", "_")
                    def prefix = "OSM_FILE_$uuid"
                    logger.info("Loading")
                    if (load.execute(datasource: datasource, osmTablesPrefix: prefix, osmFilePath: extract.results.outputFilePath)) {
                        def outputPointsTableName=null
                        def outputPolygonsTableName=null
                        def outputLinesTableName=null
                        if(dataDim.contains(0)){
                            def transform = OSMHelper.Transform.toPoints()
                            logger.info("Transforming points")
                            assert transform.execute(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tag_keys: tags)
                            outputPointsTableName = transform.results.outputTableName
                        }
                        if(dataDim.contains(1)){
                            def transform = OSMHelper.Transform.toLines()
                            logger.info("Transforming lines")
                            assert transform.execute(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tag_keys: tags)
                            outputLinesTableName = transform.results.outputTableName
                        }
                        if(dataDim.contains(2)){
                            def transform = OSMHelper.Transform.toPolygons()
                            logger.info("Transforming polygons")
                            assert transform.execute(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tag_keys: tags)
                            outputPolygonsTableName = transform.results.outputTableName
                          }
                         [datasource: datasource,
                          outputPolygonsTableName: outputPolygonsTableName,
                          outputPointsTableName:outputPointsTableName,
                          outputLinesTableName :outputLinesTableName]
                    }
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
     * @param
     * @return
     * @author Erwan Bocher
     * @author Elisabeth Le Saux
     */
public IProcess WATER() {
    return create({
        title "Water extraction for a specified place/area"
        description "Process to extract the water of a given bbox, area or zone defined with a code and an admin level"
        keywords "water", "osm to gis"
        inputs filterArea: Map, datasource: JdbcDataSource, dataDim : [2]
        outputs datasource: JdbcDataSource, outputPolygonsTableName: String, outputLinesTableName: String, outputPointsTableName: String
        run { filterArea, datasource,dataDim ->
            if (datasource == null) {
                datasource = H2GIS.open(File.createTempFile("osm","db").path)
            }
            if(filterArea!=null || dataDim!=null){
                String query = OSMHelper.Utilities.defineFilterArea(filterArea)
                def tags =  ['water', 'waterway','amenity']
                def extract = OSMHelper.Loader.extract()
            if (query != '') {
                query += ";"+OSMHelper.Utilities.defineKeysFilter(tags, OSMElement.NODE,OSMElement.WAY,OSMElement.RELATION)
                if (extract.execute(overpassQuery: query)) {
                    def load = OSMHelper.Loader.load()
                    def uuid = UUID.randomUUID().toString().replaceAll("-", "_")
                    def prefix = "OSM_FILE_$uuid"
                    logger.info("Loading")
                    if (load.execute(datasource: datasource, osmTablesPrefix: prefix, osmFilePath: extract.results.outputFilePath)) {
                        def outputPointsTableName=null
                        def outputPolygonsTableName=null
                        def outputLinesTableName=null
                        if(dataDim.contains(0)){
                        def transformPT = OSMHelper.Transform.toPoints()
                        logger.info("Transforming points")
                        assert transformPT.execute(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tag_keys: tags)
                            outputPointsTableName =transformPT.results.outputTableName
                        }
                        if(dataDim.contains(1)) {
                            def transformL = OSMHelper.Transform.toLines()
                            logger.info("Transforming lines")
                            assert transformL.execute(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tag_keys: tags)
                            outputLinesTableName =transformL.results.outputTableName
                        }
                        if(dataDim.contains(2)) {
                            def transformP = OSMHelper.Transform.toPolygons()
                            logger.info("Transforming polygons")
                            assert transformP.execute(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tag_keys: tags)
                            outputPolygonsTableName=transformP.results.outputTableName
                        }
                        [datasource: datasource,
                         outputPolygonsTableName: outputPolygonsTableName,
                         outputLinesTableName:outputLinesTableName,
                         outputPointsTableName: outputPointsTableName
                        ]
                    }
                }
            }
            } else {
                logger.error('Query area not defined')
            }
        }
    })
}