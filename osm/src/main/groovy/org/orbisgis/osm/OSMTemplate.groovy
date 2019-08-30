package org.orbisgis.osm

import groovy.transform.BaseScript
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Polygon
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanagerapi.IProcess

import static org.orbisgis.osm.OSMElement.*

@BaseScript OSMHelper osmHelper

/**
 * Perform the extraction.
 *
 * @param datasource Data source to use for the extraction, if null a new H2GIS data source is created.
 * @param filterArea Area to extract. Must be specified
 * @param dataDim Dimension of the data to extract. It should be an array with the value 0, 1, 2.
 * @param tags list of tags to be filtered.
 *
 * @return Map containing the datasource with the key 'datasource', the name of the output polygon table with the key
 * 'outputPolygonsTableName', the name of the output line table with the key 'outputLinesTableName', the name of the
 * output point table with the key 'outputPointsTableName'
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
private static def extraction(datasource, filterArea, dataDim, tags) {
    if (datasource == null) {
        datasource = H2GIS.open(File.createTempFile("osm", "db"))
    }
    if (filterArea == null) {
        error "Filter area not defined"
    }
    if (dataDim == null) {
        dataDim = [0,1,2]
    }

    def query =""
    if(filterArea instanceof Envelope || filterArea instanceof Polygon ) {
        query = OSMHelper.Utilities.buildOSMQuery(filterArea, tags, NODE, WAY, RELATION)
    }
    else {
        error "The filter area must be a JTS Envelope or a Polygon"
    }

    def extract = OSMHelper.Loader.extract()
    if (!query.isEmpty()) {
        if (extract(overpassQuery: query)) {
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
                    assert transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tagKeys: tags)
                    outputPointsTableName = transform.results.outputTableName
                }
                if (dataDim.contains(1)) {
                    def transform = OSMHelper.Transform.toLines()
                    logger.info "Transforming lines"
                    assert transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tagKeys: tags)
                    outputLinesTableName = transform.results.outputTableName
                }
                if (dataDim.contains(2)) {
                    def transform = OSMHelper.Transform.toPolygons()
                    logger.info "Transforming polygons"
                    assert transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: 2154, tagKeys: tags)
                    outputPolygonsTableName = transform.results.outputTableName
                }
                return [datasource             : datasource,
                        outputPolygonsTableName: outputPolygonsTableName,
                        outputPointsTableName  : outputPointsTableName,
                        outputLinesTableName   : outputLinesTableName]
            }
        }
    }
    return null
}

/**
 * Extract building as polygons with zindex
 *
 * @return
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess BUILDING() {
    return create({
        title "Building extraction for a specified place/area"
        inputs filterArea: Object, datasource: JdbcDataSource, dataDim: [2]
        outputs datasource: JdbcDataSource, outputPolygonsTableName: String, outputPointsTableName: String, outputLinesTableName: String
        run { filterArea, datasource, dataDim ->
            def tags = ['building', 'amenity', 'layer', 'aeroway', 'historic', 'leisure', 'monument', 'place_of_worship',
                        'military', 'railway', 'public_transport', 'barrier', 'government', 'historic:building',
                        'grandstand', 'apartments', 'ruins', 'agricultural', 'barn', 'healthcare', 'education', 'restaurant',
                        'sustenance', 'office']
            extraction(datasource, filterArea, dataDim, tags)
        }
    })

}

/**
 * Extract landcover as polygons
 *
 * @param closure
 *
 * @return
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess LANDCOVER() {
    return create({
        title "Landcover extraction for a specified place/area"
        description "Process to extract the landcover of a given bbox, area or zone defined with a code and an admin level"
        keywords "landcover", "osm to gis"
        inputs filterArea: Object, datasource: JdbcDataSource, dataDim: [2]
        outputs datasource: JdbcDataSource, outputPolygonsTableName: String, outputPointsTableName: String, outputLinesTableName: String
        run { filterArea, datasource, dataDim ->
            def tags = ['landcover', 'natural', 'landuse', 'water', 'waterway', 'leisure', 'aeroway', 'amenity']
            extraction(datasource, filterArea, dataDim, tags)
        }
    })
}

/**
 * Extract water as polygons
 *
 * @param
 *
 * @return
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess WATER() {
    return create({
        title "Water extraction for a specified place/area"
        description "Process to extract the water of a given bbox, area or zone defined with a code and an admin level"
        keywords "water", "osm to gis"
        inputs filterArea: Object, datasource: JdbcDataSource, dataDim: [2]
        outputs datasource: JdbcDataSource, outputPolygonsTableName: String, outputLinesTableName: String, outputPointsTableName: String
        run { filterArea, datasource, dataDim ->
            def tags = ['water', 'waterway', 'amenity']
            extraction(datasource, filterArea, dataDim, tags)
        }
    })
}