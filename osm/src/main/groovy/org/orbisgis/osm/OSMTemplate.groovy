package org.orbisgis.osm

import groovy.transform.BaseScript
import org.h2gis.utilities.SFSUtilities
import org.locationtech.jts.geom.Coordinate
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
 * @param filterArea Area to extract. Must be specificied *
 * @param epsg as integer value
 * Default value is -1. If the default value is used the process will find the best UTM projection
 * according the interior point of the filterArea
 * @param dataDim Dimension of the data to extract. It should be an array with the value 0, 1, 2.
 * @param tagsKeys Array of tagsKeys to extract.
 *
 * @return Map containing the datasource with the key 'datasource', the name of the output polygon table with the key
 * 'outputPolygonsTableName', the name of the output line table with the key 'outputLinesTableName', the name of the
 * output point table with the key 'outputPointsTableName'
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
private static def extraction(datasource, filterArea, epsg, dataDim, tagsKeys) {
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
    Coordinate interiorPoint
    if(filterArea instanceof Envelope ) {
        query = OSMHelper.Utilities.buildOSMQuery(filterArea, tagsKeys, NODE, WAY, RELATION)
        interiorPoint = filterArea.centre()
        epsg = SFSUtilities.getSRID(datasource.getConnection(), interiorPoint.y as float, interiorPoint.x as float)
    }
    else if( filterArea instanceof Polygon ) {
        query = OSMHelper.Utilities.buildOSMQuery(filterArea, tagsKeys, NODE, WAY, RELATION)
        interiorPoint= filterArea.getCentroid().getCoordinate()
        epsg = SFSUtilities.getSRID(datasource.getConnection(), interiorPoint.y as float, interiorPoint.x as float)
    }
    else {
        error "The filter area must a JTS Envelope or a Polygon"
    }
    if(epsg!=-1){

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
                    assert transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tagKeys: tagsKeys)
                    outputPointsTableName = transform.results.outputTableName
                }
                if (dataDim.contains(1)) {
                    def transform = OSMHelper.Transform.extractWaysAsLines()
                    info "Transforming lines"
                    assert transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tagKeys: tagsKeys)
                    outputLinesTableName = transform.results.outputTableName
                }
                if (dataDim.contains(2)) {
                    def transform = OSMHelper.Transform.toPolygons()
                    info "Transforming polygons"
                    assert transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tagKeys: tagsKeys)
                    outputPolygonsTableName = transform.results.outputTableName
                }
                return [datasource             : datasource,
                        outputPolygonsTableName: outputPolygonsTableName,
                        outputPointsTableName  : outputPointsTableName,
                        outputLinesTableName   : outputLinesTableName]
            }
        }
         }
        else{
            error "Invalid EPSG code : $epsg"
            return null
        }
    }
    return null
}

/**
 * Create a building GIS table
 *
 *
 * @param filterArea an area to extract OSM data using Overpass
 * the area must be a JTS envelope or a JTS polygon
 * @param epsg as integer value
 * Default value is -1. If the default value is used the process will find the best UTM projection
 * according the centroid of the filterArea
 * @param datasource a connection to a spatial database supported by OrbisData.
 * If null a local H2GIS data source is created.
 * @param dataDim an array of integer values to specify the output dimension of the GIS tables
 * [0, 1, 2] will return 3 GIS tables, point, line and polygon
 *
 * @return A set of tables stored in the input database with the following names :
 * BUILDING_POINT_{timestamp},BUILDING_LINE_{timestamp},BUILDING_POLYGON_{timestamp}
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess BUILDING() {
    return create({
        title "Building extraction for a specified place/area"
        inputs filterArea: Object, epsg:-1, datasource: JdbcDataSource, dataDim: [2]
        outputs datasource: JdbcDataSource, outputPolygonsTableName: String, outputPointsTableName: String, outputLinesTableName: String
        run { filterArea, epsg, datasource, dataDim ->
            def tagsKeys = ['building', 'amenity', 'layer', 'aeroway', 'historic', 'leisure', 'monument', 'place_of_worship',
                            'military', 'railway', 'public_transport', 'barrier', 'government', 'historic:building',
                            'grandstand', 'apartments', 'ruins', 'agricultural', 'barn', 'healthcare', 'education', 'restaurant',
                            'sustenance', 'office']
            extraction(datasource, filterArea, epsg, dataDim, tagsKeys)
        }
    })

}

/**
 * Extract landcover
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
        inputs filterArea: Object, epsg:-1,datasource: JdbcDataSource, dataDim: [2]
        outputs datasource: JdbcDataSource, outputPolygonsTableName: String, outputPointsTableName: String, outputLinesTableName: String
        run { filterArea, epsg, datasource, dataDim ->
            def tagsKeys = ['landcover', 'natural', 'landuse', 'water', 'waterway', 'leisure', 'aeroway', 'amenity']
            extraction(datasource, filterArea, epsg,dataDim, tagsKeys)
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
        inputs filterArea: Object, epsg:-1,datasource: JdbcDataSource, dataDim: [2]
        outputs datasource: JdbcDataSource, outputPolygonsTableName: String, outputLinesTableName: String, outputPointsTableName: String
        run { filterArea, epsg,datasource, dataDim ->
            def tagsKeys = ['water', 'waterway', 'amenity']
            extraction(datasource, filterArea, epsg, dataDim, tagsKeys)
        }
    })
}