package org.orbisgis.osm

import groovy.transform.BaseScript
import org.h2gis.functions.spatial.crs.ST_Transform
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Polygon
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

import java.util.regex.Pattern

import static org.orbisgis.osm.utils.OSMElement.NODE
import static org.orbisgis.osm.utils.OSMElement.RELATION
import static org.orbisgis.osm.utils.OSMElement.WAY

@BaseScript OSMTools osmTools

/**
 * This process extracts OSM data file and load it in a database using an area
 * The area can be an envelope or a polygon
 *
 * @param datasource A connexion to a DB to load the OSM file
 * @param filterArea Filtering area
 * @param distance to expand the envelope of the query box. Default is 0
 *
 * @return The name of the tables that contains the geometry representation of the extracted area (outputZoneTable) and
 * its envelope (outputZoneEnvelopeTable)
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess fromArea() {
    return create({
        title "Extract the OSM data using an area"
        inputs datasource: JdbcDataSource, filterArea: Object, distance : 0
        outputs zoneTableName: String, zoneEnvelopeTableName: String, osmTablesPrefix: String, epsg: int
        run { JdbcDataSource datasource, filterArea, distance ->
            if (!filterArea) {
                error( "Filter area not defined")
                return
            }
            def outputZoneTable = "ZONE_$uuid"
            def outputZoneEnvelopeTable = "ZONE_ENVELOPE_$uuid"
            def osmTablesPrefix = "OSM_DATA_$uuid"
            def geom
            if(filterArea instanceof Envelope ) {
                geom  = new GeometryFactory().toGeometry(filterArea)
            }
            else if( filterArea instanceof Polygon ) {
                geom = filterArea
            }
            else{
                error "The filter area must be an Envelope or a Polygon"
                return
            }
            if(geom.SRID <= 0) {
                geom.SRID = DEFAULT_SRID
            }
             // Extract the OSM file from the envelope of the geometry
            def geomAndEnv = OSMTools.Utilities.buildGeometryAndZone(geom, distance, datasource)
            def epsg = geomAndEnv.geom.SRID

            //Create table to store the geometry and the envelope of the extracted area
            datasource.execute "CREATE TABLE $outputZoneTable (the_geom GEOMETRY(POLYGON, $epsg));" +
                    " INSERT INTO $outputZoneTable VALUES (ST_GEOMFROMTEXT('${geomAndEnv.geom}', $epsg));"

            def text = ST_Transform.ST_Transform(datasource.connection, geomAndEnv.filterArea, epsg)
            datasource.execute "CREATE TABLE $outputZoneEnvelopeTable (the_geom GEOMETRY(POLYGON, $epsg));" +
                    "INSERT INTO $outputZoneEnvelopeTable VALUES " +
                        "(ST_GEOMFROMTEXT('$text',$epsg));"

            def query = OSMTools.Utilities.buildOSMQuery(geomAndEnv.filterArea, [], NODE, WAY, RELATION)

            def extract = OSMTools.Loader.extract()
            if (extract(overpassQuery: query)) {
                info "Downloading OSM data from the area $filterArea"
                def load = OSMTools.Loader.load()
                if (load(datasource     : datasource,
                        osmTablesPrefix : osmTablesPrefix,
                        osmFilePath     : extract.results.outputFilePath)) {
                    info "Loading OSM data from the area $filterArea"
                    return [zoneTableName         : outputZoneTable,
                            zoneEnvelopeTableName : outputZoneEnvelopeTable,
                            osmTablesPrefix       : osmTablesPrefix,
                            epsg                  : epsg]
                } else {
                    error "Cannot load the OSM data from the area $filterArea"
                }

            } else {
                error "Cannot download OSM data from the area $filterArea"
            }
        }
    })
}


/**
 * This process extracts OSM data file and load it in a database using a place name
 *
 * @param datasource A connexion to a DB to load the OSM file
 * @param placeName The name of the place to extract
 * @param distance to expand the envelope of the query box. Default is 0
 *
 * @return The name of the tables that contains the geometry representation of the extracted area (outputZoneTable) and
 * its envelope (outputZoneEnvelopeTable)
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess fromPlace() {
    return create({
        title "Extract the OSM data using a place name"
        inputs datasource: JdbcDataSource, placeName: String, distance: 0
        outputs zoneTableName: String, zoneEnvelopeTableName: String, osmTablesPrefix: String, epsg: int
        run { JdbcDataSource datasource, placeName, distance ->
            def formatedPlaceName = placeName.trim().replaceAll("(\\s|,|-|\$)+", "_")
            def outputZoneTable = "ZONE_$formatedPlaceName$uuid"
            def outputZoneEnvelopeTable = "ZONE_ENVELOPE_$formatedPlaceName$uuid"
            def osmTablesPrefix = "OSM_DATA_$formatedPlaceName$uuid"

            def geom = OSMTools.Utilities.getAreaFromPlace(placeName);
            if (!geom) {
                error("Cannot find an area from the place name $placeName")
                return
            }
            //Extract the OSM file from the envelope of the geometry
            def geomAndEnv = OSMTools.Utilities.buildGeometryAndZone(geom, distance, datasource)
            def epsg = geomAndEnv.geom.SRID

            //Create table to store the geometry and the envelope of the extracted area
            datasource.execute "CREATE TABLE $outputZoneTable (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);" +
                    "INSERT INTO $outputZoneTable VALUES (ST_GEOMFROMTEXT('${geomAndEnv.geom}', $epsg), '$placeName');"

            def text = ST_Transform.ST_Transform(datasource.getConnection(), geomAndEnv.filterArea, epsg)
            datasource.execute "CREATE TABLE $outputZoneEnvelopeTable (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);" +
                    "INSERT INTO $outputZoneEnvelopeTable VALUES (ST_GEOMFROMTEXT('$text',$epsg), '$placeName');"

            def query = OSMTools.Utilities.buildOSMQuery(geomAndEnv.filterArea, [], NODE, WAY, RELATION)

            def extract = OSMTools.Loader.extract()
            if (extract(overpassQuery: query)) {
                info "Downloading OSM data from the place $placeName"
                def load = OSMTools.Loader.load()
                if (load(datasource: datasource,
                        osmTablesPrefix: osmTablesPrefix,
                        osmFilePath: extract.results.outputFilePath)) {
                    info "Loading OSM data from the place $placeName"
                    return [zoneTableName        : outputZoneTable,
                            zoneEnvelopeTableName: outputZoneEnvelopeTable,
                            osmTablesPrefix      : osmTablesPrefix,
                            epsg                 : epsg]
                } else {
                    error "Cannot load the OSM data from the place $placeName"
                }

            } else {
                error "Cannot download OSM data from the place $placeName"
            }
        }
    })
}

/**
 * This process extracts OSM data as an XML file using the Overpass API
 *
 * @param overpassQuery The overpass api to be executed
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess extract() {
    return create({
        title "Extract the OSM data using the overpass api and save the result in an XML file"
        inputs overpassQuery: String
        outputs outputFilePath: String
        run { overpassQuery ->
            info "Extract the OSM data"
            def tmpOSMFile = File.createTempFile("extract_osm", ".osm")
            def osmFilePath = tmpOSMFile.absolutePath
            if (overpassQuery && executeOverPassQuery(overpassQuery, tmpOSMFile)) {
                info "The OSM file has been downloaded at $osmFilePath."
            } else {
                error "Cannot extract the OSM data for the query $overpassQuery"
                return
            }
            [outputFilePath: osmFilePath]
        }
    })
}

/**
 * This process is used to load an OSM file in a database.
 *
 * @param datasource A connection to a database
 * @param osmTablesPrefix A prefix to identify the 10 OSM tables
 * @param omsFilePath The path where the OSM file is
 *
 * @return datasource The connection to the database
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess load() {
    return create({
        title "Load an OSM file to the current database"
        inputs datasource: JdbcDataSource, osmTablesPrefix: String, osmFilePath: String
        outputs datasource: JdbcDataSource
        run { JdbcDataSource datasource, osmTablesPrefix, osmFilePath ->
            if(!datasource) {
                error "Please set a valid database connection."
                return
            }

            if(osmTablesPrefix == null ||
                    !Pattern.compile('^[a-zA-Z0-9_]*$').matcher(osmTablesPrefix).matches()) {
                error "Please set a valid table prefix."
                return
            }

            if(!osmFilePath){
                error "Please set a valid osm file path."
                return
            }
            File osmFile = new File(osmFilePath)
            if (!osmFile.exists()) {
                error "The input OSM file does not exist."
                return
            }

            info "Load the OSM file in the database."
            datasource.load(osmFile, osmTablesPrefix, true)
            info "The input OSM file has been loaded in the database."

            [datasource: datasource]
        }
    })
}

