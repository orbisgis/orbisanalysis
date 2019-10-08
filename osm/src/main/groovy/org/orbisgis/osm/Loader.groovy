package org.orbisgis.osm

import groovy.transform.BaseScript
import org.h2gis.functions.spatial.crs.ST_Transform
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Polygon
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

import static org.orbisgis.osm.OSMElement.NODE
import static org.orbisgis.osm.OSMElement.RELATION
import static org.orbisgis.osm.OSMElement.WAY


@BaseScript OSMTools osmTools


/**
 * This process extracts OSM data file and load it in a database using an area
 * The area can be an envelope or a polygon
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
IProcess fromArea() {
        return create({
            title "Extract the OSM data using an area"
            inputs datasource: JdbcDataSource, filterArea: Object, distance : 0
            outputs zoneTableName: String, zoneEnvelopeTableName: String, osmTablesPrefix: String, epsg: int
            run { datasource, filterArea,distance ->
                if (filterArea == null) {
                    logger.error( "Filter area not defined")
                    return null
                } else {
                    def outputZoneTable = "ZONE_${UUID.randomUUID().toString().replaceAll("-", "_")}"
                    def outputZoneEnvelopeTable = "ZONE_ENVELOPE_${UUID.randomUUID().toString().replaceAll("-", "_")}"
                    def osmTablesPrefix = "OSM_DATA_${UUID.randomUUID().toString().replaceAll("-", "_")}"
                    Geometry  geom = null
                    if(filterArea instanceof Envelope ) {
                        geom  = new GeometryFactory().toGeometry(filterArea)
                        geom.setSRID(4326)
                    }
                    else if( filterArea instanceof Polygon ) {
                        geom = filterArea
                        geom.setSRID(4326)
                    }
                    else{
                        logger.error "The filter area must be an Envelope or a Polygon"
                        return null
                    }
                    if (geom) {
                        /**
                         * Extract the OSM file from the envelope of the geometry
                         */
                        def geomAndEnv = OSMTools.Utilities.buildGeometryAndZone(geom, -1, 0, datasource)
                        epsg = geomAndEnv.geom.getSRID()

                        //Create table to store the geometry and the envelope of the extracted area
                        datasource.execute """create table ${outputZoneTable} (the_geom GEOMETRY(POLYGON, $epsg));
            INSERT INTO ${outputZoneTable} VALUES (ST_GEOMFROMTEXT('${geomAndEnv.geom.toString()
                        }', $epsg));"""


                        datasource.execute """create table ${outputZoneEnvelopeTable} (the_geom GEOMETRY(POLYGON, $epsg));
            INSERT INTO ${outputZoneEnvelopeTable} VALUES (ST_GEOMFROMTEXT('${ST_Transform.ST_Transform(datasource.getConnection(), geomAndEnv.filterArea, epsg).toString()
                        }',$epsg));"""

                        def query = OSMTools.Utilities.buildOSMQuery(geomAndEnv.filterArea, [], NODE, WAY, RELATION)

                        def extract = OSMTools.Loader.extract()
                        if (extract.execute(overpassQuery: query)) {
                            logger.info "Downloading OSM data from the area ${filterArea}"
                            def load = OSMTools.Loader.load()
                            if (load(datasource: datasource, osmTablesPrefix: osmTablesPrefix, osmFilePath: extract.results.outputFilePath)) {
                                logger.info "Loading OSM data from the area ${filterArea}"
                                return [zoneTableName        : outputZoneTable,
                                 zoneEnvelopeTableName: outputZoneEnvelopeTable,
                                 osmTablesPrefix      : osmTablesPrefix,
                                 epsg:epsg]
                            } else {
                                logger.error "Cannot load the OSM data from the area ${filterArea}"
                                return null
                            }

                        } else {
                            logger.error "Cannot download OSM data from the area ${filterArea}"
                            return null
                        }


                    } else {
                        logger.error("Cannot find an area from the area ${filterArea}")
                        return null
                    }
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
        inputs datasource: JdbcDataSource, placeName: String, distance : 0
        outputs zoneTableName: String, zoneEnvelopeTableName: String, osmTablesPrefix: String, epsg:int
        run { datasource,placeName,distance ->
            String formatedPlaceName = placeName.trim().split("\\s*(,|\\s)\\s*").join("_");
            def outputZoneTable = "ZONE_${formatedPlaceName}_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def outputZoneEnvelopeTable = "ZONE_ENVELOPE_${formatedPlaceName}_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def osmTablesPrefix = "OSM_DATA_${formatedPlaceName}_${UUID.randomUUID().toString().replaceAll("-", "_")}"

            Geometry geom = OSMTools.Utilities.getAreaFromPlace(placeName);
            if (geom) {
                /**
                 * Extract the OSM file from the envelope of the geometry
                 */
                def geomAndEnv = OSMTools.Utilities.buildGeometryAndZone(geom, -1, 0, datasource)
                epsg = geomAndEnv.geom.getSRID()

                //Create table to store the geometry and the envelope of the extracted area
                datasource.execute """create table ${outputZoneTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
            INSERT INTO ${outputZoneTable} VALUES (ST_GEOMFROMTEXT('${geomAndEnv.geom.toString()}', $epsg), '$placeName');"""


                datasource.execute """create table ${outputZoneEnvelopeTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
            INSERT INTO ${outputZoneEnvelopeTable} VALUES (ST_GEOMFROMTEXT('${ST_Transform.ST_Transform(datasource.getConnection(), geomAndEnv.filterArea, epsg).toString()}',$epsg), '$placeName');"""

                def query = OSMTools.Utilities.buildOSMQuery(geomAndEnv.filterArea, [], NODE, WAY, RELATION)

                def extract = OSMTools.Loader.extract()
                if (extract.execute(overpassQuery: query)) {
                    logger.info "Downloading OSM data from the place ${placeName}"
                    def load = OSMTools.Loader.load()
                    if (load(datasource: datasource, osmTablesPrefix: osmTablesPrefix, osmFilePath: extract.results.outputFilePath)) {
                        logger.info "Loading OSM data from the place ${placeName}"
                        [zoneTableName    : outputZoneTable,
                         zoneEnvelopeTableName    : outputZoneEnvelopeTable,
                         osmTablesPrefix:osmTablesPrefix,
                         epsg: epsg]
                    }
                    else{
                        logger.error "Cannot load the OSM data from the place ${placeName}"
                        return null
                    }

                }
                else{
                    logger.error "Cannot download OSM data from the place ${placeName}"
                    return null
                }


            } else {
                logger.error("Cannot find an area from the place name ${placeName}")
                return null
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
            if (executeOverPassQuery(overpassQuery, tmpOSMFile)) {
                info "The OSM file has been downloaded at ${osmFilePath}."
            } else {
                error "Cannot extract the OSM data for the query $overpassQuery"
                return null
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
        run { datasource, osmTablesPrefix, osmFilePath ->
            if (datasource != null) {
                info "Load the OSM file in the database"
                File osmFile = new File(osmFilePath)
                if (osmFile.exists()) {
                    datasource.load(osmFile, osmTablesPrefix, true)
                    info "The input OSM file has been loaded in the database"
                } else {
                    error "The input OSM file does not exist"
                    return null
                }
            } else {
                error "Please set a valid database connection"
                return null
            }
            [datasource: datasource]
        }
    })
}


/**
 * Method to execute an Overpass query and save the result in a file
 *
 * @param query the Overpass query
 * @param outputOSMFile the output file
 *
 * @return True if the query has been successfully executed, false otherwise.
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
static boolean executeOverPassQuery(def query, def outputOSMFile) {
    outputOSMFile.delete()
    def overpassBaseUrl = "https://overpass-api.de/api/interpreter?data="
    def queryUrl = new URL(overpassBaseUrl + utf8ToUrl(query))
    def connection = queryUrl.openConnection() as HttpURLConnection

    info queryUrl

    connection.requestMethod = "GET"

    info "Executing query... $query"
    //Save the result in a file
    if (connection.responseCode == 200) {
        info "Downloading the OSM data from overpass api in ${outputOSMFile}"
        outputOSMFile << connection.inputStream
        return true
    }
    else if(connection.responseCode in [429, 504]) {
        error "Please check bellow the status of Overpass server \n${getSERVER_STATUS()}"

    }else {
        error "Cannot execute the query"
        return false
    }
}

