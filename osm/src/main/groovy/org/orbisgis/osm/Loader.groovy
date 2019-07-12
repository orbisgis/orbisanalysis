package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript OSMHelper osmHelper


/**
 * This process extracts OSM data as an XML file using the Overpass API
 * @param overpassQuery The overpass api to be executed
 *
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 */
static IProcess extract() {
    return processFactory.create(
        "Extract the OSM data using the overpass api and save the result in an XML file",
        [ overpassQuery :String],
        [outputFilePath : String],
            { overpassQuery  ->
            logger.info('Extract the OSM data')
            def tmpOSMFile = File.createTempFile("extract_osm", ".osm")
            def osmFilePath =  tmpOSMFile.absolutePath
            if(executeOverPassQuery(overpassQuery, tmpOSMFile)){
                logger.info("The OSM file has been downloaded at ${osmFilePath}.")
            }
            else{
                logger.error("Cannot extract the OSM data for the query $overpassQuery")
                return
            }
            [outputFilePath : osmFilePath]
        }
    )
}




/**
 * This process is used to load an OSM file in a database.
 *
 * @param datasource A connection to a database
 * @param osmTablesPrefix A prefix to identify the 10 OSM tables
 * @param omsFilePath The path where the OSM file is
 * @return datasource The connection to the database
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 */
static IProcess load() {
    return processFactory.create(
         "Load an OSM file to the current database",
        [datasource: JdbcDataSource, osmTablesPrefix: String, osmFilePath :String],
        [ datasource : JdbcDataSource],
         { datasource, osmTablesPrefix, osmFilePath  ->
            if (datasource != null) {
                logger.info('Load the OSM file in the database')
                File osmFile = new File(osmFilePath)
                if (osmFile.exists()) {
                    datasource.load(osmFile.absolutePath, osmTablesPrefix, true)
                    logger.info('The input OSM file has been loaded in the database')
                } else {
                    logger.error('The input OSM file does not exist')
                }
            }
            else{
                logger.error('Please set a valid database connection')
            }
            [ datasource : datasource]
        }
    )
}


/**
 * Method to execute an Overpass query and save the result in a file
 * @param query the Overpass query
 * @param outputOSMFile the output file
 * @return
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Lesaux UBS LAB-STICC
 */
static boolean executeOverPassQuery( def query,  def outputOSMFile) {
    if (outputOSMFile.exists()) {
        outputOSMFile.delete()
    }
    def apiUrl = "https://overpass-api.de/api/interpreter?data="
    def connection = new URL(apiUrl + URLEncoder.encode(query, java.nio.charset.StandardCharsets.UTF_8.toString())).openConnection() as HttpURLConnection

    logger.info apiUrl + URLEncoder.encode(query, java.nio.charset.StandardCharsets.UTF_8.toString())

    connection.setRequestMethod("GET")

    logger.info "Executing query... $query"
    //Save the result in a file
    if (connection.responseCode == 200) {
        logger.info "Downloading the OSM data from overpass api"
        outputOSMFile << connection.inputStream
        return true
    } else {
        logger.error "Cannot execute the query"
        return false
    }
}

