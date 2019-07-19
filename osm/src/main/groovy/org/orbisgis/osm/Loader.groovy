package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript OSMHelper osmHelper


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
        run { datasource, osmTablesPrefix, osmFilePath ->
            if (datasource != null) {
                info "Load the OSM file in the database"
                File osmFile = new File(osmFilePath)
                if (osmFile.exists()) {
                    datasource.load(osmFile, osmTablesPrefix, true)
                    info "The input OSM file has been loaded in the database"
                } else {
                    error "The input OSM file does not exist"
                }
            } else {
                error "Please set a valid database connection"
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
    } else {
        error "Cannot execute the query"
        return false
    }
}

