package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript OSMHelper osmHelper


/**
 * This process extracts OSM data as an XML file using the Overpass API
 * @param overpassQuery The overpass api to be executed
 */
static IProcess extract() {
    return processFactory.create(
            "Extract the OSM data using the overpass api and save the result in an XML file",
            [
             overpassQuery :String
            ],
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
 */
static IProcess load() {
    return processFactory.create(
            "Load an OSM file to the current database",
            [datasource: JdbcDataSource,
             osmTablesPrefix: String,
             osmFilePath :String
            ],
            [datasource : JdbcDataSource],
            { datasource, osmTablesPrefix, osmFilePath  ->
                if (datasource != null) {
                    logger.info('Load the OSM file in the database')
                    File osmFile = new File(osmFilePath)
                    if (osmFile.exists()) {
                        datasource.load(osmFile.absolutePath, osmTablesPrefix, true)
                        datasource.execute createIndexesOnOSMTables(osmTablesPrefix)
                        logger.info('The input OSM file has been loaded in the database')
                    } else {
                        logger.info('The input OSM file does not exist')
                    }
                }
                else{
                    logger.info('Please set a valid database connection')
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
 */
static boolean executeOverPassQuery(def query, def outputOSMFile) {
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
        logger.error  "Cannot execute the query"
        return false
    }
}


/**
 ** Function to prepare the script to index the tables from OSM
 * @param prefix prefix of the OSM tables
 **/
static def createIndexesOnOSMTables(def prefix){
    def script = """
            CREATE INDEX IF NOT EXISTS ${prefix}_node_index on ${prefix}_node(id_node);
            CREATE INDEX IF NOT EXISTS ${prefix}_way_node_index on ${prefix}_way_node(id_node);
            CREATE INDEX IF NOT EXISTS ${prefix}_way_node_index2 on ${prefix}_way_node(node_order);
            CREATE INDEX IF NOT EXISTS ${prefix}_way_node_index3 ON ${prefix}_way_node(id_way);
            CREATE INDEX IF NOT EXISTS ${prefix}_way_index on ${prefix}_way(id_way);
            CREATE INDEX IF NOT EXISTS ${prefix}_way_tag_id_index on ${prefix}_way_tag(id_tag);
            CREATE INDEX IF NOT EXISTS ${prefix}_way_tag_va_index on ${prefix}_way_tag(id_way);
            CREATE INDEX IF NOT EXISTS ${prefix}_tag_id_index on ${prefix}_tag(id_tag);
            CREATE INDEX IF NOT EXISTS ${prefix}_tag_key_index on ${prefix}_tag(tag_key);
            CREATE INDEX IF NOT EXISTS ${prefix}_relation_tag_tag_index ON ${prefix}_relation_tag(id_tag);
            CREATE INDEX IF NOT EXISTS ${prefix}_relation_tag_tag_index2 ON ${prefix}_relation_tag(id_relation);
            CREATE INDEX IF NOT EXISTS ${prefix}_relation_tag_tag_index ON ${prefix}_relation_tag(tag_value);
            CREATE INDEX IF NOT EXISTS ${prefix}_relation_tag_rel_index ON ${prefix}_relation(id_relation);
            CREATE INDEX IF NOT EXISTS ${prefix}_way_member_index ON ${prefix}_way_member(id_relation);
            """
    return script
}
