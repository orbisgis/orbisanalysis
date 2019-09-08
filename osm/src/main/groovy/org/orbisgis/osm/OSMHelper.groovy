package org.orbisgis.osm

import org.orbisgis.processmanager.GroovyProcessFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static java.nio.charset.StandardCharsets.UTF_8

/**
 * Main script to access to all processes used to extract, transform and save OSM data as GIS layers
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 */
abstract class OSMHelper extends GroovyProcessFactory {

    public static Logger logger = LoggerFactory.getLogger(OSMHelper.class)

    //Process scripts
    public static Loader = new Loader()
    public static Transform = new Transform()
    public static OSMTemplate = new OSMTemplate()
    public static Utilities = new Utilities()

    //Utility methods
    static def getUuid() { UUID.randomUUID().toString().replaceAll("-", "_") }
    static def uuid = { UUID.randomUUID().toString().replaceAll("-", "_") }
    static def utf8ToUrl = { utf8 -> URLEncoder.encode(utf8, UTF_8.toString()) }
    static def info = { obj -> logger.info(obj.toString()) }
    static def warn = { obj -> logger.warn(obj.toString()) }
    static def error = { obj -> logger.error(obj.toString()) }

    /**
     * Return the status of the Overpass server
     * @return
     */
    static def getSERVER_STATUS()  {
        def connection = new URL("http://overpass-api.de/api/status").openConnection() as HttpURLConnection
        connection.requestMethod = "GET"
        if (connection.responseCode == 200) {
            def content = connection.inputStream.text
            return content
        } else {
            error "Cannot get the status of the server"
        }
    }


}