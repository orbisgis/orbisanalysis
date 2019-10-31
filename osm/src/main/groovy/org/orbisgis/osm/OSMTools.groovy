package org.orbisgis.osm

import org.orbisgis.osm.utils.OverpassStatus
import org.orbisgis.processmanager.GroovyProcessFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static java.nio.charset.StandardCharsets.UTF_8

/**
 * Main script to access to all processes used to extract, transform and save OSM data as GIS layers.
 *
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */
abstract class OSMTools extends GroovyProcessFactory {

    /** Url of the status of the Overpass server */
    private static final OVERPASS_STATUS_URL = "http://overpass-api.de/api/status"
    /** {@link Logger} */
    public static Logger logger = LoggerFactory.getLogger(OSMTools.class)

    /* *********************** */
    /*     Process scripts     */
    /* *********************** */
    /** {@link Loader} script with its {@link Process}. */
    public static Loader = new Loader()
    /** {@link Transform} script with its {@link Process}. */
    public static Transform = new Transform()
    /** {@link Utilities} script with its {@link Process}. */
    public static Utilities = new Utilities()

    /* *********************** */
    /*     Utility methods     */
    /* *********************** */
    /** {@link Closure} returning a {@link String} prefix/suffix build from a random {@link UUID} with '-' replaced by '_'. */
    static def getUuid() { UUID.randomUUID().toString().replaceAll("-", "_") }
    static def uuid = {getUuid()}
    /** {@link Closure} converting and UTF-8 {@link String} into an {@link URL}. */
    static def utf8ToUrl = { utf8 -> URLEncoder.encode(utf8, UTF_8.toString()) }
    /** {@link Closure} logging with INFO level the given {@link Object} {@link String} representation. */
    static def info = { obj -> logger.info(obj.toString()) }
    /** {@link Closure} logging with WARN level the given {@link Object} {@link String} representation. */
    static def warn = { obj -> logger.warn(obj.toString()) }
    /** {@link Closure} logging with ERROR level the given {@link Object} {@link String} representation. */
    static def error = { obj -> logger.error(obj.toString()) }

    /** Default SRID */
    static def DEFAULT_SRID = 4326
    /** Null SRID */
    static def NULL_SRID = -1
    /** Get method for HTTP request */
    private static def GET = "GET"
    /** Overpass server base URL */
    static def OVERPASS_BASE_URL = "https://overpass-api.de/api/interpreter?data="

    /**
     * Return the status of the Overpass server.
     * @return A {@link OverpassStatus} instance.
     */
    static def getServerStatus()  {
        def connection = new URL(OVERPASS_STATUS_URL).openConnection() as HttpURLConnection
        connection.requestMethod = GET
        if (connection.responseCode == 200) {
            def content = connection.inputStream.text
            return new OverpassStatus(content)
        } else {
            error "Cannot get the status of the server.\n Server answer with code ${connection.responseCode} : " +
                    "${connection.inputStream.text}"
        }
    }

    /**
     * Wait for a free overpass slot.
     * @param timeout Timeout to limit the waiting.
     * @return True if there is a free slot, false otherwise.
     */
    static def wait(int timeout)  {
        def to = timeout
        def status = getServerStatus()
        info("Try to wait for slot available")
        if(!status.waitForSlot(timeout)){
            //Case of too low timeout for slot availibility
            if(status.slotWaitTime > to){
                error("Wait timeout is lower than the wait time for a slot.")
                return false
            }
            info("Wait for query end")
            if(!status.waitForQueryEnd(to)){
                error("Wait timeout is lower than the wait time for a query end.")
                return false
            }
            to -= status.queryWaitTime
            //Case of too low timeout for slot availibility
            if(status.slotWaitTime > to){
                error("Wait timeout is lower than the wait time for a slot.")
                return false
            }
            info("Wait for slot available")
            return status.waitForSlot(timeout)
        }
        return true
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
        def queryUrl = new URL(OVERPASS_BASE_URL + utf8ToUrl(query))
        def connection = queryUrl.openConnection() as HttpURLConnection

        info queryUrl

        connection.requestMethod = GET

        info "Executing query... $query"
        //Save the result in a file
        if (connection.responseCode == 200) {
            info "Downloading the OSM data from overpass api in ${outputOSMFile}"
            outputOSMFile << connection.inputStream
            return true
        }
        else {
            error "Cannot execute the query.\n${getServerStatus()}"
            return false
        }
    }
}