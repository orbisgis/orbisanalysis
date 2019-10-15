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
    static def uuidCl = { UUID.randomUUID().toString().replaceAll("-", "_") }
    /** {@link Closure} converting and UTF-8 {@link String} into an {@link URL}. */
    static def utf8ToUrl = { utf8 -> URLEncoder.encode(utf8, UTF_8.toString()) }
    /** {@link Closure} logging with INFO level the given {@link Object} {@link String} representation. */
    static def info = { obj -> logger.info(obj.toString()) }
    /** {@link Closure} logging with WARN level the given {@link Object} {@link String} representation. */
    static def warn = { obj -> logger.warn(obj.toString()) }
    /** {@link Closure} logging with ERROR level the given {@link Object} {@link String} representation. */
    static def error = { obj -> logger.error(obj.toString()) }

    /**
     * Return the status of the Overpass server.
     * @return A {@link OverpassStatus} instance.
     */
    static def getServerStatus()  {
        def connection = new URL(OVERPASS_STATUS_URL).openConnection() as HttpURLConnection
        connection.requestMethod = "GET"
        if (connection.responseCode == 200) {
            def content = connection.inputStream.text
            return new OverpassStatus(content)
        } else {
            error "Cannot get the status of the server"
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
     * Handle the calling of a missing property.
     *
     * @param name Name of the property missing.
     *
     * @return The property if found, throw a {@link MissingPropertyException} otherwise.
     *
     * @throws MissingPropertyException Thrown if not able to find a property with the given name.
     */
    def propertyMissing(String name) throws MissingPropertyException {
        switch(name) {
            case "uuid":
                return uuidCl()
            case "serverStatus":
                return getServerStatus()
            default:
                throw new MissingPropertyException(name, this.class)
        }
    }

    /**
     * Handle the calling of a static missing property.
     *
     * @param name Name of the property missing.
     *
     * @return The property if found, throw a {@link MissingPropertyException} otherwise.
     *
     * @throws MissingPropertyException Thrown if not able to find a property with the given name.
     */
    static def $static_propertyMissing(String name) throws MissingPropertyException {
        switch(name) {
            case "uuid":
                return uuidCl()
            default:
                throw new MissingPropertyException(name, this.class)
        }
    }
}