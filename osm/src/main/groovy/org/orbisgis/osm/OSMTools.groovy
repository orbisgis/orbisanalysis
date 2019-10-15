package org.orbisgis.osm

import org.orbisgis.osm.utils.OverpassStatus
import org.orbisgis.processmanager.GroovyProcessFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static java.nio.charset.StandardCharsets.UTF_8

/**
 * Main script to access to all processes used to extract, transform and save OSM data as GIS layers
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 */
abstract class OSMTools extends GroovyProcessFactory {

    private static final OVERPASS_STATUS_URL = "http://overpass-api.de/api/status"
    public static Logger logger = LoggerFactory.getLogger(OSMTools.class)

    //Process scripts
    public static Loader = new Loader()
    public static Transform = new Transform()
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
    static def getServerStatus()  {
        def connection = new URL(OVERPASS_STATUS_URL).openConnection() as HttpURLConnection
        connection.requestMethod = "GET"
        if (connection.responseCode == 200) {
            def content = connection.inputStream.text
            return content
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
        def status = new OverpassStatus(getServerStatus())
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

}