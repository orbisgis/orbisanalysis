/*
 * Bundle OSM is part of the OrbisGIS platform
 *
 * OrbisGIS is a java GIS application dedicated to research in GIScience.
 * OrbisGIS is developed by the GIS group of the DECIDE team of the
 * Lab-STICC CNRS laboratory, see <http://www.lab-sticc.fr/>.
 *
 * The GIS group of the DECIDE team is located at :
 *
 * Laboratoire Lab-STICC – CNRS UMR 6285
 * Equipe DECIDE
 * UNIVERSITÉ DE BRETAGNE-SUD
 * Institut Universitaire de Technologie de Vannes
 * 8, Rue Montaigne - BP 561 56017 Vannes Cedex
 *
 * OSM is distributed under LGPL 3 license.
 *
 * Copyright (C) 2019 CNRS (Lab-STICC UMR CNRS 6285)
 *
 *
 * OSM is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * OSM is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * OSM. If not, see <http://www.gnu.org/licenses/>.
 *
 * For more information, please consult: <http://www.orbisgis.org/>
 * or contact directly:
 * info_at_ orbisgis.org
 */
package org.orbisgis.orbisanalysis.osm

import org.orbisgis.orbisanalysis.osm.utils.OverpassStatus
import org.orbisgis.orbisanalysis.osm.utils.Utilities
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessFactory
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

    OSMTools(){
        def clazz = this.class
        if(!(clazz in LOGGER_MAP)){
            LOGGER_MAP.put(clazz, LoggerFactory.getLogger(clazz))
        }
    }

    /** Url of the status of the Overpass server */
    private static final OVERPASS_STATUS_URL = "http://overpass-api.de/api/status"
    /** {@link Logger} */
    public static Logger DEFAULT_LOGGER = LoggerFactory.getLogger(OSMTools)
    public static Map<Class, Logger> LOGGER_MAP = new HashMap<>()

    /* *********************** */
    /*     Process scripts     */
    /* *********************** */
    /** {@link Loader} script with its {@link Process}. */
    public static Loader = new Loader()
    /** {@link Transform} script with its {@link Process}. */
    public static Transform = new Transform()
    /** {@link org.orbisgis.orbisanalysis.osm.utils.Utilities} script with its {@link Process}. */
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
    def info = { obj -> getLogger(this).info(obj.toString()) }
    /** {@link Closure} logging with WARN level the given {@link Object} {@link String} representation. */
    def warn = { obj ->  getLogger(this).warn(obj.toString()) }
    /** {@link Closure} logging with ERROR level the given {@link Object} {@link String} representation. */
    def error = { obj ->  getLogger(this).error(obj.toString()) }

    private static def getLogger(def source){
        if(source != null && source.class in LOGGER_MAP){
            return LOGGER_MAP.get(source.class)
        }
        else{
            return DEFAULT_LOGGER
        }
    }

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
    def getServerStatus()  {
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
    def wait(int timeout)  {
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
    boolean executeOverPassQuery(def query, def outputOSMFile) {
        if(!query){
            error "The query should not be null or empty."
            return false
        }
        if(!outputOSMFile){
            error "The output file should not be null or empty."
            return false
        }
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