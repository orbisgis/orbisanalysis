package org.orbisgis.osm

import groovy.json.JsonSlurper
import org.orbisgis.processmanager.GroovyProcessFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.regex.Pattern


/**
 * Main script to access to all processes used to extract, transform and prepare OSM data
 * to noise modelling.
 *
 * @author Erwan Bocher CNRS LAB-STICC
 */
abstract class OSMNoise extends GroovyProcessFactory {

    /** {@link Logger} */
    public static Logger logger = LoggerFactory.getLogger(OSMNoise.class)


    public static Data = new Data()
    public static Traffic = new Traffic()

    /* *********************** */
    /*     Utility methods     */
    /* *********************** */
    /** {@link Closure} returning a {@link String} prefix/suffix build from a random {@link UUID} with '-' replaced by '_'. */
    static def getUuid() { UUID.randomUUID().toString().replaceAll("-", "_") }

    /** {@link Closure} logging with INFO level the given {@link Object} {@link String} representation. */
    static def info = { obj -> logger.info(obj.toString()) }
    /** {@link Closure} logging with WARN level the given {@link Object} {@link String} representation. */
    static def warn = { obj -> logger.warn(obj.toString()) }
    /** {@link Closure} logging with ERROR level the given {@link Object} {@link String} representation. */
    static def error = { obj -> logger.error(obj.toString()) }



    /**
     * Get a set of parameters stored in a json file
     *
     * @param file
     * @param altResourceStream
     * @return
     */
    static Map parametersMapping(def file, def altResourceStream) {
        def paramStream
        def jsonSlurper = new JsonSlurper()
        if (file) {
            if (new File(file).isFile()) {
                paramStream = new FileInputStream(file)
            } else {
                warn("No file named ${file} found. Taking default instead")
                paramStream = altResourceStream
            }
        } else {
            paramStream = altResourceStream
        }
        return jsonSlurper.parse(paramStream)
    }

    static def speedPattern = Pattern.compile("([0-9]+)( ([a-zA-Z]+))?", Pattern.CASE_INSENSITIVE)
}