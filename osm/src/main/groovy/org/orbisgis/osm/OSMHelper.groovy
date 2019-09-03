package org.orbisgis.osm

import org.orbisgis.processmanager.GroovyProcessFactory
import org.orbisgis.processmanager.inoutput.Input
import org.orbisgis.processmanager.inoutput.Output
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
    static def uuid = { UUID.randomUUID().toString().replaceAll("-", "_") }
    static def utf8ToUrl = { utf8 -> URLEncoder.encode(utf8, UTF_8.toString()) }
    static def info = { obj -> logger.info(obj.toString()) }
    static def warn = { obj -> logger.warn(obj.toString()) }
    static def error = { obj -> logger.error(obj.toString()) }

}