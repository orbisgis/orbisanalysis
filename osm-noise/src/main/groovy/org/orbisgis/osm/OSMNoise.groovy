package org.orbisgis.osm

import org.h2gis.functions.spatial.crs.ST_Transform
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.MultiPolygon
import org.locationtech.jts.geom.Polygon
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanager.GroovyProcessFactory
import org.orbisgis.processmanagerapi.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static java.nio.charset.StandardCharsets.UTF_8

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





}