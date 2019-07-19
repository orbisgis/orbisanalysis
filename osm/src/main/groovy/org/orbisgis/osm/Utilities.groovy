package org.orbisgis.osm

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.LinearRing
import org.locationtech.jts.geom.Polygon



@BaseScript OSMHelper osmHelper

/**
 * List of OSM elements
 */
enum OSMElement{
    NODE,WAY,RELATION
}


/**
 * Return the area of a city name as a geometry
 *
 * @param query the nominatim request
 *
 * @return
 */
Geometry getAreaFromPlace(def query) {
    def outputOSMFile = File.createTempFile("nominatim_osm", ".geojson")
    def area
    if (executeNominatimQuery(query, outputOSMFile)) {
        def jsonSlurper = new JsonSlurper()
        def jsonRoot = jsonSlurper.parse(outputOSMFile)
        if (jsonRoot == null) {
            throw new Exception("No OSM file")
        }

        jsonRoot.features.size() == 1 ? info("ok") : error("Please ...")

        GeometryFactory geometryFactory = new GeometryFactory()

        jsonRoot.features.each() { feature ->
            if (feature.geometry != null) {
                if (feature.geometry.type.equalsIgnoreCase("polygon")) {
                    area = parsePolygon(feature.geometry.coordinates, geometryFactory)
                } else if (feature.geometry.type.equalsIgnoreCase("multipolygon")) {
                    def mp = feature.geometry.coordinates.collect { it ->
                        parsePolygon(it, geometryFactory)
                    }.toArray(new Polygon[0])
                    area = geometryFactory.createMultiPolygon(mp)
                }
            }
        }
    }
    return area
}


/**
 * Parser coordinates to create a polygon
 *
 * @param coordinates
 *
 * @return
 */
Polygon parsePolygon(def coordinates, GeometryFactory geometryFactory) {
    def ring = geometryFactory.createLinearRing(
            coordinates.get(0).collect { it ->
                if (it.size == 2) {
                    def (x, y) = it
                    new Coordinate(x, y)
                } else {
                    def (x, y, z) = it
                    new Coordinate(x, y, z)
                }
            }.toArray(new Coordinate[0]))
    def holes
    if (coordinates.size > 1) {
        def sub = coordinates[1..coordinates.size - 1]
        holes = sub.collect { it ->
            geometryFactory.createLinearRing(it.collect { it2 ->
                if (it2.size == 2) {
                    def (x, y) = it2
                    new Coordinate(x, y)
                } else {
                    def (x, y, z) = it2
                    new Coordinate(x, y, z)
                }
            }.toArray(new Coordinate[0]))
        }.toArray(new LinearRing[0])
    }
    if (holes != null) {
        area = geometryFactory.createPolygon(ring, holes)
    } else {
        area = geometryFactory.createPolygon(ring)
    }
}



/**
 * Method to execute an Nominatim query and save the result in a file
 *
 * @param query the Nominatim query
 * @param outputNominatimFile the output file
 *
 * @return
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
static boolean executeNominatimQuery(def query, def outputOSMFile) {
    def apiUrl = " https://nominatim.openstreetmap.org/search?q="
    def request = "&limit=1&format=geojson&polygon_geojson=1"

    URL url = new URL(apiUrl + query + request)
    def connection = url.openConnection()

    info url

    connection.requestMethod = "GET"

    info "Executing query... $query"
    //Save the result in a file
    if (connection.responseCode == 200) {
        info "Downloading the Nominatim data"
        outputOSMFile << connection.inputStream
        return true
    } else {
        error  "Cannot execute the query"
        return false
    }
}

/**
 * Extract the OSM bbox signature from a Geometry
 *
 * @param geometry input geometry
 *
 * @return osm bbox
 */
static String toBBox(Geometry geometry) {
    if (geometry != null) {
        Envelope env = geometry.getEnvelopeInternal()
        return "${env.getMinY()},${env.getMinX()},${env.getMaxY()}, ${env.getMaxX()}".toString()
    }
    error "Cannot convert to an OSM bounding box"
}



/**
 * OSM query builder
 *
 * @param keys
 *
 * @return
 */
static String defineKeysFilter(def keys, OSMElement... osmElement) {
    def query = "("
    osmElement.each { i ->
        keys.each {
            query += "${i.toString().toLowerCase()}[\"${it.toLowerCase()}\"];"
        }
    }
    query += ");(._;>;);out;"
    return query
}

/**
 * Define the area on which to make the query
 *
 * @param bbox the bbox on which the area should be computed
 * @param poly the polygon on which the area should be computed
 *
 * @return query the begin of the overpass query, corresponding to the defined area
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
static String defineFilterArea(def filterArea) {
    if (filterArea != null && !filterArea.isEmpty()) {
        def elementFilter = filterArea.keySet()[0]
        if (elementFilter.equalsIgnoreCase("bbox") || elementFilter.equalsIgnoreCase("poly")) {
            def filterValue = filterArea.get(elementFilter)
            return "[${elementFilter.toLowerCase()}:${filterValue}]"
        }
        error "Cannot build the OSM filter"
    }
}

