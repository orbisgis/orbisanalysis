package org.orbisgis.osm

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.h2gis.functions.io.geojson.ST_GeomFromGeoJSON
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory


@BaseScript OSMHelper osmHelper

/**
 *
 * @param query
 * @return
 */
Geometry getArea(def query) {
    def outputOSMFile = File.createTempFile("nominatim_osm", ".geojson")
    if(executeNominatimQuery(query, outputOSMFile)) {
        def jsonSlurper = new JsonSlurper()
        def jsonRoot =jsonSlurper.parse(outputOSMFile)

        if (jsonRoot == null) {
            throw new Exception("No OSM file")
        }

        jsonRoot.features.size() == 1 ? logger.info("ok"):logger.error("Please ...")

        GeometryFactory geometryFactory = new GeometryFactory();

        jsonRoot.features.each() { feature ->
            if (feature.geometry != null) {
                if(feature.geometry.type.toLowerCase()=="polygon"){
                    feature.geometry.coordinates();
                    def (x, y, z) = feature.geometry.coordinates
                    //geometryFactory.createPolygon(shell)
                }
            }
        }

    }

    return null
}





/**
 * Method to execute an Nominatim query and save the result in a file
 * @param query the Nominatim query
 * @param outputNominatimFile the output file
 * @return
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 */
boolean executeNominatimQuery(def query, def outputOSMFile) {
    def apiUrl = " https://nominatim.openstreetmap.org/search?q="
    query+= "+is_in+${query}&format=geojson&polygon_geojson=1"

    URL url = new URL(apiUrl + query)
    def connection = url.openConnection() as HttpURLConnection

    logger.info url.toString()

    connection.setRequestMethod("GET")

    logger.info "Executing query... $query"
    //Save the result in a file
    if (connection.responseCode == 200) {
        logger.info "Downloading the Nominatim data"
        outputOSMFile << connection.inputStream
        return true
    } else {
        logger.error  "Cannot execute the query"
        return false
    }
}

