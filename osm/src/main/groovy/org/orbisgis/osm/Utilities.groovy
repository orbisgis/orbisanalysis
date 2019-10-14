package org.orbisgis.osm

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.SFSUtilities
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.LinearRing
import org.locationtech.jts.geom.Polygon
import org.orbisgis.datamanager.JdbcDataSource


@BaseScript OSMTools osmTools

/*
* Utilities for OSM data
* @author Erwan Bocher (CNRS LAB-STICC)
* @author Elisabeth Le Saux (UBS LAB-STICC)
 */


/**
 * List of OSM elements
 */
enum OSMElement{
    NODE,WAY,RELATION
}


/**
 * Return the area of a city name as a geometry
 *
 * @param placeName the nominatim place name
 *
 * @return a new geometry
 */
Geometry getAreaFromPlace(def placeName) {
    def outputOSMFile = File.createTempFile("nominatim_osm", ".geojson")
    if (executeNominatimQuery(placeName, outputOSMFile)) {
        def jsonSlurper = new JsonSlurper()
        def jsonRoot = jsonSlurper.parse(outputOSMFile)
        if (jsonRoot == null) {
            throw new Exception("Cannot find any data from the place $placeName")
        }
        if(jsonRoot.features.size() == 0 ){
            error("Cannot find any features from the place $placeName")
            return null
        }

        GeometryFactory geometryFactory = new GeometryFactory()

         area = null
        jsonRoot.features.find() { feature ->
            if (feature.geometry != null) {
                if (feature.geometry.type.equalsIgnoreCase("polygon")) {
                    area = parsePolygon(feature.geometry.coordinates, geometryFactory)
                    area.setSRID(4326)
                    return true
                } else if (feature.geometry.type.equalsIgnoreCase("multipolygon")) {
                    def mp = feature.geometry.coordinates.collect { it ->
                        parsePolygon(it, geometryFactory)
                    }.toArray(new Polygon[0])
                     area = geometryFactory.createMultiPolygon(mp)
                    area.setSRID(4326)
                    return true
                }
            }
            return false
        }
    }
    return area
}


/**
 * Parser geojson coordinates to create a polygon
 *
 * @param coordinates
 * @param geometryFactory
 *
 * @return a polygon
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
        return geometryFactory.createPolygon(ring, holes)
    } else {
        return geometryFactory.createPolygon(ring)
    }
}



/**
 * Method to execute an Nominatim query and save the result in a file
 *
 * @param query the Nominatim query
 * @param outputNominatimFile the output file
 *
 * @return true if the file has been downloaded
 *
 */
static boolean executeNominatimQuery(def query, def outputOSMFile) {
    def apiUrl = " https://nominatim.openstreetmap.org/search?q="
    def request = "&limit=5&format=geojson&polygon_geojson=1"

    URL url = new URL(apiUrl + utf8ToUrl(query) + request)
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
        error  "Cannot execute the Nominatim query"
        return false
    }
}

/**
 * Extract the OSM bbox signature from a Geometry
 * e.g. (bbox:"50.7 7.1 50.7 7.12 50.71 7.11")
 * @param geometry input geometry
 *
 * @return osm bbox
 */
static String toBBox(Geometry geometry) {
    if (geometry != null) {
        Envelope env = geometry.getEnvelopeInternal()
        return "(bbox:${env.getMinY()},${env.getMinX()},${env.getMaxY()}, ${env.getMaxX()})".toString()
    }
    error "Cannot convert to an overpass bounding box"
}

/**
 * Extract the OSM poly signature from a Geometry
 * e.g. (poly:"50.7 7.1 50.7 7.12 50.71 7.11")
 * @param geometry input geometry
 *
 * @return osm poly
 */
static String toPoly(Geometry geometry) {
    if (geometry != null) {
        if (geometry instanceof Polygon) {
            Coordinate[] coordinates = ((Polygon) geometry).getExteriorRing().getCoordinates()
            def poly = "(poly:\""
            for (i in 0.. coordinates.size()-3) {
                Coordinate coord = coordinates[i]
                poly += "${coord.getY()} ${coord.getX()} "
            }
            Coordinate coord = coordinates[coordinates.size()-2]
            poly += "${coord.getY()} ${coord.getX()}"
            return poly + "\")"
        }
        error "The input geometry must be polygon"
    }
    error "Cannot convert to an overpass poly filter"
}

/**
 * Method to build a valid OSM query with a bbox
 *
 * @param envelope the envelope to filter
 * @param keys a list of OSM keys
 * @param osmElement a list of OSM elements to build the query (node, way, relation)
 *
 * @return a string representation of the OSM query
 */
static String buildOSMQuery(Envelope envelope, def keys, OSMElement... osmElement) {
    if (envelope != null) {
        def query = "[bbox:${envelope.getMinY()},${envelope.getMinX()},${envelope.getMaxY()}, ${envelope.getMaxX()}];\n(\n"
        osmElement.each { i ->
            if(keys==null || keys.isEmpty()){
                query += "\t${i.toString().toLowerCase()};\n"
            }
            else{
            keys.each {
                query += "\t${i.toString().toLowerCase()}[\"${it.toLowerCase()}\"];\n"
            }
            }
        }
        query += ");\n(._;>;);\nout;"
        return query
    }
    error "Cannot create the overpass query from the bbox $envelope"
}

/**
 * Method to build a valid and optimized OSM query
 *
 * @param polygon the polygon to filter
 * @param keys a list of OSM keys
 * @param osmElement a list of OSM elements to build the query (node, way, relation)
 *
 * @return a string representation of the OSM query
 */
static String buildOSMQuery(Polygon polygon, def keys, OSMElement... osmElement) {
    if (polygon != null || !polygon.isEmpty()) {
        Envelope envelope = polygon.getEnvelopeInternal()
        def query = "[bbox:${envelope.getMinY()},${envelope.getMinX()},${envelope.getMaxY()}, ${envelope.getMaxX()}];\n(\n"
        String filterArea =  toPoly(polygon)
        def  nokeys = false;
        osmElement.each { i ->
            if(keys==null || keys.isEmpty()){
                query += "\t${i.toString().toLowerCase()}$filterArea;\n"
                nokeys=true
            }
            else {
                keys.each {
                    query += "\t${i.toString().toLowerCase()}[\"${it.toLowerCase()}\"]$filterArea;\n"
                    nokeys=false
                }
            }
        }
        if(nokeys){
            query += ");\nout;"
        }
        else{
            query += ");\n(._;>;);\nout;"
        }

        return query
    }
    error "Cannot create the overpass query from the bbox $polygon"
}

/**
 * Parse a json file to a Map
 * @param jsonFile
 * @return
 */
static Map readJSONParameters(def jsonFile) {
    def jsonSlurper = new JsonSlurper()
    if (jsonFile) {
        if (new File(jsonFile).isFile()) {
            return jsonSlurper.parse(new File(jsonFile))
        } else {
            logger.warn("No file named ${jsonFile} found.")
        }
    }
}


/**
 * This method is used to build a new geometry and its envelope according an EPSG code and a distance
 * The geometry and the envelope are set up in an UTM coordinate system when the epsg code is unknown.
 *
 * @param geom the input geometry
 * @param epsg the input epsg code
 * @param distance a value to expand the envelope of the geometry
 * @param datasource a connexion to the database
 *
 * @return a map with the input geometry and the envelope of the input geometry. Both are projected in a new reference
 * system depending on the epsg code.
 * Note that the envelope of the geometry can be expanded according to the input distance value.
 */
def buildGeometryAndZone(Geometry geom, int epsg, int distance, def datasource) {
    GeometryFactory gf = new GeometryFactory()
    def con = datasource.getConnection();
    Polygon filterArea = null
    if(epsg==-1 || epsg==0){
        def interiorPoint = geom.getCentroid()
        epsg = SFSUtilities.getSRID(con, interiorPoint.y as float, interiorPoint.x as float)
        geom = ST_Transform.ST_Transform(con, geom, epsg);
        if(distance==0){
            Geometry tmpEnvGeom = gf.toGeometry(geom.getEnvelopeInternal())
            tmpEnvGeom.setSRID(epsg)
            filterArea = ST_Transform.ST_Transform(con, tmpEnvGeom, 4326)
        }
        else {
            def tmpEnvGeom = gf.toGeometry(geom.getEnvelopeInternal().expandBy(distance))
            tmpEnvGeom.setSRID(epsg)
            filterArea = ST_Transform.ST_Transform(con, tmpEnvGeom, 4326)
        }
    }
    else {
        geom = ST_Transform.ST_Transform(con, geom, epsg);
        if(distance==0){
            filterArea = gf.toGeometry(geom.getEnvelopeInternal())
            filterArea.setSRID(epsg)
        }
        else {
            filterArea = gf.toGeometry(geom.getEnvelopeInternal().expandBy(distance))
            filterArea.setSRID(epsg)
        }
    }
    return [geom :  geom, filterArea : filterArea]
}

/**
 ** Function to drop the temp tables coming from the OSM extraction
 * @param prefix prefix of the OSM tables
 * @param datasource connection to the database where the OSM tables are
 **/
static boolean dropOSMTables (String prefix, JdbcDataSource datasource) {
    return datasource.execute("""DROP TABLE IF EXISTS ${prefix}_NODE, ${prefix}_NODE_MEMBER, ${prefix}_NODE_TAG,
    ${prefix}_RELATION,${prefix}_RELATION_MEMBER,${prefix}_RELATION_TAG, ${prefix}_WAY,
    ${prefix}_WAY_MEMBER,${prefix}_WAY_NODE,${prefix}_WAY_TAG""")
}