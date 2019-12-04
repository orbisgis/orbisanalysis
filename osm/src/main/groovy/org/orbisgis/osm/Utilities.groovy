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
import org.orbisgis.osm.utils.OSMElement
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource

@BaseScript OSMTools osmTools

/**
 * Return the area of a city name as a geometry.
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 *
 * @param placeName The nominatim place name.
 *
 * @return a New geometry.
 */
Geometry getAreaFromPlace(def placeName) {
    def outputOSMFile = File.createTempFile("nominatim_osm", ".geojson")
    if (!executeNominatimQuery(placeName, outputOSMFile)) {
        if (!outputOSMFile.delete()) {
            warn "Unable to delete the file '$outputOSMFile'."
        }
        return
    }

    def jsonRoot = new JsonSlurper().parse(outputOSMFile)
    if (jsonRoot == null) {
        error "Cannot find any data from the place $placeName."
        return
    }

    if (jsonRoot.features.size() == 0) {
        error "Cannot find any features from the place $placeName."
        if (!outputOSMFile.delete()) {
            warn "Unable to delete the file '$outputOSMFile'."
        }
        return
    }

    GeometryFactory geometryFactory = new GeometryFactory()

    area = null
    jsonRoot.features.find() { feature ->
        if (feature.geometry != null) {
            if (feature.geometry.type.equalsIgnoreCase("polygon")) {
                area = parsePolygon(feature.geometry.coordinates, geometryFactory)
            } else if (feature.geometry.type.equalsIgnoreCase("multipolygon")) {
                def mp = feature.geometry.coordinates.collect { it ->
                    parsePolygon(it, geometryFactory)
                }.toArray(new Polygon[0])
                 area = geometryFactory.createMultiPolygon(mp)
            }
            else{
                return false
            }
            area.setSRID(4326)
            return true
        }
        return false
    }
    return area
}

/**
 * Parse geojson coordinates to create a polygon.
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 *
 * @param coordinates Coordinates to parse.
 * @param geometryFactory Geometry factory used for the geometry creation.
 *
 * @return A polygon.
 */
Polygon parsePolygon(def coordinates, GeometryFactory geometryFactory) {
    if(!coordinates in Collection || !coordinates ||
            !coordinates[0] in Collection || !coordinates[0] ||
            !coordinates[0][0] in Collection || !coordinates[0][0]){
        error "The given coordinate should be an array of an array of an array of coordinates (3D array)."
        return null
    }
    def ring
    try {
        ring = geometryFactory.createLinearRing(arrayToCoordinate(coordinates[0]))
    }
    catch(IllegalArgumentException e){
        error e.getMessage()
        return null
    }
    if(coordinates.size == 1){
        return geometryFactory.createPolygon(ring)
    }
    else {
        def holes = coordinates[1..coordinates.size - 1].collect { it ->
            geometryFactory.createLinearRing(arrayToCoordinate(it))
        }.toArray(new LinearRing[0])
        return geometryFactory.createPolygon(ring, holes)
    }
}

/**
 * Convert and array of numeric coordinates into of an array of {@link Coordinate}.
 *
 * @param coordinates Array of array of numeric value (array of numeric coordinates)
 *
 * @return Array of {@link Coordinate}.
 */
private Coordinate[] arrayToCoordinate(def coordinates){
    coordinates.collect { it ->
        if (it.size == 2) {
            def (x, y) = it
            new Coordinate(x, y)
        } else if (it.size == 3) {
            def (x, y, z) = it
            new Coordinate(x, y, z)
        }
    }.findAll {it != null}.toArray(new Coordinate[0])
}

/**
 * Method to execute an Nominatim query and save the result in a file.
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 *
 * @param query The Nominatim query.
 * @param outputNominatimFile The output file.
 *
 * @return True if the file has been downloaded, false otherwise.
 *
 */
boolean executeNominatimQuery(def query, def outputOSMFile) {
    def apiUrl = " https://nominatim.openstreetmap.org/search?q="
    def request = "&limit=5&format=geojson&polygon_geojson=1"

    URL url = new URL(apiUrl + utf8ToUrl(query) + request)
    def connection = url.openConnection()

    info url

    connection.requestMethod = "GET"

    info "Executing query... $query"
    //Save the result in a file
    if (connection.responseCode == 200) {
        info "Downloading the Nominatim data."
        outputOSMFile << connection.inputStream
        return true
    } else {
        error  "Cannot execute the Nominatim query."
        return false
    }
}

/**
 * Extract the OSM bbox signature from a Geometry.
 * e.g. (bbox:"50.7 7.1 50.7 7.12 50.71 7.11")
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 *
 * @param geometry Input geometry.
 *
 * @return OSM bbox.
 */
String toBBox(Geometry geometry) {
    if (geometry != null) {
        Envelope env = geometry.getEnvelopeInternal()
        return "(bbox:${env.getMinY()},${env.getMinX()},${env.getMaxY()}, ${env.getMaxX()})".toString()
    }
    error "Cannot convert to an overpass bounding box."
}

/**
 * Extract the OSM poly signature from a Geometry
 * e.g. (poly:"50.7 7.1 50.7 7.12 50.71 7.11")
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 *
 * @param geometry Input geometry.
 *
 * @return The OSM polygon.
 */
String toPoly(Geometry geometry) {
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
        error "The input geometry must be polygon."
    }
    error "Cannot convert to an overpass poly filter."
}

/**
 * Method to build a valid OSM query with a bbox.
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 *
 * @param envelope The envelope to filter.
 * @param keys A list of OSM keys.
 * @param osmElement A list of OSM elements to build the query (node, way, relation).
 *
 * @return A string representation of the OSM query.
 */
String buildOSMQuery(Envelope envelope, def keys, OSMElement... osmElement) {
    if (envelope != null) {
        def query = "[bbox:${envelope.getMinY()},${envelope.getMinX()},${envelope.getMaxY()},${envelope.getMaxX()}];\n(\n"
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
    error "Cannot create the overpass query from the bbox $envelope."
}

/**
 * Method to build a valid and optimized OSM query
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 *
 * @param polygon The polygon to filter.
 * @param keys A list of OSM keys.
 * @param osmElement A list of OSM elements to build the query (node, way, relation).
 *
 * @return A string representation of the OSM query.
 */
String buildOSMQuery(Polygon polygon, def keys, OSMElement... osmElement) {
    if (polygon != null || !polygon.isEmpty()) {
        Envelope envelope = polygon.getEnvelopeInternal()
        def query = "[bbox:${envelope.getMinY()},${envelope.getMinX()},${envelope.getMaxY()},${envelope.getMaxX()}];\n(\n"
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
    error "Cannot create the overpass query from the bbox $polygon."
}

/**
 * Parse a json file to a Map.
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 *
 * @param jsonFile JSON file to parse.
 *
 * @return A Map of parameters.
 */
Map readJSONParameters(def jsonFile) {
    if (jsonFile) {
        if (new File(jsonFile).isFile()) {
            def parsed = new JsonSlurper().parse(new File(jsonFile))
            if (parsed in Map) {
                return parsed
            } else {
                error "The json file doesn't contains only parameter."
            }
        } else {
            warn "No file named ${jsonFile} found."
        }
    }
}


/**
 * This method is used to build a new geometry and its envelope according an EPSG code and a distance
 * The geometry and the envelope are set up in an UTM coordinate system when the epsg code is unknown.
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 *
 * @param geom The input geometry.
 * @param distance A value to expand the envelope of the geometry.
 * @param datasource A connexion to the database.
 *
 * @return a map with the input geometry and the envelope of the input geometry. Both are projected in a new reference
 * system depending on the epsg code.
 * Note that the envelope of the geometry can be expanded according to the input distance value.
 */
static def buildGeometryAndZone(Geometry geom, int distance, def datasource) {
    return buildGeometryAndZone(geom, geom.SRID, distance, datasource)
}


/**
 * This method is used to build a new geometry and its envelope according an EPSG code and a distance
 * The geometry and the envelope are set up in an UTM coordinate system when the epsg code is unknown.
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 *
 * @param geom The input geometry.
 * @param epsg The input epsg code.
 * @param distance A value to expand the envelope of the geometry.
 * @param datasource A connexion to the database.
 *
 * @return A map with the input geometry and the envelope of the input geometry. Both are projected in a new reference
 * system depending on the epsg code.
 * Note that the envelope of the geometry can be expanded according to the input distance value.
 */
    static def buildGeometryAndZone(Geometry geom, int epsg, int distance, def datasource) {
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
            def env = geom.getEnvelopeInternal()
            env.expandBy(distance)
            def tmpEnvGeom = gf.toGeometry(env)
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
            def env = geom.getEnvelopeInternal()
            env.expandBy(distance)
            filterArea = gf.toGeometry(env)
            filterArea.setSRID(epsg)
        }
    }
    return [geom :  geom, filterArea : filterArea]
}

/**
 ** Function to drop the temp tables coming from the OSM extraction
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 *
 * @param prefix Prefix of the OSM tables.
 * @param datasource Datasource where the OSM tables are.
 **/
static boolean dropOSMTables (String prefix, JdbcDataSource datasource) {
    return datasource.execute("""DROP TABLE IF EXISTS ${prefix}_NODE, ${prefix}_NODE_MEMBER, ${prefix}_NODE_TAG,
    ${prefix}_RELATION,${prefix}_RELATION_MEMBER,${prefix}_RELATION_TAG, ${prefix}_WAY,
    ${prefix}_WAY_MEMBER,${prefix}_WAY_NODE,${prefix}_WAY_TAG""")
}