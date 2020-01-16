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

import groovy.transform.BaseScript
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Polygon
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.h2gis.utilities.jts_utils.GeographyUtils

import java.util.regex.Pattern

import static org.orbisgis.orbisanalysis.osm.utils.OSMElement.NODE
import static org.orbisgis.orbisanalysis.osm.utils.OSMElement.RELATION
import static org.orbisgis.orbisanalysis.osm.utils.OSMElement.WAY

@BaseScript OSMTools osmTools

/**
 * This process extracts OSM data file and load it in a database using an area
 * The area must be a JTS envelope
 *
 * @param datasource A connexion to a DB to load the OSM file
 * @param filterArea Filtering area as envelope
 * @param distance to expand the envelope of the query box. Default is 0
 *
 * @return The name of the tables that contains the geometry representation of the extracted area (outputZoneTable) and
 * its envelope (outputZoneEnvelopeTable)
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess fromArea() {
    return create({
        title "Extract the OSM data using an area"
        inputs datasource: JdbcDataSource, filterArea: Object, distance : 0
        outputs zoneTableName: String, zoneEnvelopeTableName: String, osmTablesPrefix: String, epsg: int
        run { JdbcDataSource datasource, filterArea, distance ->
            if (!filterArea) {
                error( "Filter area not defined")
                return
            }
            def outputZoneTable = "ZONE_$uuid"
            def outputZoneEnvelopeTable = "ZONE_ENVELOPE_$uuid"
            def osmTablesPrefix = "OSM_DATA_$uuid"
            def geom
            if(filterArea instanceof Envelope ) {
                geom  = new GeometryFactory().toGeometry(filterArea)
            }
            else if( filterArea instanceof Polygon ) {
                geom = filterArea
            }
            else{
                error "The filter area must be an Envelope or a Polygon"
                return
            }

            def epsg = DEFAULT_SRID
            def env = GeographyUtils.expandEnvelopeByMeters(geom.getEnvelopeInternal(), distance)

            //Create table to store the geometry and the envelope of the extracted area
            datasource.execute "CREATE TABLE $outputZoneTable (the_geom GEOMETRY(POLYGON, $epsg));" +
                    " INSERT INTO $outputZoneTable VALUES (ST_GEOMFROMTEXT('${geom}', $epsg));"

            def geometryFactory = new GeometryFactory()
            def geomEnv = geometryFactory.toGeometry(env)

            datasource.execute "CREATE TABLE $outputZoneEnvelopeTable (the_geom GEOMETRY(POLYGON, $epsg));" +
                    "INSERT INTO $outputZoneEnvelopeTable VALUES " +
                        "(ST_GEOMFROMTEXT('$geomEnv',$epsg));"

            def query = OSMTools.Utilities.buildOSMQuery(geomEnv, [], NODE, WAY, RELATION)
            def extract = OSMTools.Loader.extract()
            if (extract(overpassQuery: query)) {
                info "Downloading OSM data from the area $filterArea"
                def load = OSMTools.Loader.load()
                if (load(datasource     : datasource,
                        osmTablesPrefix : osmTablesPrefix,
                        osmFilePath     : extract.results.outputFilePath)) {
                    info "Loading OSM data from the area $filterArea"
                    return [zoneTableName         : outputZoneTable,
                            zoneEnvelopeTableName : outputZoneEnvelopeTable,
                            osmTablesPrefix       : osmTablesPrefix,
                            epsg                  : epsg]
                } else {
                    error "Cannot load the OSM data from the area $filterArea"
                }
            } else {
                error "Cannot download OSM data from the area $filterArea"
            }
        }
    })
}


/**
 * This process extracts OSM data file and load it in a database using a place name
 *
 * @param datasource A connexion to a DB to load the OSM file
 * @param placeName The name of the place to extract
 * @param distance to expand the envelope of the query box. Default is 0
 *
 * @return The name of the tables that contains the geometry representation of the extracted area (outputZoneTable) and
 * its envelope extended or not by a distance (outputZoneEnvelopeTable)
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess fromPlace() {
    return create({
        title "Extract the OSM data using a place name"
        inputs datasource: JdbcDataSource, placeName: String, distance: 0
        outputs zoneTableName: String, zoneEnvelopeTableName: String, osmTablesPrefix: String
        run { JdbcDataSource datasource, placeName, distance ->
            def formatedPlaceName = placeName.trim().replaceAll("(\\s|,|-|\$)+", "_")
            def outputZoneTable = "ZONE_$formatedPlaceName$uuid"
            def outputZoneEnvelopeTable = "ZONE_ENVELOPE_$formatedPlaceName$uuid"
            def osmTablesPrefix = "OSM_DATA_$formatedPlaceName$uuid"
            def epsg = DEFAULT_SRID

            def geom = OSMTools.Utilities.getAreaFromPlace(placeName);
            if (!geom) {
                error("Cannot find an area from the place name $placeName")
                return
            }
            def env = GeographyUtils.expandEnvelopeByMeters(geom.getEnvelopeInternal(), distance)

            //Create table to store the geometry and the envelope of the extracted area
            datasource.execute "CREATE TABLE $outputZoneTable (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);" +
                    "INSERT INTO $outputZoneTable VALUES (ST_GEOMFROMTEXT('${geom}', $epsg), '$placeName');"

            def geometryFactory = new GeometryFactory()
            def geomEnv = geometryFactory.toGeometry(env)
            datasource.execute "CREATE TABLE $outputZoneEnvelopeTable (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);" +
                    "INSERT INTO $outputZoneEnvelopeTable VALUES (ST_GEOMFROMTEXT('$geomEnv',$epsg), '$placeName');"

            def query = OSMTools.Utilities.buildOSMQuery(geomEnv, [], NODE, WAY, RELATION)
            def extract = OSMTools.Loader.extract()
            if (extract(overpassQuery: query)) {
                info "Downloading OSM data from the place $placeName"
                def load = OSMTools.Loader.load()
                if (load(datasource: datasource,
                        osmTablesPrefix: osmTablesPrefix,
                        osmFilePath: extract.results.outputFilePath)) {
                    info "Loading OSM data from the place $placeName"
                    return [zoneTableName        : outputZoneTable,
                            zoneEnvelopeTableName: outputZoneEnvelopeTable,
                            osmTablesPrefix      : osmTablesPrefix]
                } else {
                    error "Cannot load the OSM data from the place $placeName"
                }

            } else {
                error "Cannot download OSM data from the place $placeName"
            }
        }
    })
}

/**
 * This process extracts OSM data as an XML file using the Overpass API
 *
 * @param overpassQuery The overpass api to be executed
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess extract() {
    return create({
        title "Extract the OSM data using the overpass api and save the result in an XML file"
        inputs overpassQuery: String
        outputs outputFilePath: String
        run { overpassQuery ->
            info "Extract the OSM data"
            def tmpOSMFile = File.createTempFile("extract_osm", ".osm")
            def osmFilePath = tmpOSMFile.absolutePath
            if (overpassQuery && executeOverPassQuery(overpassQuery, tmpOSMFile)) {
                info "The OSM file has been downloaded at $osmFilePath."
            } else {
                error "Cannot extract the OSM data for the query $overpassQuery"
                return
            }
            [outputFilePath: osmFilePath]
        }
    })
}

/**
 * This process is used to load an OSM file in a database.
 *
 * @param datasource A connection to a database
 * @param osmTablesPrefix A prefix to identify the 10 OSM tables
 * @param omsFilePath The path where the OSM file is
 *
 * @return datasource The connection to the database
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
IProcess load() {
    return create({
        title "Load an OSM file to the current database"
        inputs datasource: JdbcDataSource, osmTablesPrefix: String, osmFilePath: String
        outputs datasource: JdbcDataSource
        run { JdbcDataSource datasource, osmTablesPrefix, osmFilePath ->
            if(!datasource) {
                error "Please set a valid database connection."
                return
            }

            if(osmTablesPrefix == null ||
                    !Pattern.compile('^[a-zA-Z0-9_]*$').matcher(osmTablesPrefix).matches()) {
                error "Please set a valid table prefix."
                return
            }

            if(!osmFilePath){
                error "Please set a valid osm file path."
                return
            }
            def osmFile = new File(osmFilePath)
            if (!osmFile.exists()) {
                error "The input OSM file does not exist."
                return
            }

            info "Load the OSM file in the database."
            datasource.load(osmFile, osmTablesPrefix, true)
            info "The input OSM file has been loaded in the database."

            [datasource: datasource]
        }
    })
}

