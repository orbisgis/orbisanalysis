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

import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.GeometryFactory
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertEquals

/**
 * Abstract for OSM tests. It contains some utilities methods and static variable in order to simplify test write.
 *
 * @author Erwan Bocher (CNRS)
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */
abstract class AbstractOSMTest {
    /** Main path for the databases. */
    private static final def PATH = "./target/"
    /** Main database option to make it openable from external tools. */
    private static final def DB_OPTION = ";AUTO_SERVER=TRUE"

    /** Generation of string {@link UUID}.*/
    protected static final def uuid(){ "_"+UUID.randomUUID().toString().replaceAll("-", "_")}
    /** Used to store the OSM request to ensure the good query is generated. */
    protected static def query
    /** Generation of a random named database. */
    protected static final def RANDOM_DS = { H2GIS.open(PATH + uuid() + DB_OPTION)}
    /** Regex for the string UUID. */
    protected static def uuidRegex = "[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}"
    /** Return a random file path. **/
    protected static def RANDOM_PATH = {"./target/file"+uuid()}


    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def executeOverPassQuery
    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def getAreaFromPlace
    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def extract
    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def load
    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def executeNominatimQuery

    /**
     * Preparation for test execution. Signature should not be changed ({@link org.junit.jupiter.api.BeforeEach}
     * require non stattic void method.
     */
    void beforeEach(){
        //Store the modified object
        executeOverPassQuery = OSMTools.Loader.&executeOverPassQuery
        extract = OSMTools.Loader.&extract
        load = OSMTools.Loader.&load
        getAreaFromPlace = OSMTools.Utilities.&getAreaFromPlace
        executeNominatimQuery = OSMTools.Utilities.&executeNominatimQuery
    }

    /**
     * Preparation for test execution. Signature should not be changed ({@link org.junit.jupiter.api.AfterEach}
     * require non stattic void method.
     */
    void afterEach(){
        //Restore the modified object
        OSMTools.Loader.metaClass.static.executeOverPassQuery = executeOverPassQuery
        OSMTools.Loader.metaClass.static.extract = extract
        OSMTools.Loader.metaClass.static.load = load
        OSMTools.Utilities.metaClass.static.getAreaFromPlace = getAreaFromPlace
        OSMTools.Utilities.metaClass.static.executeNominatimQuery = executeNominatimQuery
    }

    /**
     * Override the 'executeNominatimQuery' methods to avoid the call to the server
     */
    protected static void sampleExecuteNominatimPolygonQueryOverride(){
        OSMTools.Utilities.metaClass.static.executeNominatimQuery = {query, outputOSMFile ->
            AbstractOSMTest.query = query
            outputOSMFile << LoaderTest.getResourceAsStream("nominatimSamplePolygon.geojson").text
            return true
        }
    }

    /**
     * Override the 'executeNominatimQuery' methods to avoid the call to the server
     */
    protected static void sampleExecuteNominatimMultipolygonQueryOverride(){
        OSMTools.Utilities.metaClass.static.executeNominatimQuery = {query, outputOSMFile ->
            AbstractOSMTest.query = query
            outputOSMFile << LoaderTest.getResourceAsStream("nominatimSampleMultipolygon.geojson").text
            return true
        }
    }

    /**
     * Override the 'executeNominatimQuery' methods to avoid the call to the server
     */
    protected static void badExecuteNominatimQueryOverride(){
        OSMTools.Utilities.metaClass.static.executeNominatimQuery = {query, outputOSMFile ->
            return false
        }
    }

    /**
     * Override the 'executeOverPassQuery' methods to avoid the call to the server
     */
    protected static void sampleOverpassQueryOverride(){
        OSMTools.Loader.metaClass.static.executeOverPassQuery = {query, outputOSMFile ->
            AbstractOSMTest.query = query
            outputOSMFile << LoaderTest.getResourceAsStream("sample.osm").text
            return true
        }
    }

    /**
     * Override the 'executeOverPassQuery' methods to avoid the call to the server
     */
    protected static void badOverpassQueryOverride(){
        OSMTools.Loader.metaClass.static.executeOverPassQuery = {query, outputOSMFile ->
            AbstractOSMTest.query = query
            return false
        }
    }

    /**
     * Override the 'getAreaFromPlace' methods to avoid the call to the server
     */
    protected static void sampleGetAreaFromPlace(){
        OSMTools.Utilities.metaClass.static.getAreaFromPlace = {placeName ->
            def coordinates = [new Coordinate(0, 0), new Coordinate(4, 8), new Coordinate(7, 5),
                               new Coordinate(0, 0)] as Coordinate[]
            def geom = new GeometryFactory().createPolygon(coordinates)
            geom.SRID = 4326
            return geom
        }
    }

    /**
     * Override the 'getAreaFromPlace' methods to avoid the call to the server
     */
    protected static void badGetAreaFromPlace(){
        OSMTools.Utilities.metaClass.static.getAreaFromPlace = {placeName -> }
    }

    /**
     * Override the 'extract' process to make it fail
     */
    protected static void badExtract(){
        OSMTools.Loader.metaClass.static.extract = {
            return OSMTools.Loader.create({
                title "Extract the OSM data using the overpass api and save the result in an XML file"
                inputs overpassQuery: String
                outputs outputFilePath: String
                run { overpassQuery -> }
            })
        }
    }

    /**
     * Override the 'load' process to make it fail
     */
    protected static void badLoad(){
        OSMTools.Loader.metaClass.static.load = {
            return OSMTools.Loader.create({
                title "Load an OSM file to the current database"
                inputs datasource: JdbcDataSource, osmTablesPrefix: String, osmFilePath: String
                outputs datasource: JdbcDataSource
                run { JdbcDataSource datasource, osmTablesPrefix, osmFilePath -> }
            })
        }
    }

    /**
     * Implementation of the {@link org.junit.jupiter.api.Assertions#assertEquals(String, String)} method to take into
     * account GString
     *
     * @param expected Expected {@link GString}
     * @param actual Actual {@link GString}
     */
    protected static void assertGStringEquals(GString expected, GString actual) {
        assertEquals(expected.toString(), actual.toString());
    }


    /**
     * Implementation of the {@link org.junit.jupiter.api.Assertions#assertEquals(String, String)} method to take into
     * account GString
     *
     * @param expected Expected {@link String}
     * @param actual Actual {@link GString}
     */
    protected static void assertGStringEquals(String expected, GString actual) {
        assertEquals(expected, actual.toString());
    }

    /**
     * Implementation of the {@link org.junit.jupiter.api.Assertions#assertEquals(String, String)} method to take into
     * account GString
     *
     * @param expected Expected {@link GString}
     * @param actual Actual {@link String}
     */

    protected static void assertGStringEquals(GString expected, String actual) {
        assertEquals(expected.toString(), actual);
    }

    /**
     * Implementation of the {@link org.junit.jupiter.api.Assertions#assertEquals(String, String)} method to take into
     * account GString
     *
     * @param expected Expected {@link String}
     * @param actual Actual {@link String}
     */

    protected static void assertGStringEquals(String expected, String actual) {
        assertEquals(expected.toString(), actual);
    }

    /**
     * Create a sample of OSM data
     *
     * @param ds Datasource where the data should be created.
     * @param prefix Prefix of the OSM tables.
     */
    protected void createData(def ds, def prefix){
        ds.execute "CREATE TABLE ${prefix}_node_tag (id_node int, tag_key varchar, tag_value varchar)"
        ds.execute "INSERT INTO ${prefix}_node_tag VALUES(1, 'building', 'house')"
        ds.execute "INSERT INTO ${prefix}_node_tag VALUES(1, 'material', 'concrete')"
        ds.execute "INSERT INTO ${prefix}_node_tag VALUES(2, 'material', 'concrete')"
        ds.execute "INSERT INTO ${prefix}_node_tag VALUES(3, 'water', 'lake')"
        ds.execute "INSERT INTO ${prefix}_node_tag VALUES(4, 'water', 'lake')"
        ds.execute "INSERT INTO ${prefix}_node_tag VALUES(4, 'building', 'house')"

        ds.execute "CREATE TABLE ${prefix}_way_tag (id_way int, tag_key varchar, tag_value varchar)"
        ds.execute "INSERT INTO ${prefix}_way_tag VALUES(1, 'building', 'house')"
        ds.execute "INSERT INTO ${prefix}_way_tag VALUES(1, 'material', 'concrete')"
        ds.execute "INSERT INTO ${prefix}_way_tag VALUES(1, 'water', 'lake')"

        ds.execute "CREATE TABLE ${prefix}_relation_tag (id_relation int, tag_key varchar, tag_value varchar)"
        ds.execute "INSERT INTO ${prefix}_relation_tag VALUES(1, 'building', 'house')"
        ds.execute "INSERT INTO ${prefix}_relation_tag VALUES(1, 'material', 'concrete')"
        ds.execute "INSERT INTO ${prefix}_relation_tag VALUES(1, 'water', 'lake')"

        ds.execute "CREATE TABLE ${prefix}_node(the_geom geometry, id_node int)"
        ds.execute "INSERT INTO ${prefix}_node VALUES('POINT(0 0)', 1)"
        ds.execute "INSERT INTO ${prefix}_node VALUES('POINT(10 0)', 2)"
        ds.execute "INSERT INTO ${prefix}_node VALUES('POINT(0 10)', 3)"
        ds.execute "INSERT INTO ${prefix}_node VALUES('POINT(10 10)', 4)"

        ds.execute "CREATE TABLE ${prefix}_way_node(id_way int, id_node int, node_order int)"
        ds.execute "INSERT INTO ${prefix}_way_node VALUES(1, 1, 1)"
        ds.execute "INSERT INTO ${prefix}_way_node VALUES(1, 2, 2)"
        ds.execute "INSERT INTO ${prefix}_way_node VALUES(1, 3, 3)"
        ds.execute "INSERT INTO ${prefix}_way_node VALUES(1, 4, 4)"
        ds.execute "INSERT INTO ${prefix}_way_node VALUES(1, 1, 5)"

        ds.execute "CREATE TABLE ${prefix}_way(id_way int)"
        ds.execute "INSERT INTO ${prefix}_way VALUES(1)"

        ds.execute "CREATE TABLE ${prefix}_relation(id_relation int)"
        ds.execute "INSERT INTO ${prefix}_relation VALUES(1)"

        ds.execute "CREATE TABLE ${prefix}_way_member(id_relation int, id_way int, role varchar)"
        ds.execute "INSERT INTO ${prefix}_way_member VALUES(1, 1, 'outer')"
    }
}
