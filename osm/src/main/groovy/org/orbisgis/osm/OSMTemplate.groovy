package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.datamanagerapi.dataset.ISpatialTable

/**
 * Class to manage several overpass templates to extract OSM data
 */
class OSMTemplate {
    JdbcDataSource dataSource

    public OSMTemplate(){
        dataSource = H2GIS.open(File.createTempFile("osmhelper",".db"))
    }

    public OSMTemplate(JdbcDataSource dataSource){
        this.dataSource=dataSource
    }

    /**
     * Extract building as polygons
     * With zindex
     * @return
     * @author Erwan Bocher
     */
    public ISpatialTable BUILDING(Closure closure) {
        def extract = OSMHelper.Loader.extract()
        if (extract.execute(overpassQuery: query)) {
            def load = OSMHelper.Loader.load()
            def prefix = "OSM_FILE_${uuid()}"
            assertTrue load.execute(datasource: h2GIS, osmTablesPrefix: prefix, osmFilePath: extract.results.outputFilePath)
        }
    }

    /**
     * Extract landcover as polygons
     * With zindex
     * @param closure
     * @return
     * @author Erwan Bocher
     */
    public ISpatialTable LANDCOVER(Closure closure) {
        // keys = 'landcover', 'natural', 'landuse', 'water', 'waterway', 'leisure', 'aeroway', 'amenity'
        ELSW56
    }

    /**
     * Extract water as polygons
     * With zindex
     * @param closure
     * @return
     * @author Erwan Bocher
     */
    public ISpatialTable WATER(Closure closure) {
        // keys =  'water', 'waterway', 'amenity'
    }
}