package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript OSMHelper osmHelper


    /**
     * Extract building as polygons
     * With zindex
     * @return
     * @author Erwan Bocher
     */
    public IProcess BUILDING() {
        return processFactory.create({
            title ""
            inputs bbox : String, admin_level : String, place : String
            outputs outputTableName : String
            closure { bbox, area ->
                def extract = OSMHelper.Loader.extract()
                if (extract.execute(overpassQuery: query)) {
                    def load = OSMHelper.Loader.load()
                    def prefix = "OSM_FILE_${uuid()}"
                    assertTrue load.execute(datasource: h2GIS, osmTablesPrefix: prefix, osmFilePath: extract.results.outputFilePath)
                }
            }

        })

    }

    /**
     * Extract landcover as polygons
     * With zindex
     * @param closure
     * @return
     * @author Erwan Bocher
     */
    public IProcess LANDCOVER(Closure closure) {
        // keys = 'landcover', 'natural', 'landuse', 'water', 'waterway', 'leisure', 'aeroway', 'amenity'

    }

    /**
     * Extract water as polygons
     * With zindex
     * @param closure
     * @return
     * @author Erwan Bocher
     */
    public IProcess WATER(Closure closure) {
        // keys =  'water', 'waterway', 'amenity'
    }
