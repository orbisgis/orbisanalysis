package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanagerapi.dataset.ISpatialTable

@BaseScript OSMHelper osmHelper

//class OSMTemplate {
    //def dataSource

    public ISpatialTable BUILDING() {
        if (dataSource == null) {

        }
        def extract = OSMHelper.Loader.extract()
        if (extract.execute(overpassQuery: query)) {
            def load = OSMHelper.Loader.load()
            def prefix = "OSM_FILE_${uuid()}"
            assertTrue load.execute(datasource: h2GIS, osmTablesPrefix: prefix, osmFilePath: extract.results.outputFilePath)
        }

    }
//}