package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript OSMNoise osmNoise



/**
 * Compute a generic TMJA
 *
 *
 * @param datasource A connexion to a DB to load the OSM file
 * @return The name of the road table
 */
IProcess WGAEN() {
    return create({
        title "Compute default traffic data"
        inputs datasource: JdbcDataSource, roadTableName: String, jsonFilename: ""
        outputs outputTableName: String
        run { datasource, roadTableName, jsonFilename ->
            logger.info "Create the default traffic data"
            paramsDefaultFile = this.class.getResourceAsStream("roadDefaultWGAEN.json")
            parametersMap = parametersMapping(jsonFilename,paramsDefaultFile)
            [outputTableName:null]

        }
    })
}

