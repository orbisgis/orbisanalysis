/*
 * Bundle OSM Noise is part of the OrbisGIS platform
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
 * OSM Noise is distributed under LGPL 3 license.
 *
 * Copyright (C) 2019 CNRS (Lab-STICC UMR CNRS 6285)
 *
 *
 * OSM Noise is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * OSM Noise is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * OSM Noise. If not, see <http://www.gnu.org/licenses/>.
 *
 * For more information, please consult: <http://www.orbisgis.org/>
 * or contact directly:
 * info_at_ orbisgis.org
 */
package org.orbisgis.orbisanalysis.osmnoise

import groovy.transform.BaseScript
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessFactory

@BaseScript OSMNoise pf

/**
 * Generate traffic data according the type of roads and references available in
 * a traffic properties table
 * If any traffic properties table is set, the process uses a default one.
 * The parameters of the default table refer to the European Commission Working Group Assessment of Exposure to Noise.
 * See table Tool 4.5: No heavy vehicle data available
 *
 * For each  3 time periods, day (06:00-18:00), evening (ev) (18:00-22:00) and night (22:00-06:00),
 * estimated traffic variables are  : *
 * lv_hour = Number of Light Vehicles per hour (mean)
 * hv_hour = Number of Heavy Vehicles per hour (mean)
 * Number of hours when the lv_hour and hv_hour are evaluated
 * lv_speed = Mean speed of Light Vehicles
 * hv_speed = Mean speed of Heavy Vehicles
 * percent_lv = percentage of Light Vehicles per road types
 * percent_hv = percentage of Heavy Vehicles per road types
 *
 *
 * @param datasource A connexion to a DB to load the OSM file
 * @param roadTableName The road table that contains the WG_AEN types and maxspeed values
 * @param outputTablePrefix A prefix used to set the name of the output table
 * @param trafficFile A traffic properties table stored in a SQL file
 *
 * @return The name of the road table
 */
def WGAEN_ROAD() {
    create {
        title "Compute default traffic data"
        id "WGAEN_ROAD"
        inputs datasource: JdbcDataSource, roadTableName: String, outputTablePrefix: "WGAEN_ROAD", trafficFile: ""
        outputs outputTableName: String
        run { JdbcDataSource datasource, roadTableName, outputTablePrefix, trafficFile ->
            info "Create the default traffic data"
            def outputTableName = postfix "${outputTablePrefix}_ROAD_TRAFFIC"
            def paramsDefaultFile
            if (trafficFile) {
                if (new File(trafficFile).isFile()) {
                    paramsDefaultFile = new FileInputStream(file)
                } else {
                    warn "No file named ${file} found. Taking default instead"
                    paramsDefaultFile = this.class.getResourceAsStream("roadDefaultWGAEN.sql")
                }
            } else {
                paramsDefaultFile = this.class.getResourceAsStream("roadDefaultWGAEN.sql")
            }
            def trafficTable = postfix "TRAFIC_PROPERTIES"

            datasource.executeScript(paramsDefaultFile, [TRAFIC_PROPERTIES: trafficTable])

            datasource.execute "CREATE INDEX ON ${trafficTable}(WGAEN_TYPE)"

            //Update the max speed
            datasource.execute """UPDATE ${roadTableName} P SET maxspeed=
                (SELECT  A.maxspeed FROM ${trafficTable} A WHERE A.WGAEN_TYPE=P.WGAEN_TYPE)
                 WHERE maxspeed IS NULL;"""

            datasource.execute """CREATE TABLE ${outputTableName} as 
                SELECT  a.id_road,a.the_geom, a.WGAEN_TYPE, 
                 CASE WHEN a.oneway= 'yes' THEN (t.day_nb_vh*t.day_percent_lv /t.day_nb_hours)/2 ELSE (t.day_nb_vh*t.day_percent_lv /t.day_nb_hours) END as day_lv_hour,
                 CASE WHEN a.oneway= 'yes' THEN (t.day_nb_vh*t.day_percent_hv /t.day_nb_hours)/2 ELSE (t.day_nb_vh*t.day_percent_hv /t.day_nb_hours) END as day_hv_hour,
                 a.maxspeed as day_lv_speed,
                 CASE WHEN a.maxspeed >= 110 THEN 90 ELSE a.maxspeed END AS day_hv_speed,
                 CASE WHEN a.oneway= 'yes' THEN (t.night_nb_vh*t.night_percent_lv /t.night_nb_hours)/2 ELSE (t.night_nb_vh*t.night_percent_lv /t.night_nb_hours) END as night_lv_hour,
                 CASE WHEN a.oneway= 'yes' THEN (t.night_nb_vh*t.night_percent_hv /t.night_nb_hours)/2 ELSE (t.night_nb_vh*t.night_percent_hv /t.night_nb_hours) END as night_hv_hour,
                 a.maxspeed as night_lv_speed,
                 CASE WHEN a.maxspeed >= 110 THEN 90 ELSE a.maxspeed END AS night_hv_speed,
                 CASE WHEN a.oneway= 'yes' THEN (t.ev_nb_vh*t.ev_percent_lv /t.ev_nb_hours)/2 ELSE (t.ev_nb_vh*t.ev_percent_lv /t.ev_nb_hours) END as ev_lv_hour,
                 CASE WHEN a.oneway= 'yes' THEN (t.ev_nb_vh*t.ev_percent_hv /t.ev_nb_hours)/2 ELSE (t.ev_nb_vh*t.ev_percent_hv /t.ev_nb_hours) END as ev_hv_hour,
                 a.maxspeed as ev_lv_speed,
                 CASE WHEN a.maxspeed >= 110 THEN 90 ELSE a.maxspeed END AS ev_hv_speed
                 from  ${roadTableName} a, ${trafficTable}  t WHERE a.WGAEN_TYPE=t.WGAEN_TYPE"""

            datasource.execute """COMMENT ON COLUMN ${outputTableName}."WGAEN_TYPE" IS 'Default value road type';
            COMMENT ON COLUMN ${outputTableName}."DAY_LV_HOUR" IS 'Number of light vehicles per hour for day';
            COMMENT ON COLUMN ${outputTableName}."DAY_HV_HOUR" IS 'Number of heavy vehicles per hour for day';
            COMMENT ON COLUMN ${outputTableName}."DAY_LV_SPEED" IS 'Light vehicles speed for day';
            COMMENT ON COLUMN ${outputTableName}."DAY_HV_SPEED" IS 'Heavy vehicles speed for day';
            COMMENT ON COLUMN ${outputTableName}."NIGHT_LV_HOUR" IS 'Number of light vehicles per hour for night';
            COMMENT ON COLUMN ${outputTableName}."NIGHT_HV_HOUR" IS 'Number of heavy vehicles per hour for night';
            COMMENT ON COLUMN ${outputTableName}."NIGHT_LV_SPEED" IS 'Light vehicles speed for night';
            COMMENT ON COLUMN ${outputTableName}."NIGHT_HV_SPEED" IS 'Heavy vehicles speed for night';
            COMMENT ON COLUMN ${outputTableName}."EV_LV_HOUR" IS 'Number of light vehicles per hour for evening';
            COMMENT ON COLUMN ${outputTableName}."EV_HV_HOUR" IS 'Number of heavy vehicles per hour for evening';
            COMMENT ON COLUMN ${outputTableName}."EV_LV_SPEED" IS 'Light vehicles speed for evening';
            COMMENT ON COLUMN ${outputTableName}."EV_HV_SPEED" IS 'Number of heavy vehicles per hour for evening';"""

            [outputTableName: outputTableName]
        }
    }
}
