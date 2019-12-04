package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess


@BaseScript OSMNoise osmNoise



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
IProcess WGAEN_ROAD() {
    return create({
        title "Compute default traffic data"
        inputs datasource: JdbcDataSource, roadTableName: String, outputTablePrefix: "WGAEN_ROAD", trafficFile: ""
        outputs outputTableName: String
        run { JdbcDataSource datasource, roadTableName,outputTablePrefix, trafficFile ->
            logger.info "Create the default traffic data"
            def outputTableName = "${outputTablePrefix}_ROAD_TRAFFIC_$uuid"
            def paramsDefaultFile
            if (trafficFile) {
                if (new File(trafficFile).isFile()) {
                    paramsDefaultFile = new FileInputStream(file)
                } else {
                    warn("No file named ${file} found. Taking default instead")
                    paramsDefaultFile = this.class.getResourceAsStream("roadDefaultWGAEN.sql")
                }
            } else {
                paramsDefaultFile = this.class.getResourceAsStream("roadDefaultWGAEN.sql")
            }
            def trafficTable = "TRAFIC_PROPERTIES_$uuid"

            datasource.executeScript(paramsDefaultFile, [TRAFIC_PROPERTIES : trafficTable])

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

            [outputTableName:outputTableName]

        }
    })
}

