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


import org.orbisgis.orbisdata.processmanager.process.GroovyProcessFactory
import org.orbisgis.orbisanalysis.osmnoise.Data as DATA
import org.orbisgis.orbisanalysis.osmnoise.Traffic as TRAFFIC
import org.slf4j.LoggerFactory

/**
 * Main script to access to all processes used to extract, transform and prepare OSM data
 * to noise modelling.
 *
 * @author Erwan Bocher CNRS LAB-STICC
 */
abstract class OSMNoise extends GroovyProcessFactory {
    def static Data = new DATA()
    def static Traffic = new TRAFFIC()

    public static def logger = LoggerFactory.getLogger(OSMNoise.class)
    static def info = { obj -> logger.info(obj.toString()) }
    static def warn = { obj -> logger.warn(obj.toString()) }
    static def error = { obj -> logger.error(obj.toString()) }
    static def debug = { obj -> logger.debug(obj.toString()) }
}