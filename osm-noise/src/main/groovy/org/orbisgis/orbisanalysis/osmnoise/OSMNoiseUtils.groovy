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

import groovy.json.JsonSlurper
import java.util.regex.Pattern

/**
 * @author Erwan Bocher CNRS LAB-STICC
 */
class OSMNoiseUtils {

    /**
     * Get a set of parameters stored in a json file
     *
     * @param file
     * @param altResourceStream
     * @return
     */
    static Map parametersMapping(def file, def altResourceStream) {
        def paramStream
        def jsonSlurper = new JsonSlurper()
        if (file) {
            if (new File(file).isFile()) {
                paramStream = new FileInputStream(file)
            } else {
                warn("No file named ${file} found. Taking default instead")
                paramStream = altResourceStream
            }
        } else {
            paramStream = altResourceStream
        }
        return jsonSlurper.parse(paramStream)
    }

    static def speedPattern = Pattern.compile("([0-9]+)( ([a-zA-Z]+))?", Pattern.CASE_INSENSITIVE)
}
