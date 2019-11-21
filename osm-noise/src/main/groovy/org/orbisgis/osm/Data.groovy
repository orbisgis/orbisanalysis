package org.orbisgis.osm

import groovy.transform.BaseScript
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.SFSUtilities
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.MultiPolygon
import org.locationtech.jts.geom.Polygon
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanagerapi.dataset.ISpatialTable
import org.orbisgis.processmanagerapi.IProcess
import org.orbisgis.osm.OSMTools


@BaseScript OSMNoise osmNoise



/**
 * Download the OSM data and transform it to a set of GIS layers
 *
 *
 * @param datasource A connexion to a DB to load the OSM file
 * @param placeName the name of the place to extract the data
 * @param epsg code to reproject the GIS layers, default is -1
 * @return The name of the resulting GIS tables : zoneTableName, zoneEnvelopeTableName
 *  , the epsg of the processed zone and the path where the OSM file is stored
 */
IProcess GISLayers() {
    return create({
        title "Download and transform the OSM data to a set of GIS layers"
        inputs datasource: JdbcDataSource, placeName: String, epsg:-1
        outputs buildingTableName: String, roadTableName:String,railTableName:String,
                zoneTableName:String,zoneEnvelopeTableName:String
        run { datasource, placeName, epsg ->
            String formatedPlaceName = placeName.trim().split("\\s*(,|\\s)\\s*").join("_");
            IProcess downloadData = download()
            if(downloadData.execute(datasource: datasource, placeName: placeName, epsg:epsg)){
                epsg = downloadData.results.epsg
                def prefix = "OSM_DATA_${UUID.randomUUID().toString().replaceAll("-", "_")}"
                def load = OSMTools.Loader.load()
                info "Loading OSM data from the place name $placeName"
                if (load(datasource: datasource, osmTablesPrefix: prefix, osmFilePath: downloadData.results.osmFilePath)) {

                    def outputBuildingTableName =null
                    def outputRoadTableName =null
                    def outputRailTableName =null
                    IProcess buildingFormating = createBuildingLayer()
                    if (buildingFormating.execute(datasource: datasource, osmTablesPrefix: prefix,epsg:epsg,
                            outputTablePrefix :formatedPlaceName)){
                            outputBuildingTableName =buildingFormating.results.outputTableName
                    }
                    else{
                        error "Cannot create the building layer"
                    }

                    //createRoadLayer(datasource, prefix, epsg)

                    //TODO : Create landcover with G coeff

                    //Delete OSM tables
                    OSMTools.Utilities.dropOSMTables(osmTablesPrefix, datasource)

                    [buildingTableName  : outputBuildingTableName,
                     roadTableName      : outputRoadTableName,
                     railTableName      : outputRailTableName,
                     zoneTableName      : downloadData.results.zoneTableName,
                     zoneEnvelopeTableName: downloadData.results.zoneEnvelopeTableName]

                }
                else{
                    error"Cannot load the OSM data from the place name $placeName"
                }


            }
            else{
                error"Cannot create the OSM GIS layers from the place $placeName"
            }


        }
    })

}

/**
 * This process creates the building layer
 *
 * @param datasource A connexion to a DB to load the OSM file
 * @param osmTablesPrefix prefix for the OSM tables
 * @param epsg to transform the geometries into a referenced coordinate system
 * @param inputZoneEnvelopeTableName a table used to keep the geometries that intersect
 * @param outputTablePrefix prefix of the output table
 *
 * @return the name of the output building layer
 */
IProcess createBuildingLayer() {
    return create({
    title "Create the building layer"
    inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsg:int, inputZoneEnvelopeTableName:"",
            outputTablePrefix: String,jsonFilename: ""
    outputs outputTableName: String
    run { datasource, osmTablesPrefix, epsg, inputZoneEnvelopeTableName, outputTablePrefix,jsonFilename ->
        def transform = OSMTools.Transform.toPolygons()
        logger.info "Create the building layer"
        def paramsDefaultFile = this.class.getResourceAsStream("buildingParams.json")
        def parametersMap = parametersMapping(jsonFilename, paramsDefaultFile)
        def tags = parametersMap.get("tags")
        def columnsToKeep = parametersMap.get("columns")
        def mappingTypeAndUse = parametersMap.get("type")
        def typeAndLevel = parametersMap.get("level")
        def h_lev_min = parametersMap.get("h_lev_min")
        def h_lev_max = parametersMap.get("h_lev_max")
        def hThresholdLev2 = parametersMap.get("hThresholdLev2")
        if (transform(datasource: datasource, osmTablesPrefix: osmTablesPrefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)) {
            def inputTableName = transform.results.outputTableName
            logger.info "Formating building layer"
            def outputTableName = "${outputTablePrefix}_BUILDING_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            datasource.execute """ DROP TABLE if exists ${outputTableName};
                        CREATE TABLE ${outputTableName} (THE_GEOM GEOMETRY(POLYGON, $epsg), id_build serial, ID_SOURCE VARCHAR, HEIGHT_WALL FLOAT, HEIGHT_ROOF FLOAT,
                              NB_LEV INTEGER, TYPE VARCHAR, MAIN_USE VARCHAR, ZINDEX INTEGER);"""
            def queryMapper = "SELECT "
            ISpatialTable inputSpatialTable = datasource.getSpatialTable(inputTableName)
            if (inputSpatialTable.rowCount > 0) {
                inputSpatialTable.the_geom.createSpatialIndex()
                def columnNames = inputSpatialTable.columnNames
                columnNames.remove("THE_GEOM")
                queryMapper += columnsMapper(columnNames, columnsToKeep)
                if (inputZoneEnvelopeTableName) {
                    queryMapper += " , case when st_isvalid(a.the_geom) then a.the_geom else st_makevalid(st_force2D(a.the_geom)) end  as the_geom FROM $inputTableName as a,  $inputZoneEnvelopeTableName as b WHERE a.the_geom && b.the_geom and st_intersects(CASE WHEN ST_ISVALID(a.the_geom) THEN a.the_geom else st_makevalid(a.the_geom) end, b.the_geom)"
                } else {
                    queryMapper += " , case when st_isvalid(a.the_geom) then a.the_geom else st_makevalid(st_force2D(a.the_geom)) end as the_geom FROM $inputTableName as a"
                }
                datasource.withBatch(1000) { stmt ->
                    datasource.eachRow(queryMapper) { row ->
                        String height = row.'height'
                        String b_height = row.'building:height'
                        String roof_height = row.'roof:height'
                        String b_roof_height = row.'building:roof:height'
                        String b_lev = row.'building:levels'
                        String roof_lev = row.'roof:levels'
                        String b_roof_lev = row.'building:roof:levels'
                        def heightWall = getHeightWall(height, b_height, roof_height, b_roof_height)
                        def heightRoof = getHeightRoof(height, b_height)

                        def nbLevels = getNbLevels(b_lev, roof_lev, b_roof_lev)
                        def typeAndUseValues = getTypeAndUse(row, columnNames, mappingTypeAndUse)
                        def use = typeAndUseValues[1]
                        def type = typeAndUseValues[0]
                        if (type == null || type.isEmpty()) {
                            type = 'building'
                        }

                        def nbLevelFromType = typeAndLevel[type]

                        def formatedHeight = formatHeightsAndNbLevels(heightWall, heightRoof, nbLevels, h_lev_min,
                                h_lev_max, hThresholdLev2, nbLevelFromType == null ? 0 : nbLevelFromType)

                        def zIndex = getZIndex(row.'layer')

                        if (formatedHeight.nbLevels > 0 && zIndex >= 0 && type) {
                            Geometry geom = row.the_geom
                            for (int i = 0; i < geom.getNumGeometries(); i++) {
                                Geometry subGeom = geom.getGeometryN(i)
                                if (subGeom instanceof Polygon) {
                                    stmt.addBatch """insert into ${outputTableName} values(ST_GEOMFROMTEXT('${
                                        subGeom
                                    }',$epsg), null, '${row.id}',${formatedHeight.heightWall},${
                                        formatedHeight.heightRoof
                                    },${formatedHeight.nbLevels},'${type}','${use}',${zIndex})""".toString()
                                }
                            }
                        }
                    }
                }
                [outputTableName:outputTableName]
            }
            else{
                error "Cannot create the building layer"
            }
        }
    }
    })
}

/**
 * This process creates the road layer
 *
 * @param datasource A connexion to a DB to load the OSM file
 * @param osmTablesPrefix prefix for the OSM tables
 * @param epsg to transform the geometries into a referenced coordinate system
 * @param inputZoneEnvelopeTableName a table used to keep the geometries that intersect
 * @param outputTablePrefix prefix of the output table
 *
 * @return the name of the output building layer
 */
IProcess createRoadLayer() {
    return create({
        title "Create the road layer"
        inputs datasource: JdbcDataSource, osmTablesPrefix: String, epsg:int, inputZoneEnvelopeTableName:"",
                outputTablePrefix: String,jsonFilename: ""
        outputs outputTableName: String
        run { datasource, osmTablesPrefix, epsg, inputZoneEnvelopeTableName, outputTablePrefix,jsonFilename ->
            //Create the road layer
            transform = OSMTools.Transform.extractWaysAsLines()
            logger.info "Create the road layer"
            paramsDefaultFile = this.class.getResourceAsStream("roadParams.json")
            parametersMap = parametersMapping(jsonFilename,paramsDefaultFile)
            def mappingTypeAndUse = parametersMap.get("type")
            def mappingForSurface = parametersMap.get("surface")
            def mappingMaxSpeed = parametersMap.get("maxspeed")
            tags = parametersMap.get("tags")
            columnsToKeep = parametersMap.get("columns")
            if (transform(datasource: datasource, osmTablesPrefix: osmTablesPrefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)) {
                def inputTableName = transform.results.outputTableName
                logger.info('Formating road layer')
                def outputTableName = "${outputTablePrefix}_ROAD_${UUID.randomUUID().toString().replaceAll("-", "_")}"
                datasource.execute """drop table if exists $outputTableName;
                            CREATE TABLE $outputTableName (THE_GEOM GEOMETRY(GEOMETRY, $epsg), id_road serial, ID_SOURCE VARCHAR, WGAEN_TYPE VARCHAR,
                            SURFACE VARCHAR, MAXSPEED INTEGER, ZINDEX INTEGER);"""
                    def queryMapper = "SELECT "
                    ISpatialTable inputSpatialTable = datasource.getSpatialTable(inputTableName)
                    if(inputSpatialTable.rowCount>0) {
                        inputSpatialTable.the_geom.createSpatialIndex()
                        def columnNames = inputSpatialTable.columnNames
                        columnNames.remove("THE_GEOM")
                        queryMapper += columnsMapper(columnNames, columnsToKeep)
                        if(inputZoneEnvelopeTableName) {
                            queryMapper += ", CASE WHEN st_overlaps(CASE WHEN ST_ISVALID(a.the_geom) THEN a.the_geom else st_makevalid(a.the_geom) end, b.the_geom) then st_intersection(st_force2D(a.the_geom), b.the_geom) else a.the_geom end as the_geom FROM $inputTableName  as a, $inputZoneEnvelopeTableName as b where a.the_geom && b.the_geom"
                        }else{
                            queryMapper += ", a.the_geom FROM $inputTableName  as a"
                        }
                        datasource.withBatch(1000) { stmt ->
                            datasource.eachRow(queryMapper) { row ->
                                String type = row."highway"


                                /*Return a default value road type from OSM tag
                                * ref :  Tool 4.5: No heavy vehicle data available
                                * see the json config file
                                */
                                def typeAndUseValues = getTypeAndUse(row, columnNames, mappingTypeAndUse)
                                type = typeAndUseValues[0]
                                if (!type) {
                                    type = "Small main road"
                                }

                                int maxspeed = getSpeedInKmh(row."maxspeed")

                                if(maxspeed==-1){
                                    maxspeed = mappingMaxSpeed[type]
                                }

                                String surface = getTypeValue(row, columnNames, mappingForSurface)
                                def zIndex = getZIndex(row.'layer')
                                if (type) {
                                    Geometry geom = row.the_geom
                                    for (int i = 0; i < geom.getNumGeometries(); i++) {
                                        stmt.addBatch """insert into $outputTableName values(ST_GEOMFROMTEXT('${
                                            geom.getGeometryN(i)}',$epsg), null, '${row.id}','${type}', '${
                                            surface}',${maxspeed}, ${zIndex})""".toString()
                                    }
                                }
                            }
                        }
                        logger.info('Roads transformation finishes')
                        [outputTableName: outputTableName]
                    }
            }
        }
        })
}

/**
 * Create the rail layer
 * @param datasource
 * @param prefix
 * @param epsg
 */
def createRailLayer(datasource, prefix, epsg){
    transform = OSMTools.Transform.extractWaysAsLines()
    logger.info "Create the rail layer"
    paramsDefaultFile = this.class.getResourceAsStream("railParams.json")
    parametersMap = readJSONParameters(paramsDefaultFile)
    tags  = parametersMap.get("tags")
    columnsToKeep = parametersMap.get("columns")
    if(transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)){
        outputRailTableName = transform.results.outputTableName
        logger.info "Rail layer created"
    }
}


/**
 * Download the OSM data using the overpass api
 * The data is stored in an XML file
 *
 * @param datasource A connexion to a DB to load the OSM file
 * @param placeName the name of the place to extract the data
 * @param epsg code to reproject the GIS layers, default is -1
 * @return The name of the resulting GIS tables : zoneTableName, zoneEnvelopeTableName
 *  , the epsg of the processed zone and the path where the OSM file is stored
 */
IProcess download() {
    return create({
        title "Download the OSM data from a place name"
        inputs datasource: JdbcDataSource, placeName: String, epsg:-1
        outputs zoneTableName: String, zoneEnvelopeTableName: String, epsg :int, osmFilePath : String
        run { datasource, placeName, epsg ->
            def outputZoneTable = "ZONE_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def outputZoneEnvelopeTable = "ZONE_ENVELOPE_${UUID.randomUUID().toString().replaceAll("-", "_")}"

            if (datasource == null) {
                logger.error('The datasource cannot be null')
                return null
            }
            Geometry geom = OSMTools.Utilities.getAreaFromPlace(placeName);

            if (geom == null) {
                logger.error("Cannot find an area from the place name ${placeName}")
                return null
            } else {
                def GEOMETRY_TYPE = "GEOMETRY"
                if(geom instanceof Polygon){
                    GEOMETRY_TYPE ="POLYGON"
                }else if(geom instanceof MultiPolygon){
                    GEOMETRY_TYPE ="MULTIPOLYGON"
                }
                /**
                 * Extract the OSM file from the envelope of the geometry
                 */
                def geomAndEnv = OSMTools.Utilities.buildGeometryAndZone(geom, epsg, 0, datasource)
                epsg = geomAndEnv.geom.getSRID()

                datasource.execute """create table ${outputZoneTable} (the_geom GEOMETRY(${GEOMETRY_TYPE}, $epsg), ID_ZONE VARCHAR);
            INSERT INTO ${outputZoneTable} VALUES (ST_GEOMFROMTEXT('${
                    geomAndEnv.geom.toString()
                }', $epsg), '$placeName');"""

                datasource.execute """create table ${outputZoneEnvelopeTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
            INSERT INTO ${outputZoneEnvelopeTable} VALUES (ST_GEOMFROMTEXT('${
                    ST_Transform.ST_Transform(datasource.getConnection(), geomAndEnv.filterArea, epsg).toString()
                }',$epsg), '$placeName');"""

                Envelope envelope  = geomAndEnv.filterArea.getEnvelopeInternal()
                def query =  "[maxsize:1073741824];((node(${envelope.getMinY()},${envelope.getMinX()},${envelope.getMaxY()}, ${envelope.getMaxX()});" +
                        "way(${envelope.getMinY()},${envelope.getMinX()},${envelope.getMaxY()}, ${envelope.getMaxX()});" +
                        "relation(${envelope.getMinY()},${envelope.getMinX()},${envelope.getMaxY()}, ${envelope.getMaxX()}););>;);out;"

                def extract = OSMTools.Loader.extract()
                if (extract.execute(overpassQuery: query)){
                    [zoneTableName      : outputZoneTable,
                     zoneEnvelopeTableName: outputZoneEnvelopeTable,
                     epsg: epsg,
                     osmFilePath: extract.results.outputFilePath]
                } else {
                    logger.error "Cannot extract the OSM data from the place  $placeName"
                }
            }

        }
    })
}



/**
 * Return a maxspeed value expressed in kmh
 * @param maxspeedValue from OSM
 * @return
 */
private double getSpeedInKmh(String maxspeedValue){
        if (!maxspeedValue) return -1
        def matcher = speedPattern.matcher(maxspeedValue)
        if (!matcher.matches()) return -1

        def speed = Integer.parseInt(matcher.group(1))

        if (!(matcher.group(3))) return speed

        def type = matcher.group(3).toLowerCase()
        switch (type) {
            case "kmh":
                return speed
            case "mph":
                return speed * 1.609
            default:
                return -1
        }

}


/**
 * This function defines the input values for both columns type and use to follow the constraints
 * of the input model
 * @param row The row of the raw table to examine
 * @param columnNames the names of the column in the raw table
 * @param myMap A map between the target values in the model and the associated key/value tags retrieved from OSM
 * @return A list of Strings : first value is for "type" and second for "use"
 */
static String[] getTypeAndUse(def row,def columnNames, def myMap) {
    String strType = null
    String strUse = null
    myMap.each { finalVal ->
        def finalKey = finalVal.key
        finalVal.value.each { osmVals ->
            if(columnNames.contains(osmVals.key.toUpperCase())){
                def  columnValue = row.getString(osmVals.key)
                if(columnValue!=null){
                    osmVals.value.each { osmVal ->
                        if (osmVal.startsWith("!")) {
                            osmVal = osmVal.replace("! ","")
                            if ((columnValue != osmVal) && (columnValue != null)) {
                                if (strType == null) {
                                    strType = finalKey
                                } else {
                                    strUse = finalKey
                                }
                            }
                        } else {
                            if (columnValue == osmVal) {
                                if (strType == null) {
                                    strType = finalKey
                                } else {
                                    strUse = finalKey
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if (strUse==null) {
        strUse = strType
    }
    return [strType,strUse]
}

/**
 * This function defines the value of the column height_wall according to the values of height, b_height, r_height and b_r_height
 * @param row The row of the raw table to examine
 * @return The calculated value of height_wall (default value : 0)
 */
static float getHeightWall(height, b_height, r_height, b_r_height) {
    float result = 0
    if ((height != null && height.isFloat()) || (b_height != null && b_height.isFloat())) {
        if ((r_height != null && r_height.isFloat()) || (b_r_height != null && b_r_height.isFloat())) {
            if (b_height != null && b_height.isFloat()) {
                if (b_r_height != null && b_r_height.isFloat()) {
                    result = b_height.toFloat() - b_r_height.toFloat()
                } else {
                    result = b_height.toFloat() - r_height.toFloat()
                }
            } else {
                if (b_r_height != null && b_r_height.isFloat()) {
                    result = height.toFloat() - b_r_height.toFloat()
                } else {
                    result = height.toFloat() - r_height.toFloat()
                }
            }
        }
    }
    return result
}

/**
 * Rule to guarantee the height wall, height roof and number of levels values
 * @param height_wall value
 * @param height_roof value
 * @param nb_lev value
 * @param h_lev_min value
 * @param h_lev_max value
 * @param hThresholdLev2 value
 * @param nbLevFromType value
 * @param hThresholdLev2 value
 * @return a map with the new values
 */
static Map formatHeightsAndNbLevels(def heightWall, def heightRoof, def nbLevels, def h_lev_min,
                                    def h_lev_max,def hThresholdLev2, def nbLevFromType){
    //Initialisation of heights and number of levels
    // Update height_wall
    if(heightWall==0){
        if(heightRoof==0){
            if(nbLevels==0){
                heightWall= h_lev_min
            }
            else {
                heightWall= h_lev_min*nbLevels
            }
        }
        else {
            heightWall= heightRoof
        }
    }
    // Update height_roof
    if(heightRoof==0){
        if(heightWall==0){
            if(nbLevels==0){
                heightRoof= h_lev_min
            }
            else {
                heightRoof= h_lev_min*nbLevels
            }
        }
        else{
            heightRoof= heightWall
        }
    }
    // Update nb_lev
    // If the nb_lev parameter (in the abstract table) is equal to 1 or 2
    // (and height_wall > 10m) then apply the rule. Else, the nb_lev is equal to 1
    if(nbLevFromType==1 || nbLevFromType==2 && heightWall> hThresholdLev2){
        if(nbLevels==0){
            if(heightWall==0){
                if(heightRoof==0){
                    nbLevels= 1
                }
                else{
                    nbLevels= heightRoof/h_lev_min
                }
            }
            else {
                nbLevels= heightWall/h_lev_min
            }
        }
    }
    else{
        nbLevels = 1
    }

    // Control of heights and number of levels
    // Check if height_roof is lower than height_wall. If yes, then correct height_roof
    if(heightWall>heightRoof){
        heightRoof = heightWall
    }
    def tmpHmin=  nbLevels*h_lev_min
    // Check if there is a high difference beetween the "real" and "theorical (based on the level number) roof heights
    if(tmpHmin>heightRoof){
        heightRoof= tmpHmin
    }
    def tmpHmax=  nbLevels*h_lev_max
    if(nbLevFromType==1 || nbLevFromType==2 && heightWall> hThresholdLev2){
        if(tmpHmax<heightWall){
            nbLevels= heightWall/h_lev_max
        }
    }
    return [heightWall:heightWall, heightRoof:heightRoof, nbLevels:nbLevels]

}


/**
 * This function defines the value of the column height_roof according to the values of height and b_height
 * @param row The row of the raw table to examine
 * @return The calculated value of height_roof (default value : 0)
 */
static float getHeightRoof(height,b_height ) {
    float result = 0
    if ((height != null && height.isFloat()) || (b_height != null && b_height.isFloat())) {
        if (height != null && height.isFloat()) {
            result = height.toFloat()
        } else {
            result = b_height.toFloat()
        }
    }
    return result
}

/**
 * This function defines the value of the column nb_lev according to the values of b_lev, r_lev and b_r_lev
 * @param row The row of the raw table to examine
 * @return The calculated value of nb_lev (default value : 0)
 */
static int getNbLevels (b_lev ,r_lev,b_r_lev) {
    int result = 0
    if (b_lev != null && b_lev.isFloat()) {
        if ((r_lev != null && r_lev.isFloat()) || (b_r_lev != null && b_r_lev.isFloat())) {
            if (r_lev != null && r_lev.isFloat()) {
                result = b_lev.toFloat() + r_lev.toFloat()
            } else {
                result = b_lev.toFloat() + b_r_lev.toFloat()
            }
        } else {
            result = b_lev.toFloat()
        }
    }
    return result
}

/**
 * This function defines the value of the column width according to the value of width from OSM
 * @param width The original width value
 * @return the calculated value of width (default value : null)
 */
static Float getWidth (String width){
    return (width != null && width.isFloat()) ? width.toFloat() : 0
}

/**
 * This function defines the value of the column zindex according to the value of zindex from OSM
 * @param width The original zindex value
 * @return The calculated value of zindex (default value : null)
 */
static int getZIndex (String zindex){
    return (zindex != null && zindex.isInteger()) ? zindex.toInteger() : 0
}


/**
 * This function defines the input value for a column according to a given mapping between the expected value and the set of key/values tag in OSM
 * @param row The row of the raw table to examine
 * @param columnNames the names of the column in the raw table
 * @param myMap A map between the target values in the model and the associated key/value tags retrieved from OSM
 * @return A list of Strings : first value is for "type" and second for "use"
 */
static String getTypeValue(def row, def columnNames, def myMap) {
    String strType = null
    myMap.each { finalVal ->
        def finalKey = finalVal.key
        finalVal.value.each { osmVals ->
            if (columnNames.contains(osmVals.key.toUpperCase())) {
                def columnValue = row.getString(osmVals.key)
                if (columnValue != null) {
                    osmVals.value.each { osmVal ->
                        if (osmVal.startsWith("!")) {
                            osmVal = osmVal.replace("! ", "")
                            if ((columnValue != osmVal) && (columnValue != null)) {
                                if (strType == null) {
                                    strType = finalKey
                                }
                            }
                        } else {
                            if (columnValue == osmVal) {
                                if (strType == null) {
                                    strType = finalKey
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    return strType
}



/**
 * Method to find the difference between two list
 * right - left
 *
 * @param inputColumns left list
 * @param columnsToMap right list
 * @return a flat list with escaped elements
 */
static String columnsMapper(def inputColumns, def columnsToMap){
    //def flatList = "\"${inputColumns.join("\",\"")}\""
    def flatList =  inputColumns.inject([]) { result, iter ->
        result+= "a.\"$iter\""
    }.join(",")

    columnsToMap.each {it ->
        if(!inputColumns*.toLowerCase().contains(it)){
            flatList+= ", null as \"${it}\""
        }
    }
    return flatList
}
