# OSM-Noise

OSM-Noise is library to extract and transform OSM data to a set of GIS layers requiered for noise simmulation.


# Download

OSM-Noise is avalaible as a Maven artifact from the repository http://nexus.orbisgis.org

To use the current version add in the `pom`

```xml
<dependency>
  <groupId>org.orbisgis</groupId>
  <artifactId>osm-noise</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```
# How to use

The simple way to use the OSM-Noise chain is to run it in a Groovy console, using Grab annotation (http://groovy-lang.org/groovyconsole.html).

Put the following script and run it to extract OSM data from a place name and transform it to GIS layers.

```groovy
// Declaration of the maven repository
@GrabResolver(name='orbisgis', root='http://nexus-ng.orbisgis.org/repository/orbisgis/')
@Grab(group='org.orbisgis', module='osm-noise', version='1.0.0-SNAPSHOT')

import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.orbisanalysis.osm.OSMNoise

//Create a local H2GIS database
def h2GIS = H2GIS.open('/tmp/osmdb;AUTO_SERVER=TRUE')

//Run the process to extract OSM data from a place name and transform it to a set of GIS layers
def process = OSMNoise.Data.GISLayers()
process.execute(datasource: h2GIS, placeName: "Paimpol")
 
 //Save the GIS layers in a shapeFile        
 process.getResults().each {it ->
        if(it.value!=null){
                h2GIS.getTable(it.value).save("/tmp/${it.value}.shp")
            }
        }
```
Use the next script to generate default traffic data using the WG-AEN road caterogies.

```groovy
// Declaration of the maven repository
@GrabResolver(name='orbisgis', root='http://nexus-ng.orbisgis.org/repository/orbisgis/')
@Grab(group='org.orbisgis', module='osm-noise', version='1.0.0-SNAPSHOT')

import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.orbisanalysis.osm.OSMNoise

//Create a local H2GIS database
def h2GIS = H2GIS.open('/tmp/osmdb;AUTO_SERVER=TRUE')

//Run the process to extract OSM data from a place name and transform it to a set of GIS layers
def process = OSMNoise.Data.GISLayers()
process.execute(datasource: h2GIS, placeName: "Paimpol")

def trafficProcess = OSMNoise.Traffic.WGAEN_ROAD()
trafficProcess.execute(datasource: h2GIS,roadTableName:process.results.roadTableName, outputTablePrefix:"Paimpol")

 
 //Save the layers in a shapeFile
 //Building
  h2GIS.getSpatialTable(process.results.buildingTableName,).save("/tmp/building.shp")
 //Road with traffic data
 h2GIS.getSpatialTable(trafficProcess.results.roadTableName).save("/tmp/road_traffic.shp")
 
```
