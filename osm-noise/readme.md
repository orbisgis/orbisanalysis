# OSM-Noise

OSM-Noise is library to extract and transform OSM data to a set of GIS layers requiered for noise simmulation.


# Download

OSM-Noise is avalaible as a Maven artifact from the repository http://nexus.orbisgis.org

To use the current snapshot add in the `pom`

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
import org.orbisgis.osm.OSMNoise

//Create a local H2GIS database
def h2GIS = H2GIS.open('/tmp/osmdb;AUTO_SERVER=TRUE')

//Run the process to extract OSM data from a place name and transform it to a set of GIS layers
def process = OSMNoise.Data.GISLayers()
process.execute(datasource: h2GIS, placeName: "Paimpol")
 
 //Save the GIS layers in a shapeFile        
 process.getResults().each {it ->
        if(it.value!=null && it.key!="epsg"){
                h2GIS.getTable(it.value).save("/tmp/${it.value}.shp")
            }
        }
```

