# Tests

The test are performed usin the JUnit5 library and the test coverage uses the Jacoco library.

All the test classes should extends the `AbstractOSMTest` Groovy class. THis class contains different utilities :
 - `RANDOM_DS()` : return an empty random named H2GIS DataSource.
 - `uuid()` : return a string formatted UUID.
 - Basic `beforeEach()` and `afterEach()` methods which store/restore references to the methods doing a call to external web service like Overpass or Nominatim.
 - Methods overriding methods doing a call to external web services to return preset data. The result of those methods are defined in resources files.

All functions should be test with two test method :
 - A first one with the normal behaviour and normal data with this name pattern : `methodNameOrDescriptionInCamelCase + "Test"`
 - A second one with bad behaviour or bad data with this name pattern : `"bad" + MethodNameOrDescriptionInCamelCase + "Test"`
 All the test should have a JavaDoc header (between `/** ... **/`) listing the methods tested and additional comments.
