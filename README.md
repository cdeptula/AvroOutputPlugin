Avro Output Plugin
===

The Avro Output Plugin for Pentaho Data Integration allows you to output Avro files using Kettle.  Avro files are commonly used in Hadoop allowing for schema evolution and truly separating the write schema from the read schema.

A big thank you to my employer [Inquidia Consulting](www.inquidia.com) for allowing me to open source this plugin.

System Requirements
---
-Pentaho Data Integration 5.0 or above

Installation
---
**Using Pentaho Marketplace**

1. In the Pentaho Marketplace find the Avro Output plugin and click Install
2. Restart Spoon

**Manual Install**

1. Place the logManager folder in the ${DI\_HOME}/plugins/steps directory
2. Restart Spoon

Usage
---
At this time the Avro Output requires the Avro schema file to be exist before writing a file using this step.

**Schema Limitations**
Arrays are not supported by this step.  It is currently not possible to output an Avro array using the Avro Output Plugin.  All other Avro types are supported including complex records.

**File Tab**
* Filename - The name of the file to output
* Schema filename - The name of the Avro schema file to use when writing.
* Create parent folder - Create the parent folder if it does not exist?
* Include stepnr in filename? - Should the step number be included in the filename?  USed for staring multiple copies of the step.
* Include partition nr in filename? - Used for partitioned transformations.
* Include date in filname? - Include the current date in the filename in yyyyMMdd format.
* Include time in filename? - Include the current time in the filename in HHmmss format.
* Specify date format? - Specify your own format for including the date time in the filename.
* Date time format - The date time format to use.

**Fields Tab**
* Name - The name of the field on the stream
* Avro Path - The dot delimited path to where the field will be stored in the Avro.  (If the schema file exists and is valid, the drop down will automatically populate with the fields from the schema.)
* Avro Type - The type used to store the field in Avro.  Since Avro supports unions of multiple types you must select a type.  (If the schema file exists and is valid the drop down will automatically limit to types that are available for the Avro Path selected.)
* Get Fields button - Gets the list of input fields, and tries to map them to an Avro field by an exact name match.
* Update Types button - Based on the Avro Path for the field, will make a best guess effort for the Avro Type that should be used.

Building from Source
---
The Avro Output Plugin is built using Ant.  Since I do not want to deal with the complexities of Ivy the following instructions must be followed before building this plugin.

1. Edit the build.properties file.
2. Set pentahoclasspath to the data-integration/lib directory on your machine.
3. Set the pentahoswtclasspath to the data-integration/libswt directory on your machine.
4. Set the pentahobigdataclasspath to the data-integration/plugins/pentaho-big-data-plugin/lib directory on your machine.
5. Run "ant dist" to build the plugin.
