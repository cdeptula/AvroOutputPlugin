Avro Output Plugin
===

The Avro Output Plugin for Pentaho Data Integration allows you to output Avro files using Kettle.  Avro files are commonly used in Hadoop allowing for schema evolution and truly separating the write schema from the read schema.

A big thank you to my employer [Inquidia Consulting](www.inquidia.com) for allowing me to open source this plugin.

System Requirements
---
-Pentaho Data Integration 6.0 or above (Plugin Version 2.2.0 and above)
-Pentaho Data Integration 5.x or above (Plugin Version 2.1.x and below)

Installation
---
**Using Pentaho Marketplace**

1. In the Pentaho Marketplace find the Avro Output plugin and click Install
2. Restart Spoon

**Manual Install**

1. Place the AvroOutputPlugin folder in the ${DI\_HOME}/plugins/steps directory
2. Restart Spoon

Usage
---
**Schema Requirements**

Arrays are not supported by this step.  It is currently not possible to output an Avro array using the Avro Output Plugin.  All other Avro types are supported including complex records.

**File Tab**
* Filename - The name of the file to output
* Automatically create schema? - Should the step automatically create the schema for the output records?
* Write schema to file? - Should the step persist the automatically created schema to a file?
* Avro namespace - The namespace for the automatically created schema.
* Avro record name - The record name for the automatically created schema.
* Avro documentation - The documentation for the automatically created schema.
* Schema filename - The name of the Avro schema file to use when writing.
* Create parent folder? - Create the parent folder if it does not exist.
* Include stepnr in filename? - Should the step number be included in the filename?  Used for starting multiple copies of the step.
* Include partition nr in filename? - Used for partitioned transformations.
* Include date in filname? - Include the current date in the filename in yyyyMMdd format.
* Include time in filename? - Include the current time in the filename in HHmmss format.
* Specify date format? - Specify your own format for including the date time in the filename.
* Date time format - The date time format to use.

**Fields Tab**
* Name - The name of the field on the stream
* Avro Path - The dot delimited path to where the field will be stored in the Avro file.  (If this is empty the stream name will be used.  If the schema file exists and is valid, the drop down will automatically populate with the fields from the schema.)
* Avro Type - The type used to store the field in Avro.  Since Avro supports unions of multiple types you must select a type.  (If the schema file exists and is valid the drop down will automatically limit to types that are available for the Avro Path selected.)
* Nullable? - Should the field be nullable in the Avro schema.  Only used if "automatically create avro schema" is checked.
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
