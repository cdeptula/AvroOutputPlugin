/*! ******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.openbi.kettle.plugins.avrooutput;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.StepInjectionMetaEntry;
import org.pentaho.di.trans.step.StepMetaInjectionInterface;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This takes care of the external metadata injection into the AvroOutputMeta class
 *
 * @author Inquidia Consulting
 */
public class AvroOutputMetaInjection implements StepMetaInjectionInterface {

  public enum Entry {

    FILENAME( ValueMetaInterface.TYPE_STRING, "The output filename." ),
    AUTO_CREATE_SCHEMA( ValueMetaInterface.TYPE_STRING, "Automatically generate the Avro schema? (Y/N)" ),
    WRITE_SCHEMA_TO_FILE( ValueMetaInterface.TYPE_STRING, "Write the Avro schema to file? (Y/N)" ),
    AVRO_NAMESPACE( ValueMetaInterface.TYPE_STRING, "The namespace for the Avro schema." ),
    AVRO_RECORD_NAME( ValueMetaInterface.TYPE_STRING, "The record name for the Avro schema." ),
    AVRO_DOC( ValueMetaInterface.TYPE_STRING, "The documentation for the Avro schema." ),
    SCHEMA_FILENAME( ValueMetaInterface.TYPE_STRING, "The filename for the Avro schema." ),
    CREATE_PARENT_FOLDER( ValueMetaInterface.TYPE_STRING, "Create the parent folder? (Y/N)" ),
    COMPRESSION_CODEC( ValueMetaInterface.TYPE_STRING, "The compression codec to use. (none, deflate, snappy)" ),
    INCLUDE_STEPNR( ValueMetaInterface.TYPE_STRING, "Include the step nr in filename? (Y/N)" ),
    INCLUDE_PARTNR( ValueMetaInterface.TYPE_STRING, "Include partition nr in filename? (Y/N)" ),
    INCLUDE_DATE( ValueMetaInterface.TYPE_STRING, "Include date in filename? (Y/N)" ),
    INCLUDE_TIME( ValueMetaInterface.TYPE_STRING, "Include time in filename? (Y/N)" ),
    SPECIFY_FORMAT( ValueMetaInterface.TYPE_STRING, "Specify date format to include in filename? (Y/N)" ),
    DATE_FORMAT( ValueMetaInterface.TYPE_STRING, "Date format for filename" ),
    ADD_TO_RESULT( ValueMetaInterface.TYPE_STRING, "Add output filename to result? (Y/N)" ),

    OUTPUT_FIELDS( ValueMetaInterface.TYPE_NONE, "The output fileds" ),
    OUTPUT_FIELD( ValueMetaInterface.TYPE_NONE, "One output field" ),
    STREAM_NAME( ValueMetaInterface.TYPE_STRING, "Field to output" ),
    AVRO_PATH( ValueMetaInterface.TYPE_STRING, "Avro path to output to." ),
    AVRO_TYPE( ValueMetaInterface.TYPE_STRING, "The avro type to use when outputting. (Boolean, Double, Float, Int, Long, String)" ),
    NULLABLE( ValueMetaInterface.TYPE_STRING, "Is the field nullable? (Y/N)" );

    private int valueType;
    private String description;

    private Entry( int valueType, String description ) {
      this.valueType = valueType;
      this.description = description;
    }

    /**
     * @return the valueType
     */
    public int getValueType() {
      return valueType;
    }

    /**
     * @return the description
     */
    public String getDescription() {
      return description;
    }

    public static Entry findEntry( String key ) {
      return Entry.valueOf( key );
    }
  }

  private AvroOutputMeta meta;

  public AvroOutputMetaInjection( AvroOutputMeta meta ) {
    this.meta = meta;
  }

  @Override
  public List<StepInjectionMetaEntry> getStepInjectionMetadataEntries() throws KettleException {
    List<StepInjectionMetaEntry> all = new ArrayList<StepInjectionMetaEntry>();

    Entry[] topEntries =
      new Entry[] {
        Entry.FILENAME, Entry.AUTO_CREATE_SCHEMA, Entry.WRITE_SCHEMA_TO_FILE, Entry.AVRO_NAMESPACE,
        Entry.AVRO_RECORD_NAME, Entry.AVRO_DOC, Entry.SCHEMA_FILENAME, Entry.CREATE_PARENT_FOLDER,
        Entry.COMPRESSION_CODEC, Entry.INCLUDE_STEPNR, Entry.INCLUDE_PARTNR, Entry.INCLUDE_DATE,
        Entry.INCLUDE_TIME, Entry.SPECIFY_FORMAT, Entry.DATE_FORMAT, Entry.ADD_TO_RESULT, };
    for ( Entry topEntry : topEntries ) {
      all.add( new StepInjectionMetaEntry( topEntry.name(), topEntry.getValueType(), topEntry.getDescription() ) );
    }

    // The fields
    //
    StepInjectionMetaEntry fieldsEntry =
      new StepInjectionMetaEntry(
        Entry.OUTPUT_FIELDS.name(), ValueMetaInterface.TYPE_NONE, Entry.OUTPUT_FIELDS.description );
    all.add( fieldsEntry );
    StepInjectionMetaEntry fieldEntry =
      new StepInjectionMetaEntry(
        Entry.OUTPUT_FIELD.name(), ValueMetaInterface.TYPE_NONE, Entry.OUTPUT_FIELD.description );
    fieldsEntry.getDetails().add( fieldEntry );

    Entry[] fieldsEntries = new Entry[] { Entry.STREAM_NAME, Entry.AVRO_PATH, Entry.AVRO_TYPE, Entry.NULLABLE, };
    for ( Entry entry : fieldsEntries ) {
      StepInjectionMetaEntry metaEntry =
        new StepInjectionMetaEntry( entry.name(), entry.getValueType(), entry.getDescription() );
      fieldEntry.getDetails().add( metaEntry );
    }

    return all;
  }

  @Override
  public void injectStepMetadataEntries( List<StepInjectionMetaEntry> all ) throws KettleException {

    List<String> outputFields = new ArrayList<String>();
    List<String> avroPaths = new ArrayList<String>();
    List<String> avroTypes = new ArrayList<String>();
    List<Boolean> nullables = new ArrayList<Boolean>();

    // Parse the fields, inject into the meta class..
    //
    for ( StepInjectionMetaEntry lookFields : all ) {
      Entry fieldsEntry = Entry.findEntry( lookFields.getKey() );
      if ( fieldsEntry == null ) {
        continue;
      }

      String lookValue = (String) lookFields.getValue();
      switch ( fieldsEntry ) {
        case OUTPUT_FIELDS:
          for ( StepInjectionMetaEntry lookField : lookFields.getDetails() ) {
            Entry fieldEntry = Entry.findEntry( lookField.getKey() );
            if ( fieldEntry == Entry.OUTPUT_FIELD ) {

              String outputField = null;
              String avroPath = null;
              String avroType = null;
              boolean nullable = false;

              List<StepInjectionMetaEntry> entries = lookField.getDetails();
              for ( StepInjectionMetaEntry entry : entries ) {
                Entry metaEntry = Entry.findEntry( entry.getKey() );
                if ( metaEntry != null ) {
                  String value = (String) entry.getValue();
                  switch ( metaEntry ) {
                    case STREAM_NAME:
                      outputField = value;
                      break;
                    case AVRO_PATH:
                      avroPath = value;
                      break;
                    case AVRO_TYPE:
                      avroType = value;
                      break;
                    case NULLABLE:
                      nullable = "Y".equalsIgnoreCase( value );
                    default:
                      break;
                  }
                }
              }

              outputFields.add( outputField );
              avroPaths.add( avroPath );
              avroTypes.add( avroType );
              nullables.add( nullable );
            }
          }
          break;

        case FILENAME:
          meta.setFileName( lookValue );
          break;
        case AUTO_CREATE_SCHEMA:
          meta.setCreateSchemaFile( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case WRITE_SCHEMA_TO_FILE:
          meta.setWriteSchemaFile( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case AVRO_NAMESPACE:
          meta.setNamespace( lookValue );
          break;
        case AVRO_RECORD_NAME:
          meta.setRecordName( lookValue );
          break;
        case AVRO_DOC:
          meta.setDoc( lookValue );
          break;
        case SCHEMA_FILENAME:
          meta.setSchemaFileName( lookValue );
          break;

        case CREATE_PARENT_FOLDER:
          meta.setCreateParentFolder( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case COMPRESSION_CODEC:
          meta.setCompressionType( lookValue );
          break;
        case INCLUDE_STEPNR:
          meta.setStepNrInFilename( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case INCLUDE_PARTNR:
          meta.setPartNrInFilename( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case INCLUDE_DATE:
          meta.setDateInFilename( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case INCLUDE_TIME:
          meta.setTimeInFilename( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case SPECIFY_FORMAT:
          meta.setSpecifyingFormat( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case DATE_FORMAT:
          meta.setDateTimeFormat( lookValue );
          break;
        case ADD_TO_RESULT:
          meta.setAddToResultFiles( "Y".equalsIgnoreCase( lookValue ) );
          break;
        default:
          break;
      }
    }

    // Pass the grid to the step metadata
    //
    if ( outputFields.size() > 0 ) {
      AvroOutputField[] aof = new AvroOutputField[outputFields.size()];
      Iterator<String> iOutputFields = outputFields.iterator();
      Iterator<String> iAvroPaths = avroPaths.iterator();
      Iterator<String> iAvroTypes = avroTypes.iterator();
      Iterator<Boolean> iNullables = nullables.iterator();

      int i = 0;
      while ( iOutputFields.hasNext() ) {
        AvroOutputField field = new AvroOutputField();
        field.setName( iOutputFields.next() );
        field.setAvroName( iAvroPaths.next() );
        field.setAvroType( iAvroTypes.next() );
        field.setNullable( iNullables.next() );
        aof[i] = field;
        i++;
      }
      meta.setOutputFields( aof );
    }
  }


  public AvroOutputMeta getMeta() {
    return meta;
  }
}