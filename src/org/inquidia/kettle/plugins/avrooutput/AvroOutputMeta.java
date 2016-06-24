/*! ******************************************************************************
*
* Avro Output Plugin
*
* Author: Inquidia Consulting
*
* Copyright(c) 2014-2016 Inquidia Consulting (www.inquidia.com)
*
*******************************************************************************
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

package org.inquidia.kettle.plugins.avrooutput;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/*
 * Created on 4-apr-2003
 * @author Inquidia Consulting
 */
@Step( id = "AvroOutputPlugin", image = "avo.svg", name = "Step.Name", description = "Step.Description",
  categoryDescription = "Category.Description",
  i18nPackageName = "org.inquidia.kettle.plugins.avrooutput",
  documentationUrl = "https://github.com/cdeptula/AvroOutputPlugin",
  casesUrl = "https://github.com/cdeptula/AvroOutputPlugin/issues",
  isSeparateClassLoaderNeeded = true )
@InjectionSupported( localizationPrefix = "AvroOutput.Injection.", groups = { "OUTPUT_FIELDS" } )
public class AvroOutputMeta extends BaseStepMeta implements StepMetaInterface {
  public static final String CREATE_PARENT_FOLDER = "create_parent_folder";
  public static final String WRITE_SCHEMA_FILE = "write_schema_file";
  public static final String CREATE_SCHEMA_FILE = "create_schema_file";
  public static final String NAMESPACE = "namespace";
  public static final String DOC = "doc";
  public static final String RECORDNAME = "recordname";
  public static final String FILENAME = "filename";
  public static final String SCHEMAFILENAME = "schemafilename";
  public static final String COMPRESSIONTYPE = "compressiontype";
  public static final String SPLIT = "split";
  public static final String HASPARTNO = "haspartno";
  public static final String ADD_DATE = "add_date";
  public static final String ADD_TIME = "add_time";
  public static final String SPECIFY_FORMAT = "SpecifyFormat";
  public static final String DATE_TIME_FORMAT = "date_time_format";
  public static final String ADD_TO_RESULT_FILENAMES = "add_to_result_filenames";
  public static final String FILE = "file";
  public static final String FIELDS = "fields";
  public static final String FIELD = "field";
  public static final String NAME = "name";
  public static final String AVRONAME = "avroname";
  public static final String AVROTYPE = "avrotype";
  public static final String NULLABLE = "nullable";
  public static final String FILE_NAME = "file_name";
  public static final String SCHEMA_FILE_NAME = "schemaFileName";
  public static final String FILE_ADD_STEPNR = "file_add_stepnr";
  public static final String FILE_ADD_PARTNR = "file_add_partnr";
  public static final String FILE_ADD_DATE = "file_add_date";
  public static final String FILE_ADD_TIME = "file_add_time";
  public static final String FIELD_NAME = "field_name";
  public static final String OUTPUT_TYPE = "output_type";
  public static final String OUTPUT_FIELD_NAME = "output_field_name";
  private static Class<?> PKG = AvroOutputMeta.class; // for i18n purposes, needed by Translator2!!

  //Avro 1.7.6 supports bzip2 as an additional codec; however, Pentaho is still on Avro 1.6.2.
  public static final String[] compressionTypes = {"none","deflate","snappy"};

  public static final String[] OUTPUT_TYPES = { "BinaryFile", "BinaryField", "JsonField" };
  public static final int OUTPUT_TYPE_BINARY_FILE = 0;
  public static final int OUTPUT_TYPE_FIELD = 1;
  public static final int OUTPUT_TYPE_JSON_FIELD = 2;

  /** The base name of the output file */
  @Injection( name = "FILENAME" )
  private String fileName;

  /** The base name of the schema file */
  @Injection( name = "SCHEMA_FILENAME" )
  private String schemaFileName;

  /** Flag: create schema file, default to false */
  @Injection( name = "AUTO_CREATE_SCHEMA" )
  private boolean createSchemaFile = false;

  /** Flag: write schema file, default to true */
  @Injection( name = "WRITE_SCHEMA_TO_FILE" )
  private boolean writeSchemaFile = true;

  /** The namespace for the schema file */
  @Injection( name = "AVRO_NAMESPACE" )
  private String namespace;

  /** The record name for the schema file */
  @Injection( name = "AVRO_RECORD_NAME" )
  private String recordName;

  /** The documentation for the schema file */
  @Injection( name = "AVRO_DOC" )
  private String doc;

  /** Flag: create parent folder, default to true */
  @Injection( name = "CREATE_PARENT_FOLDER" )
  private boolean createParentFolder = true;

  /** Flag: add the stepnr in the filename */
  @Injection( name = "INCLUDE_STEPNR" )
  private boolean stepNrInFilename;

  /** Flag: add the partition number in the filename */
  @Injection( name = "INCLUDE_PARTNR" )
  private boolean partNrInFilename;

  /** Flag: add the date in the filename */
  @Injection( name = "INCLUDE_DATE" )
  private boolean dateInFilename;

  /** Flag: add the time in the filename */
  @Injection( name = "INCLUDE_TIME" )
  private boolean timeInFilename;

  /** The compression type */
  @Injection( name = "COMPRESSION_CODEC" )
  private String compressionType;

    /* THE FIELD SPECIFICATIONS ... */

  /** The output fields */
  @InjectionDeep
  private AvroOutputField[] outputFields;

  /** Flag: add the filenames to result filenames */
  @Injection( name = "ADD_TO_RESULT" )
  private boolean addToResultFilenames;

  @Injection( name = "SPECIFY_FORMAT" )
  private boolean specifyingFormat;

  @Injection( name = "DATE_FORMAT" )
  private String dateTimeFormat;

  @Injection( name = "OUTPUT_TYPE" )
  private String outputType;

  @Injection( name = "OUTPUT_FIELD_NAME" )
  private String outputFieldName;
  
  public AvroOutputMeta() {
    super(); // allocate BaseStepMeta
    allocate(0);
  }

  /**
   *
   * @return Returns the createSchemaFile
   */
  public boolean getCreateSchemaFile()
  {
    return createSchemaFile;
  }

  /**
   *
   * @param createSchemaFile
   *          The createSchemaFile to set.
   */
  public void setCreateSchemaFile( boolean createSchemaFile )
  {
    this.createSchemaFile = createSchemaFile;
  }

  /**
   *
   * @return Returns whether the schema should be persisted
   */
  public boolean getWriteSchemaFile() {
    return writeSchemaFile;
  }

  /**
   *
   * @param writeSchemaFile whether the schema file should be persisted
   */
  public void setWriteSchemaFile( boolean writeSchemaFile ) {
    this.writeSchemaFile = writeSchemaFile;
  }

  /**
   *
   * @return Returns the namespace.
   */
  public String getNamespace()
  {
    return namespace;
  }

  /**
   *
   * @param namespace The namespace to set.
   */
  public void setNamespace( String namespace )
  {
    this.namespace = namespace;
  }

  /**
   *
   * @return Returns the doc
   */
  public String getDoc() {
    return doc;
  }

  /**
   *
   * @param doc The doc to set.
   */
  public void setDoc( String doc ) {
    this.doc = doc;
  }

  /**
   *
   * @return Returns the record name
   */
  public String getRecordName() {
    return recordName;
  }

  /**
   *
   * @param recordName The record name to set.
   */
  public void setRecordName( String recordName ) {
    this.recordName = recordName;
  }

  /**
   * 
   * @return Returns the createParentFolder
   */
  public boolean getCreateParentFolder() {
    return createParentFolder;
  }

  /**
   * @param createParentFolder
   *          The createParentFolder to set.
   */
  public void setCreateParentFolder( boolean createParentFolder ) {
    this.createParentFolder = createParentFolder;
  }

  /**
   * @return Returns the dateInFilename.
   */
  public boolean getDateInFilename() {
    return dateInFilename;
  }

  /**
   * @param dateInFilename
   *          The dateInFilename to set.
   */
  public void setDateInFilename( boolean dateInFilename ) {
    this.dateInFilename = dateInFilename;
  }

  /**
   * @return Returns the add to result filesname.
   */
  public boolean getAddToResultFiles() {
    return addToResultFilenames;
  }

  /**
   * @param addtoresultfilenamesin
   *          The addtoresultfilenames to set.
   */
  public void setAddToResultFiles( boolean addtoresultfilenamesin ) {
    this.addToResultFilenames = addtoresultfilenamesin;
  }

  /**
   * @return Returns the fileName.
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * @param fileName
   *          The fileName to set.
   */
  public void setFileName( String fileName ) {
    this.fileName = fileName;
  }

  /**
   * @return Returns the schema fileName.
   */
  public String getSchemaFileName() {
    return schemaFileName;
  }

  /**
   * @param schemaFileName
   *          The schemaFileName to set.
   */
  public void setSchemaFileName( String schemaFileName ) {
    this.schemaFileName = schemaFileName;
  }

  /**
   * @return Returns the stepNrInFilename.
   */
  public boolean getStepNrInFilename() {
    return stepNrInFilename;
  }

  /**
   * @param stepNrInFilename
   *          The stepNrInFilename to set.
   */
  public void setStepNrInFilename( boolean stepNrInFilename ) {
    this.stepNrInFilename = stepNrInFilename;
  }

  /**
   * @return Returns the partNrInFilename.
   */
  public boolean getPartNrInFilename() {
    return partNrInFilename;
  }

  /**
   * @param partNrInFilename
   *          The partNrInFilename to set.
   */
  public void setPartNrInFilename( boolean partNrInFilename ) {
    this.partNrInFilename = partNrInFilename;
  }

  /**
   * @return Returns the timeInFilename.
   */
  public boolean getTimeInFilename() {
    return timeInFilename;
  }

  /**
   * @param timeInFilename
   *          The timeInFilename to set.
   */
  public void setTimeInFilename( boolean timeInFilename ) {
    this.timeInFilename = timeInFilename;
  }

  public boolean getSpecifyingFormat() {
    return specifyingFormat;
  }

  public void setSpecifyingFormat( boolean specifyingFormat ) {
    this.specifyingFormat = specifyingFormat;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public void setDateTimeFormat( String dateTimeFormat ) {
    this.dateTimeFormat = dateTimeFormat;
  }

  public String getCompressionType() {
    return compressionType;
  }

  public void setCompressionType( String compressionType ) {
    this.compressionType = compressionType;
  }

  public String getOutputType() {
    return outputType;
  }

  public void setOutputType( String outputType ) {
    this.outputType = outputType;
  }

  public int getOutputTypeId() {
    if( outputType != null ) {
      for ( int i = 0; i < OUTPUT_TYPES.length; i++ ) {
        if ( outputType.equals( OUTPUT_TYPES[i] ) ) {
          return i;
        }
      }
    }
    return -1;
  }

  public void setOutputTypeById( int outputTypeId ) {
    if( outputTypeId >= 0 && outputTypeId < OUTPUT_TYPES.length ) {
      this.outputType = OUTPUT_TYPES[outputTypeId];
    } else {
      this.outputType = null;
    }
  }

  public String getOutputFieldName() {
    return outputFieldName;
  }

  public void setOutputFieldName( String outputFieldName ) {
    this.outputFieldName = outputFieldName;
  }

  /**
   * @return Returns the outputFields.
   */
  public AvroOutputField[] getOutputFields() {
    return outputFields;
  }

  /**
   * @param outputFields
   *          The outputFields to set.
   */
  public void setOutputFields( AvroOutputField[] outputFields ) {
    this.outputFields = outputFields;
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore )
    throws KettleXMLException {
    readData( stepnode );
  }

  public void allocate( int nrfields ) {
    outputFields = new AvroOutputField[nrfields];
  }

  public Object clone() {
    AvroOutputMeta retval = (AvroOutputMeta) super.clone();
    int nrfields = outputFields.length;

    retval.allocate( nrfields );

    for ( int i = 0; i < nrfields; i++ ) {
      retval.outputFields[i] = (AvroOutputField) outputFields[i].clone();
    }

    return retval;
  }

  public void readData( Node stepnode ) throws KettleXMLException {
    try {
      // Default createparentfolder to true if the tag is missing
      String createParentFolderTagValue = XMLHandler.getTagValue( stepnode, CREATE_PARENT_FOLDER );
      String writeSchemaFileTagValue = XMLHandler.getTagValue( stepnode, WRITE_SCHEMA_FILE );
      String createSchemaFileTagValue = XMLHandler.getTagValue( stepnode, CREATE_SCHEMA_FILE );
      writeSchemaFile =
        writeSchemaFileTagValue == null ? false : "Y".equalsIgnoreCase( writeSchemaFileTagValue );
      createSchemaFile =
        ( createSchemaFileTagValue == null ) ? false : "Y".equalsIgnoreCase( createSchemaFileTagValue );
      namespace = XMLHandler.getTagValue( stepnode, NAMESPACE );
      doc = XMLHandler.getTagValue( stepnode, DOC );
      recordName = XMLHandler.getTagValue( stepnode, RECORDNAME );

      createParentFolder =
        ( createParentFolderTagValue == null ) ? true : "Y".equalsIgnoreCase( createParentFolderTagValue );
      
      fileName = XMLHandler.getTagValue( stepnode, FILENAME );
      schemaFileName = XMLHandler.getTagValue( stepnode, SCHEMAFILENAME );
      compressionType = XMLHandler.getTagValue( stepnode, COMPRESSIONTYPE );
    		  
      stepNrInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, SPLIT ) );
      partNrInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, HASPARTNO ) );
      dateInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, ADD_DATE ) );
      timeInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, ADD_TIME ) );
      specifyingFormat = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, SPECIFY_FORMAT ) );
      dateTimeFormat = XMLHandler.getTagValue( stepnode, DATE_TIME_FORMAT );
      outputType = XMLHandler.getTagValue( stepnode, OUTPUT_TYPE );
      if ( Const.isEmpty( outputType ) ) {
        outputType = OUTPUT_TYPES[OUTPUT_TYPE_BINARY_FILE];
      }
      outputFieldName = XMLHandler.getTagValue( stepnode, OUTPUT_FIELD_NAME );

      String AddToResultFiles = XMLHandler.getTagValue( stepnode, FILE, ADD_TO_RESULT_FILENAMES );
      if ( Const.isEmpty( AddToResultFiles ) ) {
        addToResultFilenames = true;
      } else {
        addToResultFilenames = "Y".equalsIgnoreCase( AddToResultFiles );
      }      
      
      Node fields = XMLHandler.getSubNode( stepnode, FIELDS );
      int nrfields = XMLHandler.countNodes( fields, FIELD );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, FIELD, i );

        outputFields[i] = new AvroOutputField();
        outputFields[i].setName( XMLHandler.getTagValue( fnode, NAME ) );
        outputFields[i].setAvroName( XMLHandler.getTagValue( fnode, AVRONAME ) );
        outputFields[i].setAvroType( Const.toInt( XMLHandler.getTagValue( fnode, AVROTYPE ), 0 ) );
        outputFields[i].setNullable( XMLHandler.getTagValue( fnode, NULLABLE ) == null ? true :
          "Y".equalsIgnoreCase( XMLHandler.getTagValue( fnode, NULLABLE ) ) );
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load step info from XML", e );
    }
  }

  public void setDefault() {
    createParentFolder = true; // Default createparentfolder to true
    createSchemaFile = false;
    writeSchemaFile = true;
    namespace = NAMESPACE;
    recordName = RECORDNAME;
    specifyingFormat = false;
    dateTimeFormat = null;
    fileName = "file.avro";
    schemaFileName = "schema.avsc";
    stepNrInFilename = false;
    partNrInFilename = false;
    dateInFilename = false;
    timeInFilename = false;
    addToResultFilenames = true;
    compressionType = "none";
    outputType = OUTPUT_TYPES[OUTPUT_TYPE_BINARY_FILE];
    outputFieldName = "avro_record";

    }

  public String buildFilename( VariableSpace space, int stepnr, String partnr, int splitnr, boolean ziparchive ) {
    return buildFilename( fileName, space, stepnr, partnr, splitnr, ziparchive, this );
  }

  public String buildFilename( String filename, VariableSpace space, int stepnr, String partnr,
    int splitnr, boolean ziparchive, AvroOutputMeta meta ) {
    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String realFileName = space.environmentSubstitute( filename );
    String extension = "";
    String retval = "";
    if( realFileName.contains(".") )
    {
    	retval = realFileName.substring( 0 , realFileName.lastIndexOf(".") );
    	extension = realFileName.substring( realFileName.lastIndexOf(".") +1 ); 
    } else {
    	retval = realFileName;
    }
    
    
    Date now = new Date();

    if ( meta.getSpecifyingFormat() && !Const.isEmpty( meta.getDateTimeFormat() ) ) {
      daf.applyPattern( meta.getDateTimeFormat() );
      String dt = daf.format( now );
      retval += dt;
    } else {
      if ( meta.getDateInFilename() ) {
        daf.applyPattern( "yyyMMdd" );
        String d = daf.format( now );
        retval += "_" + d;
      }
      if ( meta.getTimeInFilename() ) {
        daf.applyPattern( "HHmmss" );
        String t = daf.format( now );
        retval += "_" + t;
      }
    }
    if ( meta.getStepNrInFilename() ) {
      retval += "_" + stepnr;
    }
    if ( meta.getPartNrInFilename() ) {
      retval += "_" + partnr;
    }

    if ( extension != null && extension.length() != 0 ) {
      retval += "." + extension;
    }
    return retval;
  }

  public String getXML() {
    StringBuffer retval = new StringBuffer( 800 );

    retval.append( "    " + XMLHandler.addTagValue( CREATE_SCHEMA_FILE, createSchemaFile ) );
    retval.append( "    " + XMLHandler.addTagValue( WRITE_SCHEMA_FILE, writeSchemaFile ) );
    retval.append( "    " + XMLHandler.addTagValue( NAMESPACE, namespace ) );
    retval.append( "    " + XMLHandler.addTagValue( DOC, doc ) );
    retval.append( "    " + XMLHandler.addTagValue( RECORDNAME, recordName ) );
    retval.append( "    " + XMLHandler.addTagValue( CREATE_PARENT_FOLDER, createParentFolder ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( FILENAME, fileName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( SCHEMAFILENAME, schemaFileName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( COMPRESSIONTYPE, compressionType ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( SPLIT, stepNrInFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( HASPARTNO, partNrInFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( ADD_DATE, dateInFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( ADD_TIME, timeInFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( SPECIFY_FORMAT, specifyingFormat ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( DATE_TIME_FORMAT, dateTimeFormat ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( OUTPUT_TYPE, outputType ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( OUTPUT_FIELD_NAME, outputFieldName ) );

    retval.append( "      " ).append( XMLHandler.addTagValue( ADD_TO_RESULT_FILENAMES, addToResultFilenames ) );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < outputFields.length; i++ ) {
      AvroOutputField field = outputFields[i];

      if ( field.getName() != null && field.getName().length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( NAME, field.getName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( AVRONAME, field.getAvroName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( AVROTYPE, field.getAvroType() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( NULLABLE, field.getNullable() ) );
        retval.append( "      </field>" ).append( Const.CR );
      }
    }
    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      createParentFolder = rep.getStepAttributeBoolean( id_step, CREATE_PARENT_FOLDER );
      namespace = rep.getStepAttributeString( id_step, NAMESPACE );
      doc = rep.getStepAttributeString( id_step, DOC );
      recordName = rep.getStepAttributeString( id_step, RECORDNAME );
      createSchemaFile = rep.getStepAttributeBoolean( id_step, CREATE_SCHEMA_FILE );
      writeSchemaFile = rep.getStepAttributeBoolean( id_step, WRITE_SCHEMA_FILE );
      fileName = rep.getStepAttributeString( id_step, FILE_NAME );
      schemaFileName = rep.getStepAttributeString( id_step, SCHEMA_FILE_NAME );
      compressionType = rep.getStepAttributeString( id_step, COMPRESSIONTYPE );
      stepNrInFilename = rep.getStepAttributeBoolean( id_step, FILE_ADD_STEPNR );
      partNrInFilename = rep.getStepAttributeBoolean( id_step, FILE_ADD_PARTNR );
      dateInFilename = rep.getStepAttributeBoolean( id_step, FILE_ADD_DATE );
      timeInFilename = rep.getStepAttributeBoolean( id_step, FILE_ADD_TIME );
      specifyingFormat = rep.getStepAttributeBoolean( id_step, SPECIFY_FORMAT );
      dateTimeFormat = rep.getStepAttributeString( id_step, DATE_TIME_FORMAT );
      outputType = rep.getStepAttributeString( id_step, OUTPUT_TYPE );
      if ( Const.isEmpty( outputType ) ) {
        outputType = OUTPUT_TYPES[OUTPUT_TYPE_BINARY_FILE];
      }
      outputFieldName = rep.getStepAttributeString( id_step, OUTPUT_FIELD_NAME );

      String AddToResultFiles = rep.getStepAttributeString( id_step, ADD_TO_RESULT_FILENAMES );
      if ( Const.isEmpty( AddToResultFiles ) ) {
        addToResultFilenames = true;
      } else {
        addToResultFilenames = rep.getStepAttributeBoolean( id_step, ADD_TO_RESULT_FILENAMES );
      }

      int nrfields = rep.countNrStepAttributes( id_step, FIELD_NAME );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        outputFields[i] = new AvroOutputField();

        outputFields[i].setName( rep.getStepAttributeString( id_step, i, FIELD_NAME ) );
        outputFields[i].setAvroName( rep.getStepAttributeString( id_step, i, AVRONAME ) );
        Long avroType = rep.getStepAttributeInteger( id_step, i, AVROTYPE );
        outputFields[i].setAvroType( avroType.intValue() );
        outputFields[i].setNullable( rep.getStepAttributeBoolean( id_step, i, NULLABLE ) );
      }

    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, CREATE_SCHEMA_FILE, createSchemaFile );
      rep.saveStepAttribute( id_transformation, id_step, WRITE_SCHEMA_FILE, writeSchemaFile );
      rep.saveStepAttribute( id_transformation, id_step, NAMESPACE, namespace );
      rep.saveStepAttribute( id_transformation, id_step, DOC, doc );
      rep.saveStepAttribute( id_transformation, id_step, RECORDNAME, recordName );
      rep.saveStepAttribute( id_transformation, id_step, CREATE_PARENT_FOLDER, createParentFolder );
      rep.saveStepAttribute( id_transformation, id_step, FILE_NAME, fileName );
      rep.saveStepAttribute( id_transformation, id_step, SCHEMA_FILE_NAME, schemaFileName );
      rep.saveStepAttribute( id_transformation, id_step, COMPRESSIONTYPE, compressionType );
      rep.saveStepAttribute( id_transformation, id_step, FILE_ADD_STEPNR, stepNrInFilename );
      rep.saveStepAttribute( id_transformation, id_step, FILE_ADD_PARTNR, partNrInFilename );
      rep.saveStepAttribute( id_transformation, id_step, FILE_ADD_DATE, dateInFilename );
      rep.saveStepAttribute( id_transformation, id_step, DATE_TIME_FORMAT, dateTimeFormat );
      rep.saveStepAttribute( id_transformation, id_step, SPECIFY_FORMAT, specifyingFormat );
      rep.saveStepAttribute( id_transformation, id_step, OUTPUT_TYPE, outputType );;
      rep.saveStepAttribute( id_transformation, id_step, OUTPUT_FIELD_NAME, outputFieldName );

      rep.saveStepAttribute( id_transformation, id_step, ADD_TO_RESULT_FILENAMES, addToResultFilenames );
      rep.saveStepAttribute( id_transformation, id_step, FILE_ADD_TIME, timeInFilename );

      for ( int i = 0; i < outputFields.length; i++ ) {
        AvroOutputField field = outputFields[i];

        rep.saveStepAttribute( id_transformation, id_step, i, FIELD_NAME, field.getName() );
        rep.saveStepAttribute( id_transformation, id_step, i, AVRONAME, field.getAvroName() );
        rep.saveStepAttribute( id_transformation, id_step, i, AVROTYPE, field.getAvroType() );
        rep.saveStepAttribute( id_transformation, id_step, i, NULLABLE, field.getNullable() );
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    Repository repository, IMetaStore metaStore ) {
    CheckResult cr;

    // Check output fields
    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "TextFileOutputMeta.CheckResult.FieldsReceived", "" + prev.size() ), stepMeta );
      remarks.add( cr );

      String error_message = "";
      boolean error_found = false;

      // Starting from selected fields in ...
      for ( int i = 0; i < outputFields.length; i++ ) {
        int idx = prev.indexOfValue( outputFields[i].getName() );
        if ( idx < 0 ) {
          error_message += "\t\t" + outputFields[i].getName() + Const.CR;
          error_found = true;
        }
      }
      if ( error_found ) {
        error_message =
          BaseMessages.getString( PKG, "TextFileOutputMeta.CheckResult.FieldsNotFound", error_message );
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "TextFileOutputMeta.CheckResult.AllFieldsFound" ), stepMeta );
        remarks.add( cr );
      }
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "TextFileOutputMeta.CheckResult.ExpectedInputOk" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "TextFileOutputMeta.CheckResult.ExpectedInputError" ), stepMeta );
      remarks.add( cr );
    }

    cr =
      new CheckResult( CheckResultInterface.TYPE_RESULT_COMMENT, BaseMessages.getString(
        PKG, "TextFileOutputMeta.CheckResult.FilesNotChecked" ), stepMeta );
    remarks.add( cr );
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
    TransMeta transMeta, Trans trans ) {
    return new AvroOutput( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new AvroOutputData();
  }

  public void setFilename( String fileName ) {
    this.fileName = fileName;
  }

  public void getFields( RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
    // change the case insensitive flag too

    if ( outputType.equalsIgnoreCase( OUTPUT_TYPES[OUTPUT_TYPE_FIELD] ) ) {
      ValueMetaInterface v = new ValueMeta( space.environmentSubstitute( outputFieldName ), ValueMetaInterface.TYPE_BINARY );
      v.setOrigin( name );
      row.addValueMeta( v );
    } else if ( outputType.equalsIgnoreCase( OUTPUT_TYPES[OUTPUT_TYPE_JSON_FIELD] ) ) {
      ValueMetaInterface valueMetaInterface = new ValueMeta( space.environmentSubstitute( outputFieldName ), ValueMetaInterface.TYPE_STRING );
      valueMetaInterface.setOrigin( name );
      row.addValueMeta( valueMetaInterface );
    }
  }


}
