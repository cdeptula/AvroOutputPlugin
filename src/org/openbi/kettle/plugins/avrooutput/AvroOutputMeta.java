/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.openbi.kettle.plugins.avrooutput;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
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
 *
 */
public class AvroOutputMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = AvroOutputMeta.class; // for i18n purposes, needed by Translator2!!

  //Avro 1.7.6 supports bzip2 as an additional codec; however, Pentaho is still on Avro 1.6.2.
  public static final String[] compressionTypes = {"none","deflate","snappy"};

    /** The base name of the output file */
  private String fileName;

  /** The base name of the schema file */
  private String schemaFileName;

  /** Flag: create schema file, default to false */
  private boolean createSchemaFile = false;

  /** Flag: write schema file, default to true */
  private boolean writeSchemaFile = true;

  /** The namespace for the schema file */
  private String namespace;

  /** The record name for the schema file */
  private String recordName;

  /** The documentation for the schema file */
  private String doc;

  /** Flag: create parent folder, default to true */
  private boolean createParentFolder = true;

  /** Flag: add the stepnr in the filename */
  private boolean stepNrInFilename;

  /** Flag: add the partition number in the filename */
  private boolean partNrInFilename;

  /** Flag: add the date in the filename */
  private boolean dateInFilename;

  /** Flag: add the time in the filename */
  private boolean timeInFilename;

  /** The compression type */
  private String compressionType;

    /* THE FIELD SPECIFICATIONS ... */

  /** The output fields */
  private AvroOutputField[] outputFields;

  /** Flag: add the filenames to result filenames */
  private boolean addToResultFilenames;

    private boolean specifyingFormat;

  private String dateTimeFormat;
  
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
      String createParentFolderTagValue = XMLHandler.getTagValue( stepnode, "create_parent_folder" );
      String writeSchemaFileTagValue = XMLHandler.getTagValue( stepnode, "write_schema_file" );
      String createSchemaFileTagValue = XMLHandler.getTagValue( stepnode, "create_schema_file" );
      writeSchemaFile =
        writeSchemaFileTagValue == null ? false : "Y".equalsIgnoreCase( writeSchemaFileTagValue );
      createSchemaFile =
        ( createSchemaFileTagValue == null ) ? false : "Y".equalsIgnoreCase( createSchemaFileTagValue );
      namespace = XMLHandler.getTagValue( stepnode, "namespace" );
      doc = XMLHandler.getTagValue( stepnode, "doc" );
      recordName = XMLHandler.getTagValue( stepnode, "recordname" );

      createParentFolder =
        ( createParentFolderTagValue == null ) ? true : "Y".equalsIgnoreCase( createParentFolderTagValue );
      
      fileName = XMLHandler.getTagValue( stepnode, "filename" );
      schemaFileName = XMLHandler.getTagValue( stepnode, "schemafilename" );
      compressionType = XMLHandler.getTagValue( stepnode, "compressiontype" );
    		  
      stepNrInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "split" ) );
      partNrInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "haspartno" ) );
      dateInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "add_date" ) );
      timeInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "add_time" ) );
      specifyingFormat = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "SpecifyFormat" ) );
      dateTimeFormat = XMLHandler.getTagValue( stepnode, "date_time_format" );

      String AddToResultFiles = XMLHandler.getTagValue( stepnode, "file", "add_to_result_filenames" );
      if ( Const.isEmpty( AddToResultFiles ) ) {
        addToResultFilenames = true;
      } else {
        addToResultFilenames = "Y".equalsIgnoreCase( AddToResultFiles );
      }      
      
      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        outputFields[i] = new AvroOutputField();
        outputFields[i].setName( XMLHandler.getTagValue( fnode, "name" ) );
        outputFields[i].setAvroName( XMLHandler.getTagValue( fnode, "avroname" ) );
        outputFields[i].setAvroType( Const.toInt( XMLHandler.getTagValue( fnode, "avrotype" ), 0 ) );
        outputFields[i].setNullable( XMLHandler.getTagValue( fnode, "nullable" ) == null ? true :
          "Y".equalsIgnoreCase( XMLHandler.getTagValue( fnode, "nullable" ) ) );
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load step info from XML", e );
    }
  }

  public void setDefault() {
    createParentFolder = true; // Default createparentfolder to true
    createSchemaFile = false;
    writeSchemaFile = true;
    namespace = "namespace";
    recordName = "recordname";
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

    retval.append( "    " + XMLHandler.addTagValue( "create_schema_file", createSchemaFile ) );
    retval.append( "    " + XMLHandler.addTagValue( "write_schema_file", writeSchemaFile ) );
    retval.append( "    " + XMLHandler.addTagValue( "namespace", namespace ) );
    retval.append( "    " + XMLHandler.addTagValue( "doc", doc ) );
    retval.append( "    " + XMLHandler.addTagValue( "recordname", recordName ) );
    retval.append( "    " + XMLHandler.addTagValue( "create_parent_folder", createParentFolder ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "filename", fileName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "schemafilename", schemaFileName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "compressiontype", compressionType ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "split", stepNrInFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "haspartno", partNrInFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_date", dateInFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_time", timeInFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "SpecifyFormat", specifyingFormat ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "date_time_format", dateTimeFormat ) );

    retval.append( "      " ).append( XMLHandler.addTagValue( "add_to_result_filenames", addToResultFilenames ) );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < outputFields.length; i++ ) {
      AvroOutputField field = outputFields[i];

      if ( field.getName() != null && field.getName().length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( "name", field.getName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "avroname", field.getAvroName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "avrotype", field.getAvroType() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "nullable", field.getNullable() ) );
        retval.append( "      </field>" ).append( Const.CR );
      }
    }
    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      createParentFolder = rep.getStepAttributeBoolean( id_step, "create_parent_folder" );
      namespace = rep.getStepAttributeString( id_step, "namespace" );
      doc = rep.getStepAttributeString( id_step, "doc" );
      recordName = rep.getStepAttributeString( id_step, "recordname" );
      createSchemaFile = rep.getStepAttributeBoolean( id_step, "create_schema_file" );
      writeSchemaFile = rep.getStepAttributeBoolean( id_step, "write_schema_file" );
      fileName = rep.getStepAttributeString( id_step, "file_name" );
      schemaFileName = rep.getStepAttributeString( id_step, "schemaFileName" );
      compressionType = rep.getStepAttributeString( id_step, "compressiontype" );
      stepNrInFilename = rep.getStepAttributeBoolean( id_step, "file_add_stepnr" );
      partNrInFilename = rep.getStepAttributeBoolean( id_step, "file_add_partnr" );
      dateInFilename = rep.getStepAttributeBoolean( id_step, "file_add_date" );
      timeInFilename = rep.getStepAttributeBoolean( id_step, "file_add_time" );
      specifyingFormat = rep.getStepAttributeBoolean( id_step, "SpecifyFormat" );
      dateTimeFormat = rep.getStepAttributeString( id_step, "date_time_format" );

      String AddToResultFiles = rep.getStepAttributeString( id_step, "add_to_result_filenames" );
      if ( Const.isEmpty( AddToResultFiles ) ) {
        addToResultFilenames = true;
      } else {
        addToResultFilenames = rep.getStepAttributeBoolean( id_step, "add_to_result_filenames" );
      }

      int nrfields = rep.countNrStepAttributes( id_step, "field_name" );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        outputFields[i] = new AvroOutputField();

        outputFields[i].setName( rep.getStepAttributeString( id_step, i, "field_name" ) );
        outputFields[i].setAvroName( rep.getStepAttributeString( id_step, i, "avroname" ) );
        Long avroType = rep.getStepAttributeInteger( id_step, i, "avrotype" );
        outputFields[i].setAvroType( avroType.intValue() );
        outputFields[i].setNullable( rep.getStepAttributeBoolean( id_step, i, "nullable" ) );
      }

    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "create_schema_file", createSchemaFile );
      rep.saveStepAttribute( id_transformation, id_step, "write_schema_file", writeSchemaFile );
      rep.saveStepAttribute( id_transformation, id_step, "namespace", namespace );
      rep.saveStepAttribute( id_transformation, id_step, "doc", doc );
      rep.saveStepAttribute( id_transformation, id_step, "recordname", recordName );
      rep.saveStepAttribute( id_transformation, id_step, "create_parent_folder", createParentFolder );
      rep.saveStepAttribute( id_transformation, id_step, "file_name", fileName );
      rep.saveStepAttribute( id_transformation, id_step, "schemaFileName", schemaFileName );
      rep.saveStepAttribute( id_transformation, id_step, "compressiontype", compressionType );
      rep.saveStepAttribute( id_transformation, id_step, "file_add_stepnr", stepNrInFilename );
      rep.saveStepAttribute( id_transformation, id_step, "file_add_partnr", partNrInFilename );
      rep.saveStepAttribute( id_transformation, id_step, "file_add_date", dateInFilename );
      rep.saveStepAttribute( id_transformation, id_step, "date_time_format", dateTimeFormat );
      rep.saveStepAttribute( id_transformation, id_step, "SpecifyFormat", specifyingFormat );

      rep.saveStepAttribute( id_transformation, id_step, "add_to_result_filenames", addToResultFilenames );
      rep.saveStepAttribute( id_transformation, id_step, "file_add_time", timeInFilename );

      for ( int i = 0; i < outputFields.length; i++ ) {
        AvroOutputField field = outputFields[i];

        rep.saveStepAttribute( id_transformation, id_step, i, "field_name", field.getName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "avroname", field.getAvroName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "avrotype", field.getAvroType() );
        rep.saveStepAttribute( id_transformation, id_step, i, "nullable", field.getNullable() );
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

}
