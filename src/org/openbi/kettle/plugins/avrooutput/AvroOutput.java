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

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.vfs.FileObject;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Converts input rows to text and then writes this text to one or more files.
 *
 * @author Matt
 * @since 4-apr-2003
 */
public class AvroOutput extends BaseStep implements StepInterface {
  private static Class<?> PKG = AvroOutputMeta.class; // for i18n purposes, needed by Translator2!!

  public AvroOutputMeta meta;

  public AvroOutputData data;

  private AvroOutputField[] avroOutputFields;
  private int outputFieldIndex;

  public AvroOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  private GenericRecord getRecord( Object[] r, String parentPath, Schema recordSchema ) throws KettleException
  {
    String parentName ="";
    if(parentPath != null) {
      parentName = new String( parentPath );
    }
    GenericRecord result = new GenericData.Record( recordSchema );
    while( outputFieldIndex < avroOutputFields.length )
    {

      AvroOutputField aof = avroOutputFields[outputFieldIndex];
      String avroName = aof.getAvroName();

      if( avroName.startsWith( "$." ) )
      {
        avroName = avroName.substring( 2 );
      }
      if( parentName == null || parentName.length() == 0 || avroName.startsWith( parentName + "." ) )
      {
        if( parentName != null && parentName.length() > 0 ) {
          avroName = avroName.substring( parentName.length() + 1 );
        }
        if( avroName.contains( "." ) )
        {
          String currentAvroPath = avroName.substring( 0, avroName.indexOf( "." ) );
          Schema childSchema = recordSchema.getField( currentAvroPath ).schema();
          String childPath = parentName + "." + currentAvroPath;
          if( parentName == null || parentName.length() == 0 )
          {
            childPath = currentAvroPath;
          }
          GenericRecord fieldRecord = getRecord( r, childPath, childSchema );
          result.put( currentAvroPath, fieldRecord );
        } else {
          Object value = getValue( r, meta.getOutputFields()[outputFieldIndex], data.fieldnrs[outputFieldIndex] );
          if( value != null )
          {
            result.put( avroName, value );
          }
          outputFieldIndex++;
        }
      } else {
        break;
      }
    }
    return result;
  }

  public Schema createAvroSchema( List<AvroOutputField> avroFields, String parentPath ) throws KettleException
  {
    String doc = meta.getDoc();
    String recordName = meta.getRecordName();
    String namespace = meta.getNamespace();

    if( parentPath.startsWith( "$." ) )
    {
      parentPath = parentPath.substring( 2 );
    }
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    if( ! parentPath.isEmpty() )
    {
      doc = "Auto generated for path "+parentPath;
      recordName=parentPath.replaceAll( "[^A-Za-z0-9\\_]", "_" );
    }
    Schema result = Schema.createRecord( recordName, doc, namespace, false );

    Iterator<AvroOutputField> it = avroFields.iterator();
    String currentPath = parentPath;
    boolean iterate = false;
    List<AvroOutputField> subFields = new ArrayList<AvroOutputField>();
    while( it.hasNext() )
    {
      AvroOutputField avroField = it.next();
      String avroName = avroField.getAvroName();
      if( avroName.startsWith( "$." ) )
      {
        avroName = avroName.substring( 2 );
      }

      String finalName = avroName;
      if( ! parentPath.isEmpty() )
      {
        finalName = avroName.substring( parentPath.length() + 1 );
      }

      if( ( ! currentPath.isEmpty() ) && ( ! avroName.startsWith( currentPath ) ) )
      {
        Schema fieldSchema = createAvroSchema( subFields, currentPath );
        String fieldName = currentPath;
        if( currentPath.contains( "." ) )
        {
          fieldName = currentPath.substring( currentPath.lastIndexOf( "." ) + 1 );
        }
        Schema.Field outField = new Schema.Field( fieldName, fieldSchema, null, null );
        fields.add( outField );
        currentPath = parentPath;
        subFields.clear();
      }

      //We are at the lowest level, no need to iterate.
      if( ! finalName.contains( "." ) )
      {
        Schema fieldSchema = Schema.create( avroField.getAvroSchemaType() );
        Schema outSchema;
        if( avroField.getNullable() )
        {
          Schema nullSchema = Schema.create( Schema.Type.NULL );

          List< Schema > unionSchema = new ArrayList<Schema>();
          unionSchema.add( nullSchema );
          unionSchema.add( fieldSchema );
          outSchema = Schema.createUnion( unionSchema );
        } else {
          outSchema = fieldSchema;
        }
        Schema.Field outField = new Schema.Field( finalName, outSchema, null, null );
        fields.add( outField );
      } else {
        String nextPath = finalName.substring( 0, finalName.indexOf( "." ) );
        if( currentPath.equals( nextPath ) )
        {
          //do nothing
        } else if( currentPath.isEmpty() )
        {
          currentPath = nextPath;
        } else {
          currentPath += "." + nextPath;
        }
        subFields.add( avroField );
      }

    }

    result.setFields( fields );
    return result;
  }

  public void writeSchemaFile() throws KettleException
  {
    List<AvroOutputField> fields = new ArrayList<AvroOutputField>();
    for( AvroOutputField avroField : avroOutputFields )
    {
      fields.add( avroField );
    }
    data.avroSchema = createAvroSchema( fields, "" );

    if( meta.getWriteSchemaFile() ) {

      try {
        String schemaFileName = buildFilename( environmentSubstitute( meta.getSchemaFileName() ), true );
        OutputStream outputStream = getOutputStream( schemaFileName, getTransMeta(), false );

        if ( log.isDetailed() ) {
          logDetailed( "Opening output stream in default encoding" );
        }
        OutputStream schemaWriter = new BufferedOutputStream( outputStream, 5000 );

        if ( log.isDetailed() ) {
          logDetailed( "Opened new file with name [" + schemaFileName + "]" );
        }

        schemaWriter.write( data.avroSchema.toString( true ).getBytes() );
        schemaWriter.close();
        if ( log.isDetailed() ) {
          logDetailed( "Closed schema file with name [" + schemaFileName + "]" );
        }

      } catch ( Exception e ) {
        throw new KettleException( "Error opening new file : " + e.toString() );
      }
    }
  }

  public synchronized boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (AvroOutputMeta) smi;
    data = (AvroOutputData) sdi;

    boolean result = true;
    Object[] r = getRow(); // This also waits for a row to be finished.

    if ( r != null && first ) {
      first = false;

      avroOutputFields = meta.getOutputFields();
      Arrays.sort( avroOutputFields );
      try
      {
        if( meta.getCreateSchemaFile() )
        {
          writeSchemaFile();
        } else {
          data.avroSchema = new Parser().parse( new File( meta.getSchemaFileName() ) );
        }
        data.datumWriter = new GenericDatumWriter<GenericRecord>( data.avroSchema );
        data.dataFileWriter = new DataFileWriter<GenericRecord>( data.datumWriter );
        data.dataFileWriter.create( data.avroSchema, data.writer );
      } catch ( IOException ex )
      {
        logError( "Could not open or create file " + meta.getSchemaFileName(), ex );
        setErrors( 1L );
        stopAll();
      }

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );

      data.fieldnrs = new int[avroOutputFields.length];
      for ( int i = 0; i < avroOutputFields.length; i++ ) {
        if( avroOutputFields[i].validate() ) {
          data.fieldnrs[ i ] = data.outputRowMeta.indexOfValue( avroOutputFields[ i ].getName() );
          if ( data.fieldnrs[ i ] < 0 ) {
            throw new KettleStepException( "Field ["
              + avroOutputFields[ i ].getName() + "] couldn't be found in the input stream!" );
          }
        }
      }
    }

    if ( r == null ) {
      // no more input to be expected...
      closeFile();
      setOutputDone();
      return false;
    }

    outputFieldIndex = 0;
    GenericRecord row = getRecord( r, null, data.avroSchema );


    try {
      data.dataFileWriter.append( row );
    } catch ( IOException i ) {
      throw new KettleException( i );
    }


    // First handle the file name in field
    // Write a header line as well if needed
    //
    putRow( data.outputRowMeta, r ); // in case we want it to go further...

    if ( checkFeedback( getLinesOutput() ) ) {
      logBasic( "linenr " + getLinesOutput() );
    }

    return result;
  }

  public Object getValue( Object[] r, AvroOutputField outputField, int inputFieldIndex) throws KettleException
  {
    Object value;

    switch( outputField.getAvroType() ) {
      case AvroOutputField.AVRO_TYPE_INT :
        value = data.outputRowMeta.getInteger( r, inputFieldIndex ).intValue();
        break;
      case AvroOutputField.AVRO_TYPE_STRING :
        value = data.outputRowMeta.getString( r, inputFieldIndex );
        break;
      case AvroOutputField.AVRO_TYPE_LONG :
        value = data.outputRowMeta.getInteger( r, inputFieldIndex );
        break;
      case AvroOutputField.AVRO_TYPE_FLOAT :
        value = data.outputRowMeta.getNumber( r, inputFieldIndex ).floatValue();
        break;
      case AvroOutputField.AVRO_TYPE_DOUBLE :
        value = data.outputRowMeta.getNumber( r, inputFieldIndex );
        break;
      case AvroOutputField.AVRO_TYPE_BOOLEAN :
        value = data.outputRowMeta.getBoolean( r, inputFieldIndex );
        break;
      default :
        throw new KettleException("Avro type "+outputField.getAvroTypeDesc()+" is not supported for field "+outputField.getAvroName()+".");
    }

    return value;
  }

  public String buildFilename( String filename, boolean ziparchive ) {
    return meta.buildFilename(
      filename, this, getCopy(), getPartitionID(), data.splitnr, ziparchive, meta );
  }

  public void openNewFile( String baseFilename ) throws KettleException {
    if ( baseFilename == null ) {
      throw new KettleFileException( BaseMessages.getString( PKG, "AvroOutput.Exception.FileNameNotSet" ) );
    }

    data.writer = null;

    String filename = buildFilename( environmentSubstitute( baseFilename ), true );

    try {
     // Check for parent folder creation only if the user asks for it
     //
     if ( meta.getCreateParentFolder() ) {
       createParentFolder( filename, meta.getSchemaFileName() );
     }

     OutputStream outputStream = getOutputStream( filename, getTransMeta(), false );

     if ( log.isDetailed() ) {
       logDetailed( "Opening output stream in default encoding" );
     }
     data.writer = new BufferedOutputStream( outputStream, 5000 );
  
     if ( log.isDetailed() ) {
       logDetailed( "Opened new file with name [" + filename + "]" );
     }
   } catch ( Exception e ) {
     throw new KettleException( "Error opening new file : " + e.toString() );
   }

    data.splitnr++;

    if ( meta.getAddToResultFiles() ) {
      // Add this to the result file names...
      ResultFile resultFile =
        new ResultFile( ResultFile.FILE_TYPE_GENERAL, getFileObject( filename, getTransMeta() ), getTransMeta()
          .getName(), getStepname() );
      resultFile.setComment( BaseMessages.getString( PKG, "AvroOutput.AddResultFile" ) );
      addResultFile( resultFile );
    }
  }

  private boolean closeFile() {
    boolean retval = false;

    try {
      if ( data.writer != null ) {
        data.writer.flush();

        if ( log.isDebug() ) {
          logDebug( "Closing output stream" );
        }
        data.dataFileWriter.close();
        data.writer.close();
        data.writer = null;
        if ( log.isDebug() ) {
          logDebug( "Closed output stream" );
        }
      }

      retval = true;
    } catch ( Exception e ) {
      logError( "Exception trying to close file: " + e.toString() );
      setErrors( 1 );
      retval = false;
    }

    return retval;
  }
  
  
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (AvroOutputMeta) smi;
    data = (AvroOutputData) sdi;

    if ( super.init( smi, sdi ) ) {
      data.splitnr = 0;
      try {
        openNewFile( meta.getFileName() );
      } catch ( Exception e ) {
        logError( "Couldn't open file " + meta.getFileName(), e );
        setErrors( 1L );
        stopAll();
      }
      
      return true;
    }

    return false;
  }


  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (AvroOutputMeta) smi;
    data = (AvroOutputData) sdi;

    if ( data.writer != null ) {
	  closeFile();
    }
	
	super.dispose( smi, sdi );
  }


  private void createParentFolder( String filename, String schemaFilename ) throws Exception {
    // Check for parent folder
    FileObject parentfolder = null;
    FileObject schemaParentFolder = null;
    try {
      // Get parent folder
      parentfolder = getFileObject( filename ).getParent();
      schemaParentFolder = getFileObject( schemaFilename ).getParent();
      if ( parentfolder.exists() ) {
        if ( isDetailed() ) {
          logDetailed( BaseMessages
            .getString( PKG, "AvroOutput.Log.ParentFolderExist", parentfolder.getName() ) );
        }
      } else {
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "AvroOutput.Log.ParentFolderNotExist", parentfolder
            .getName() ) );
        }
        if ( meta.getCreateParentFolder() ) {
          parentfolder.createFolder();
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "AvroOutput.Log.ParentFolderCreated", parentfolder
              .getName() ) );
          }
        } else {
          throw new KettleException( BaseMessages.getString(
            PKG, "AvroOutput.Log.ParentFolderNotExistCreateIt", parentfolder.getName(), filename ) );
        }
      }
      
      if( ! schemaParentFolder.equals(parentfolder) )
      {
    	  if ( schemaParentFolder.exists() ) {
	        if ( isDetailed() ) {
	          logDetailed( BaseMessages
	            .getString( PKG, "AvroOutput.Log.SchemaParentFolderExist", schemaParentFolder.getName() ) );
	        }
	      } else {
	        if ( isDetailed() ) {
	          logDetailed( BaseMessages.getString( PKG, "AvroOutput.Log.SchemaParentFolderNotExist", schemaParentFolder
	            .getName() ) );
	        }
	        if ( meta.getCreateParentFolder() ) {
	        	schemaParentFolder.createFolder();
	          if ( isDetailed() ) {
	            logDetailed( BaseMessages.getString( PKG, "AvroOutput.Log.SchemaParentFolderCreated", schemaParentFolder
	              .getName() ) );
	          }
	        } else {
	          throw new KettleException( BaseMessages.getString(
	            PKG, "AvroOutput.Log.SchemaParentFolderNotExistCreateIt", schemaParentFolder.getName(), schemaFilename ) );
	        }
	      }
      }
    } finally {
      if ( parentfolder != null ) {
        try {
          parentfolder.close();
        } catch ( Exception ex ) {
          // Ignore
        }
      }
      if( schemaParentFolder != null ) {
    	try {
    		schemaParentFolder.close();
    	} catch ( Exception ex ) {
    		//Ignore
    	}
      }
    }
  }

  protected FileObject getFileObject( String vfsFilename ) throws KettleFileException {
    return KettleVFS.getFileObject( vfsFilename );
  }

  protected FileObject getFileObject( String vfsFilename, VariableSpace space ) throws KettleFileException {
    return KettleVFS.getFileObject( vfsFilename, space );
  }

  protected OutputStream getOutputStream( String vfsFilename, VariableSpace space, boolean append )
    throws KettleFileException {
    return KettleVFS.getOutputStream( vfsFilename, space, append );
    
  }

}
