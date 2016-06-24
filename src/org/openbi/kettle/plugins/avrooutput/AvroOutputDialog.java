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

package org.openbi.kettle.plugins.avrooutput;

import org.apache.avro.Schema;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboValuesSelectionListener;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Inquidia Consulting
 */
public class AvroOutputDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = AvroOutputMeta.class; // for i18n purposes, needed by Translator2!!

  private static final String[] YES_NO_COMBO = new String[] { BaseMessages.getString( PKG, "System.Combo.No" ),
    BaseMessages.getString( PKG, "System.Combo.Yes" ) };

  private static final String[] OUTPUT_TYPE_DESC = new String[] {
    BaseMessages.getString( PKG, "AvroOutputDialog.OutputType.BinaryFile" ),
    BaseMessages.getString( PKG, "AvroOutputDialog.OutputType.BinaryField" ),
    BaseMessages.getString( PKG, "AvroOutputDialog.OutputType.JsonField" ) };

  private CTabFolder wTabFolder;
  private FormData fdTabFolder;

  private CTabItem wFileTab, wFieldsTab;

  private FormData fdFileComp, fdFieldsComp;

  private Label wlOutputType;
  private CCombo wOutputType;
  private FormData fdlOutputType, fdOutputType;

  private Label wlFilename;
  private Button wbFilename;
  private TextVar wFilename;
  private FormData fdlFilename, fdbFilename, fdFilename;

  private Label wlOutputField;
  private TextVar wOutputField;
  private FormData fdlOutputField, fdOutputField;
  
  private Label wlSchema;
  private Button wbSchema;
  private TextVar wSchema;
  private FormData fdlSchema, fdbSchema, fdSchema;

  private Label wlCreateParentFolder;
  private Button wCreateParentFolder;
  private FormData fdlCreateParentFolder, fdCreateParentFolder;

  private Label wlAddStepnr;
  private Button wAddStepnr;
  private FormData fdlAddStepnr, fdAddStepnr;

  private Label wlAddPartnr;
  private Button wAddPartnr;
  private FormData fdlAddPartnr, fdAddPartnr;

  private Label wlAddDate;
  private Button wAddDate;
  private FormData fdlAddDate, fdAddDate;

  private Label wlAddTime;
  private Button wAddTime;
  private FormData fdlAddTime, fdAddTime;

  private Label wlDateTimeFormat;
  private CCombo wDateTimeFormat;
  private FormData fdlDateTimeFormat, fdDateTimeFormat;

  private Label wlCompression;
  private CCombo wCompression;
  private FormData fdlCompression, fdCompression;

  private Label wlSpecifyFormat;
  private Button wSpecifyFormat;
  private FormData fdlSpecifyFormat, fdSpecifyFormat;

  private Label wlCreateSchemaFile;
  private Button wCreateSchemaFile;
  private FormData fdlCreateSchemaFile, fdCreateSchemaFile;

  private Label wlWriteSchemaFile;
  private Button wWriteSchemaFile;
  private FormData fdlWriteSchemaFile, fdWriteSchemaFile;

  private Label wlRecordName;
  private TextVar wRecordName;
  private FormData fdlRecordName, fdRecordName;

  private Label wlNamespace;
  private TextVar wNamespace;
  private FormData fdlNamespace, fdNamespace;

  private Label wlDoc;
  private TextVar wDoc;
  private FormData fdlDoc, fdDoc;

  private TableView wFields;
  private FormData fdFields;

  private AvroOutputMeta input;

  private Label wlAddToResult;
  private Button wAddToResult;
  private FormData fdlAddToResult, fdAddToResult;

  private ColumnInfo[] colinf;

  private Button wUpdateTypes;
  private FormData fdUpdateTypes;

  private Link wDevelopedBy;
  private FormData fdDevelopedBy;

  private Map<String, Integer> inputFields;

  private Schema avroSchema;
  private boolean validSchema = false;
  private String[] avroFieldNames = null;

  public AvroOutputDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (AvroOutputMeta) in;
    inputFields = new HashMap<String, Integer>();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };

    final SelectionListener lsFlags = new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        setFlags();
      }
    };

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "AvroOutputDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.top = new FormAttachment( 0, margin );
    fdlStepname.right = new FormAttachment( middle, -margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );

    // ////////////////////////
    // START OF FILE TAB///
    // /
    wFileTab = new CTabItem( wTabFolder, SWT.NONE );
    wFileTab.setText( BaseMessages.getString( PKG, "AvroOutputDialog.FileTab.TabTitle" ) );

    Composite wFileComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFileComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout( fileLayout );

    //
    // Output type
    wlOutputType = new Label( wFileComp, SWT.RIGHT );
    wlOutputType.setText( BaseMessages.getString( PKG, "AvroOutputDialog.OutputType.Label" ) );
    props.setLook( wlOutputType );
    fdlOutputType = new FormData();
    fdlOutputType.left = new FormAttachment( 0, 0 );
    fdlOutputType.top = new FormAttachment( 0, margin );
    fdlOutputType.right = new FormAttachment( middle, -margin );
    wlOutputType.setLayoutData( fdlOutputType );

    wOutputType = new CCombo( wFileComp, SWT.BORDER | SWT.READ_ONLY );
    wOutputType.setEditable( false );
    props.setLook( wOutputType );
    wOutputType.addModifyListener( lsMod );
    wOutputType.addSelectionListener( lsFlags );
    fdOutputType = new FormData();
    fdOutputType.left = new FormAttachment( middle, 0 );
    fdOutputType.top = new FormAttachment( 0, margin );
    fdOutputType.right = new FormAttachment( 75, 0 );
    wOutputType.setLayoutData( fdOutputType );
    for ( String outputTypeDesc : OUTPUT_TYPE_DESC ) {
      wOutputType.add( outputTypeDesc );
    }

    // Filename line
    wlFilename = new Label( wFileComp, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wOutputType, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData( fdlFilename );

    wbFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename );
    wbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wOutputType, margin );
    wbFilename.setLayoutData( fdbFilename );

    wFilename = new TextVar( transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( wOutputType, margin );
    fdFilename.right = new FormAttachment( wbFilename, -margin );
    wFilename.setLayoutData( fdFilename );

    // Fieldname line
    wlOutputField = new Label( wFileComp, SWT.RIGHT );
    wlOutputField.setText( BaseMessages.getString( PKG, "AvroOutputDialog.OutputField.Label" ) );
    props.setLook( wlOutputField );
    fdlOutputField = new FormData();
    fdlOutputField.left = new FormAttachment( 0, 0 );
    fdlOutputField.top = new FormAttachment( wFilename, margin );
    fdlOutputField.right = new FormAttachment( middle, -margin );
    wlOutputField.setLayoutData( fdlOutputField );

    wOutputField = new TextVar( transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wOutputField );
    wOutputField.addModifyListener( lsMod );
    fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment( middle, 0 );
    fdOutputField.top = new FormAttachment( wFilename, margin );
    fdOutputField.right = new FormAttachment( 100, -margin );
    wOutputField.setLayoutData( fdOutputField );


    // Create Schema File
    //
    wlCreateSchemaFile = new Label( wFileComp, SWT.RIGHT );
    wlCreateSchemaFile.setText( BaseMessages.getString( PKG, "AvroOutputDialog.CreateSchemaFile.Label" ) );
    props.setLook( wlCreateSchemaFile );
    fdlCreateSchemaFile = new FormData();
    fdlCreateSchemaFile.left = new FormAttachment( 0, 0 );
    fdlCreateSchemaFile.top = new FormAttachment( wOutputField, margin );
    fdlCreateSchemaFile.right = new FormAttachment( middle, -margin );
    wlCreateSchemaFile.setLayoutData( fdlCreateSchemaFile );
    wCreateSchemaFile = new Button( wFileComp, SWT.CHECK );
    wCreateSchemaFile.setToolTipText( BaseMessages.getString( PKG, "AvroOutputDialog.CreateSchemaFile.Tooltip" ) );
    props.setLook( wCreateSchemaFile );
    fdCreateSchemaFile = new FormData();
    fdCreateSchemaFile.left = new FormAttachment( middle, 0 );
    fdCreateSchemaFile.top = new FormAttachment( wOutputField, margin );
    fdCreateSchemaFile.right = new FormAttachment( 100, 0 );
    wCreateSchemaFile.setLayoutData( fdCreateSchemaFile );
    wCreateSchemaFile.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setCreateSchemaFile();
      }
    } );

    // Write Schema File
    //
    wlWriteSchemaFile = new Label( wFileComp, SWT.RIGHT );
    wlWriteSchemaFile.setText( BaseMessages.getString( PKG, "AvroOutputDialog.WriteSchemaFile.Label" ) );
    props.setLook( wlWriteSchemaFile );
    fdlWriteSchemaFile = new FormData();
    fdlWriteSchemaFile.left = new FormAttachment( 0, 0 );
    fdlWriteSchemaFile.top = new FormAttachment( wCreateSchemaFile, margin );
    fdlWriteSchemaFile.right = new FormAttachment( middle, -margin );
    wlWriteSchemaFile.setLayoutData( fdlWriteSchemaFile );
    wWriteSchemaFile = new Button( wFileComp, SWT.CHECK );
    wWriteSchemaFile.setToolTipText( BaseMessages.getString( PKG, "AvroOutputDialog.WriteSchemaFile.Tooltip" ) );
    props.setLook( wWriteSchemaFile );
    fdWriteSchemaFile = new FormData();
    fdWriteSchemaFile.left = new FormAttachment( middle, 0 );
    fdWriteSchemaFile.top = new FormAttachment( wCreateSchemaFile, margin );
    fdWriteSchemaFile.right = new FormAttachment( 100, 0 );
    wWriteSchemaFile.setLayoutData( fdWriteSchemaFile );
    wWriteSchemaFile.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setCreateSchemaFile();
      }
    } );

    // Namespace
    //
    wlNamespace = new Label( wFileComp, SWT.RIGHT );
    wlNamespace.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Namespace.Label" ) );
    props.setLook( wlNamespace );
    fdlNamespace = new FormData();
    fdlNamespace.left = new FormAttachment( 0, 0 );
    fdlNamespace.top = new FormAttachment( wWriteSchemaFile, margin );
    fdlNamespace.right = new FormAttachment( middle, -margin );
    wlNamespace.setLayoutData( fdlNamespace );
    wNamespace = new TextVar( transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wNamespace );
    fdNamespace = new FormData();
    fdNamespace.left = new FormAttachment( middle, 0 );
    fdNamespace.top = new FormAttachment( wWriteSchemaFile, margin );
    fdNamespace.right = new FormAttachment( 100, 0 );
    wNamespace.setLayoutData( fdNamespace );
    wNamespace.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Record Name
    //
    wlRecordName = new Label( wFileComp, SWT.RIGHT );
    wlRecordName.setText( BaseMessages.getString( PKG, "AvroOutputDialog.RecordName.Label" ) );
    props.setLook( wlRecordName );
    fdlRecordName = new FormData();
    fdlRecordName.left = new FormAttachment( 0, 0 );
    fdlRecordName.top = new FormAttachment( wNamespace, margin );
    fdlRecordName.right = new FormAttachment( middle, -margin );
    wlRecordName.setLayoutData( fdlRecordName );
    wRecordName = new TextVar( transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRecordName );
    fdRecordName = new FormData();
    fdRecordName.left = new FormAttachment( middle, 0 );
    fdRecordName.top = new FormAttachment( wNamespace, margin );
    fdRecordName.right = new FormAttachment( 100, 0 );
    wRecordName.setLayoutData( fdRecordName );
    wRecordName.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Doc
    //
    wlDoc = new Label( wFileComp, SWT.RIGHT );
    wlDoc.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Doc.Label" ) );
    props.setLook( wlDoc );
    fdlDoc = new FormData();
    fdlDoc.left = new FormAttachment( 0, 0 );
    fdlDoc.top = new FormAttachment( wRecordName, margin );
    fdlDoc.right = new FormAttachment( middle, -margin );
    wlDoc.setLayoutData( fdlDoc );
    wDoc = new TextVar( transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDoc );
    fdDoc = new FormData();
    fdDoc.left = new FormAttachment( middle, 0 );
    fdDoc.top = new FormAttachment( wRecordName, margin );
    fdDoc.right = new FormAttachment( 100, 0 );
    wDoc.setLayoutData( fdDoc );
    wDoc.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Schema Filename line

    ModifyListener lsSchema = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        updateSchema();
      }
    };

    wlSchema = new Label( wFileComp, SWT.RIGHT );
    wlSchema.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Schema.Label" ) );
    props.setLook( wlSchema );
    fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment( 0, 0 );
    fdlSchema.top = new FormAttachment( wDoc, margin );
    fdlSchema.right = new FormAttachment( middle, -margin );
    wlSchema.setLayoutData( fdlSchema );

    wbSchema = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSchema );
    wbSchema.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbSchema = new FormData();
    fdbSchema.right = new FormAttachment( 100, 0 );
    fdbSchema.top = new FormAttachment( wDoc, 0 );
    wbSchema.setLayoutData( fdbSchema );

    wSchema = new TextVar( transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchema );
    wSchema.addModifyListener( lsMod );
    wSchema.addModifyListener( lsSchema );
    fdSchema = new FormData();
    fdSchema.left = new FormAttachment( middle, 0 );
    fdSchema.top = new FormAttachment( wDoc, margin );
    fdSchema.right = new FormAttachment( wbSchema, -margin );
    wSchema.setLayoutData( fdSchema );


    // Create Parent Folder?
    //
    wlCreateParentFolder = new Label( wFileComp, SWT.RIGHT );
    wlCreateParentFolder.setText( BaseMessages.getString( PKG, "AvroOutputDialog.CreateParentFolder.Label" ) );
    props.setLook( wlCreateParentFolder );
    fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment( 0, 0 );
    fdlCreateParentFolder.top = new FormAttachment( wSchema, margin );
    fdlCreateParentFolder.right = new FormAttachment( middle, -margin );
    wlCreateParentFolder.setLayoutData( fdlCreateParentFolder );
    wCreateParentFolder = new Button( wFileComp, SWT.CHECK );
    wCreateParentFolder.setToolTipText( BaseMessages.getString( PKG, "AvroOutputDialog.CreateParentFolder.Tooltip" ) );
    props.setLook( wCreateParentFolder );
    fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment( middle, 0 );
    fdCreateParentFolder.top = new FormAttachment( wSchema, margin );
    fdCreateParentFolder.right = new FormAttachment( 100, 0 );
    wCreateParentFolder.setLayoutData( fdCreateParentFolder );
    wCreateParentFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Compression
    //
    // DateTimeFormat
    wlCompression = new Label( wFileComp, SWT.RIGHT );
    wlCompression.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Compression.Label" ) );
    props.setLook( wlCompression );
    fdlCompression = new FormData();
    fdlCompression.left = new FormAttachment( 0, 0 );
    fdlCompression.top = new FormAttachment( wCreateParentFolder, margin );
    fdlCompression.right = new FormAttachment( middle, -margin );
    wlCompression.setLayoutData( fdlCompression );
    wCompression = new CCombo( wFileComp, SWT.BORDER | SWT.READ_ONLY );
    wCompression.setEditable( true );
    props.setLook( wCompression );
    wCompression.addModifyListener( lsMod );
    fdCompression = new FormData();
    fdCompression.left = new FormAttachment( middle, 0 );
    fdCompression.top = new FormAttachment( wCreateParentFolder, margin );
    fdCompression.right = new FormAttachment( 75, 0 );
    wCompression.setLayoutData( fdCompression );
    String[] compressions = AvroOutputMeta.compressionTypes;
    for ( String compression : compressions ) {
      wCompression.add( compression );
    }

    // Create multi-part file?
    wlAddStepnr = new Label( wFileComp, SWT.RIGHT );
    wlAddStepnr.setText( BaseMessages.getString( PKG, "AvroOutputDialog.AddStepnr.Label" ) );
    props.setLook( wlAddStepnr );
    fdlAddStepnr = new FormData();
    fdlAddStepnr.left = new FormAttachment( 0, 0 );
    fdlAddStepnr.top = new FormAttachment( wCompression, margin );
    fdlAddStepnr.right = new FormAttachment( middle, -margin );
    wlAddStepnr.setLayoutData( fdlAddStepnr );
    wAddStepnr = new Button( wFileComp, SWT.CHECK );
    props.setLook( wAddStepnr );
    fdAddStepnr = new FormData();
    fdAddStepnr.left = new FormAttachment( middle, 0 );
    fdAddStepnr.top = new FormAttachment( wCompression, margin );
    fdAddStepnr.right = new FormAttachment( 100, 0 );
    wAddStepnr.setLayoutData( fdAddStepnr );
    wAddStepnr.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Create multi-part file?
    wlAddPartnr = new Label( wFileComp, SWT.RIGHT );
    wlAddPartnr.setText( BaseMessages.getString( PKG, "AvroOutputDialog.AddPartnr.Label" ) );
    props.setLook( wlAddPartnr );
    fdlAddPartnr = new FormData();
    fdlAddPartnr.left = new FormAttachment( 0, 0 );
    fdlAddPartnr.top = new FormAttachment( wAddStepnr, margin );
    fdlAddPartnr.right = new FormAttachment( middle, -margin );
    wlAddPartnr.setLayoutData( fdlAddPartnr );
    wAddPartnr = new Button( wFileComp, SWT.CHECK );
    props.setLook( wAddPartnr );
    fdAddPartnr = new FormData();
    fdAddPartnr.left = new FormAttachment( middle, 0 );
    fdAddPartnr.top = new FormAttachment( wAddStepnr, margin );
    fdAddPartnr.right = new FormAttachment( 100, 0 );
    wAddPartnr.setLayoutData( fdAddPartnr );
    wAddPartnr.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Create multi-part file?
    wlAddDate = new Label( wFileComp, SWT.RIGHT );
    wlAddDate.setText( BaseMessages.getString( PKG, "AvroOutputDialog.AddDate.Label" ) );
    props.setLook( wlAddDate );
    fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment( 0, 0 );
    fdlAddDate.top = new FormAttachment( wAddPartnr, margin );
    fdlAddDate.right = new FormAttachment( middle, -margin );
    wlAddDate.setLayoutData( fdlAddDate );
    wAddDate = new Button( wFileComp, SWT.CHECK );
    props.setLook( wAddDate );
    fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment( middle, 0 );
    fdAddDate.top = new FormAttachment( wAddPartnr, margin );
    fdAddDate.right = new FormAttachment( 100, 0 );
    wAddDate.setLayoutData( fdAddDate );
    wAddDate.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        // System.out.println("wAddDate.getSelection()="+wAddDate.getSelection());
      }
    } );
    // Create multi-part file?
    wlAddTime = new Label( wFileComp, SWT.RIGHT );
    wlAddTime.setText( BaseMessages.getString( PKG, "AvroOutputDialog.AddTime.Label" ) );
    props.setLook( wlAddTime );
    fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment( 0, 0 );
    fdlAddTime.top = new FormAttachment( wAddDate, margin );
    fdlAddTime.right = new FormAttachment( middle, -margin );
    wlAddTime.setLayoutData( fdlAddTime );
    wAddTime = new Button( wFileComp, SWT.CHECK );
    props.setLook( wAddTime );
    fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment( middle, 0 );
    fdAddTime.top = new FormAttachment( wAddDate, margin );
    fdAddTime.right = new FormAttachment( 100, 0 );
    wAddTime.setLayoutData( fdAddTime );
    wAddTime.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Specify date time format?
    wlSpecifyFormat = new Label( wFileComp, SWT.RIGHT );
    wlSpecifyFormat.setText( BaseMessages.getString( PKG, "AvroOutputDialog.SpecifyFormat.Label" ) );
    props.setLook( wlSpecifyFormat );
    fdlSpecifyFormat = new FormData();
    fdlSpecifyFormat.left = new FormAttachment( 0, 0 );
    fdlSpecifyFormat.top = new FormAttachment( wAddTime, margin );
    fdlSpecifyFormat.right = new FormAttachment( middle, -margin );
    wlSpecifyFormat.setLayoutData( fdlSpecifyFormat );
    wSpecifyFormat = new Button( wFileComp, SWT.CHECK );
    props.setLook( wSpecifyFormat );
    wSpecifyFormat.setToolTipText( BaseMessages.getString( PKG, "AvroOutputDialog.SpecifyFormat.Tooltip" ) );
    fdSpecifyFormat = new FormData();
    fdSpecifyFormat.left = new FormAttachment( middle, 0 );
    fdSpecifyFormat.top = new FormAttachment( wAddTime, margin );
    fdSpecifyFormat.right = new FormAttachment( 100, 0 );
    wSpecifyFormat.setLayoutData( fdSpecifyFormat );
    wSpecifyFormat.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setDateTimeFormat();
      }
    } );

    // DateTimeFormat
    wlDateTimeFormat = new Label( wFileComp, SWT.RIGHT );
    wlDateTimeFormat.setText( BaseMessages.getString( PKG, "AvroOutputDialog.DateTimeFormat.Label" ) );
    props.setLook( wlDateTimeFormat );
    fdlDateTimeFormat = new FormData();
    fdlDateTimeFormat.left = new FormAttachment( 0, 0 );
    fdlDateTimeFormat.top = new FormAttachment( wSpecifyFormat, margin );
    fdlDateTimeFormat.right = new FormAttachment( middle, -margin );
    wlDateTimeFormat.setLayoutData( fdlDateTimeFormat );
    wDateTimeFormat = new CCombo( wFileComp, SWT.BORDER | SWT.READ_ONLY );
    wDateTimeFormat.setEditable( true );
    props.setLook( wDateTimeFormat );
    wDateTimeFormat.addModifyListener( lsMod );
    fdDateTimeFormat = new FormData();
    fdDateTimeFormat.left = new FormAttachment( middle, 0 );
    fdDateTimeFormat.top = new FormAttachment( wSpecifyFormat, margin );
    fdDateTimeFormat.right = new FormAttachment( 75, 0 );
    wDateTimeFormat.setLayoutData( fdDateTimeFormat );
    String[] dats = Const.getDateFormats();
    for ( String dat : dats ) {
      wDateTimeFormat.add( dat );
    }

    // Add File to the result files name
    wlAddToResult = new Label( wFileComp, SWT.RIGHT );
    wlAddToResult.setText( BaseMessages.getString( PKG, "AvroOutputDialog.AddFileToResult.Label" ) );
    props.setLook( wlAddToResult );
    fdlAddToResult = new FormData();
    fdlAddToResult.left = new FormAttachment( 0, 0 );
    fdlAddToResult.top = new FormAttachment( wDateTimeFormat, 2 * margin );
    fdlAddToResult.right = new FormAttachment( middle, -margin );
    wlAddToResult.setLayoutData( fdlAddToResult );
    wAddToResult = new Button( wFileComp, SWT.CHECK );
    wAddToResult.setToolTipText( BaseMessages.getString( PKG, "AvroOutputDialog.AddFileToResult.Tooltip" ) );
    props.setLook( wAddToResult );
    fdAddToResult = new FormData();
    fdAddToResult.left = new FormAttachment( middle, 0 );
    fdAddToResult.top = new FormAttachment( wDateTimeFormat, 2 * margin );
    fdAddToResult.right = new FormAttachment( 100, 0 );
    wAddToResult.setLayoutData( fdAddToResult );
    SelectionAdapter lsSelR = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wAddToResult.addSelectionListener( lsSelR );

    fdFileComp = new FormData();
    fdFileComp.left = new FormAttachment( 0, 0 );
    fdFileComp.top = new FormAttachment( 0, 0 );
    fdFileComp.right = new FormAttachment( 100, 0 );
    fdFileComp.bottom = new FormAttachment( 100, 0 );
    wFileComp.setLayoutData( fdFileComp );

    wFileComp.layout();
    wFileTab.setControl( wFileComp );

    // ///////////////////////////////////////////////////////////
    // / END OF FILE TAB
    // ///////////////////////////////////////////////////////////

    // Fields tab...
    //
    wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( PKG, "AvroOutputDialog.FieldsTab.TabTitle" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook( wFieldsComp );

    wGet = new Button( wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "System.Button.GetFields" ) );
    wGet.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.GetFields" ) );
    fdGet = new FormData();
    fdGet.right = new FormAttachment( 50, -margin );
    fdGet.bottom = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    wUpdateTypes = new Button( wFieldsComp, SWT.PUSH );
    wUpdateTypes.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Button.UpdateTypes" ) );
    wUpdateTypes.setToolTipText( BaseMessages.getString( PKG, "AvroOutputDialog.Tooltip.UpdateTypes" ) );
    fdUpdateTypes = new FormData();
    fdUpdateTypes.left = new FormAttachment( wGet, margin*2 );
    fdUpdateTypes.bottom = new FormAttachment( 100, 0 );
    wUpdateTypes.setLayoutData( fdUpdateTypes );

    final int FieldsCols = 4;
    final int FieldsRows = input.getOutputFields().length;

    colinf = new ColumnInfo[FieldsCols];
    colinf[0] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "AvroOutputDialog.StreamColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    colinf[1] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "AvroOutputDialog.AvroColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, getSchemaFields(), false );
    colinf[2] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "AvroOutputDialog.AvroType.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, AvroOutputField.getAvroTypeArraySorted(), false );
    colinf[3] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "AvroOutputDialog.Nullable.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, YES_NO_COMBO, false );

    colinf[2].setComboValuesSelectionListener( new ComboValuesSelectionListener( ) {
      @Override public String[] getComboValues( TableItem tableItem, int rowNr, int colNr ) {
        String[] comboValues = new String[] {};
        if( !( wCreateSchemaFile.getSelection() ) && validSchema ) {
          String avroColumn = tableItem.getText( colNr - 1 );
          String streamColumn = tableItem.getText( colNr - 2 );
          avroColumn = ( avroColumn != null ? avroColumn : streamColumn );

          Schema fieldSchema = getFieldSchema( avroColumn );
          if( fieldSchema != null ) {
          String[] combo = AvroOutputField.mapAvroType( fieldSchema,
            fieldSchema.getType() );
          comboValues = combo;
            fieldSchema = null;
          } else {
            comboValues = AvroOutputField.getAvroTypeArraySorted();
          }
        } else {
              comboValues = AvroOutputField.getAvroTypeArraySorted();
        }
         return comboValues;
      }
    });

    wFields =
      new TableView(
        transMeta, wFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, -margin );
    wFields.setLayoutData( fdFields );

    //
    // Search the fields in the background

    final Runnable runnable = new Runnable() {
      public void run() {
        StepMeta stepMeta = transMeta.findStep( stepname );
        if ( stepMeta != null ) {
          try {
            RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );

            // Remember these fields...
            for ( int i = 0; i < row.size(); i++ ) {
              inputFields.put( row.getValueMeta( i ).getName(), i );
            }
            setComboBoxes();
          } catch ( KettleException e ) {
            logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
          }
        }
      }
    };
    new Thread( runnable ).start();

    fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fdFieldsComp );

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wStepname, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fdTabFolder );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, wTabFolder );

    wDevelopedBy = new Link( shell, SWT.PUSH );
    wDevelopedBy.setText("Developed by Inquidia Consulting (<a href=\"http://www.inquidia.com\">www.inquidia.com</a>)");
    fdDevelopedBy = new FormData();
    fdDevelopedBy.right = new FormAttachment( 100, margin );
    fdDevelopedBy.bottom = new FormAttachment( 100, margin );
    wDevelopedBy.setLayoutData( fdDevelopedBy );
    wDevelopedBy.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        Program.launch("http://www.inquidia.com");
      }
    } );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        get();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    Listener lsUpdateTypes = new Listener() {
      public void handleEvent( Event e )
      {
        updateTypes();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wGet.addListener( SWT.Selection, lsGet );
    wUpdateTypes.addListener( SWT.Selection, lsUpdateTypes );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wFilename.addSelectionListener( lsDef );
    wSchema.addSelectionListener( lsDef );

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wFilename.setToolTipText( transMeta.environmentSubstitute( wFilename.getText() ) );
      }
    } );

    wbFilename.addSelectionListener( new SelectionAdapter() {
        public void widgetSelected( SelectionEvent e ) {
          FileDialog dialog = new FileDialog( shell, SWT.SAVE );
          dialog.setFilterExtensions( new String[] { "*.avro", "*" } );
          if ( wFilename.getText() != null ) {
            dialog.setFileName( transMeta.environmentSubstitute( wFilename.getText() ) );
          }
          dialog.setFilterNames( new String[] {
            BaseMessages.getString( PKG, "AvroOutputDialog.FileType.Avro" ),
            BaseMessages.getString( PKG, "System.FileType.AllFiles" ) } );
          if ( dialog.open() != null ) {
              wFilename.setText( dialog.getFilterPath()
                + System.getProperty( "file.separator" ) + dialog.getFileName() );
          }
        }
      } );

   // Whenever something changes, set the tooltip to the expanded version:
    wSchema.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wSchema.setToolTipText( transMeta.environmentSubstitute( wSchema.getText() ) );
      }
    } );

    wbSchema.addSelectionListener( new SelectionAdapter() {
        public void widgetSelected( SelectionEvent e ) {
          FileDialog dialog = new FileDialog( shell, SWT.SAVE );
          dialog.setFilterExtensions( new String[] { "*.json;*.avsc", "*" } );
          if ( wSchema.getText() != null ) {
            dialog.setFileName( transMeta.environmentSubstitute( wSchema.getText() ) );
          }
          dialog.setFilterNames( new String[] {
            BaseMessages.getString( PKG, "AvroOutputDialog.FileType.Schema" ),
            BaseMessages.getString( PKG, "System.FileType.AllFiles" ) } );
          if ( dialog.open() != null ) {
            wSchema.setText( dialog.getFilterPath()
              + System.getProperty( "file.separator" ) + dialog.getFileName() );
            updateSchema();
          }
        }
      } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    lsResize = new Listener() {
      public void handleEvent( Event event ) {
        Point size = shell.getSize();
        wFields.setSize( size.x - 10, size.y - 50 );
        wFields.table.setSize( size.x - 10, size.y - 50 );
        wFields.redraw();
      }
    };
    shell.addListener( SWT.Resize, lsResize );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );
    setCreateSchemaFile();
    setFlags();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  private Schema getFieldSchema( String avroName )
  {
    if( avroName.startsWith( "$." ) )
    {
      avroName = avroName.substring( 2 );
    }

    Schema recordSchema = avroSchema;
    while( avroName.contains( "." ) )
    {
      String parentRecord = avroName.substring( 0, avroName.indexOf( "." ) );
      avroName = avroName.substring( avroName.indexOf( "." )+1 );
      Schema.Field field = recordSchema.getField( parentRecord );
      if( field == null )
      {
        return null;
      } else {
        recordSchema = field.schema();
      }
    }
    Schema.Field f = recordSchema.getField( avroName );
    recordSchema = null;
    if( f == null )
    {
      return null;
    }
    return f.schema();
  }


  private void updateSchema()
  {

    try {
      avroSchema = null;
      avroSchema = new Schema.Parser().parse( new File( transMeta.environmentSubstitute( wSchema.getText() ) ) );

      validSchema = true;

      wFields.setColumnInfo( 1, new ColumnInfo(
        BaseMessages.getString( PKG, "AvroOutputDialog.AvroColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, getSchemaFields(), false ) );
      avroSchema = null;
    } catch (Exception ex) {
      validSchema = false;
      avroSchema = null;
    }

  }

  private ArrayList<String> getFieldsList( Schema schema, String parent )
  {
    ArrayList<String> result = new ArrayList<String>();
    if( schema.getType() != Schema.Type.RECORD )
    {
      return result;
    }

    List<Schema.Field> fields = schema.getFields();
    Iterator<Schema.Field> it = fields.iterator();

    while( it.hasNext() )
    {
      Schema.Field f = it.next();
      List<Schema> fieldSchemas;
      if( f.schema().getType() == Schema.Type.UNION )
      {
        fieldSchemas = f.schema().getTypes();
      } else {
        fieldSchemas = new ArrayList<Schema>();
        fieldSchemas.add( f.schema() );
      }

      Iterator<Schema> itTypes = fieldSchemas.iterator();
      boolean added = false;
      while( itTypes.hasNext() )
      {
        Schema type = itTypes.next();
        if( type.getType() == Schema.Type.NULL )
        {
          //do nothing
        } else if ( type.getType() == Schema.Type.RECORD )
        {
          String child = "$."+f.name();
          if( parent!= null && parent.length()>0 && !parent.equals( "$." ) )
          {
            child = parent+"."+f.name();
          }
          ArrayList<String> children = getFieldsList( type, child );
          result.addAll( children );
        } else if ( !added )
        {
          added = true;
          String name = "$."+f.name();
          if( parent!= null && parent.length()>0 && !parent.equals( "$." ) )
          {
            name = parent+"."+f.name();
          }
          result.add( name );
        }
        type = null;
      }

    }

    return result;

  }

  private String[] getSchemaFields()
  {
    String[] avroFields;

    if( validSchema )
    {
      ArrayList<String> fields = getFieldsList( avroSchema, "$." );
      String field = "";
      avroFields = new String[fields.size() + 1];
      avroFields[0]="";
      Iterator<String> itNames = fields.iterator();
      int i = 1;
      while( itNames.hasNext() )
      {
        avroFields[i] = itNames.next();
        i++;
      }
    } else {
      avroFields = new String[1];
      avroFields[0]="";
    }
    avroFieldNames = avroFields;
    return avroFields;
  }

    protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<String, Integer>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<String>( keySet );

    String[] fieldNames = entries.toArray( new String[entries.size()] );

    Const.sortStrings( fieldNames );
    colinf[0].setComboValues( fieldNames );
  }

  private void setFlags() {
    int outputTypeId = wOutputType.getSelectionIndex();
    if( outputTypeId == AvroOutputMeta.OUTPUT_TYPE_BINARY_FILE ) {
      wFilename.setEnabled( true );
      wCompression.setEnabled( true );
      wAddStepnr.setEnabled( true );
      wAddPartnr.setEnabled( true );
      wAddDate.setEnabled( true );
      wAddTime.setEnabled( true );
      wSpecifyFormat.setEnabled( true );
      wDateTimeFormat.setEnabled( true );
      wOutputField.setEnabled( false );
    } else if( outputTypeId == AvroOutputMeta.OUTPUT_TYPE_FIELD
        || outputTypeId == AvroOutputMeta.OUTPUT_TYPE_JSON_FIELD ) {
      wFilename.setEnabled( false );
      wCompression.setEnabled( false );
      wAddStepnr.setEnabled( false );
      wAddPartnr.setEnabled( false );
      wAddDate.setEnabled( false );
      wAddTime.setEnabled( false );
      wSpecifyFormat.setEnabled( false );
      wDateTimeFormat.setEnabled( false );
      wOutputField.setEnabled( true );
    }
  }

  private void setCreateSchemaFile() {
    wDoc.setEnabled( wCreateSchemaFile.getSelection() );
    wNamespace.setEnabled( wCreateSchemaFile.getSelection() );
    wRecordName.setEnabled( wCreateSchemaFile.getSelection() );
    wWriteSchemaFile.setEnabled( wCreateSchemaFile.getSelection() );
    if( wWriteSchemaFile.getSelection() || (! wCreateSchemaFile.getSelection() ) ) {
      wSchema.setEnabled( true );
      wbSchema.setEnabled( true );
    } else {
      wSchema.setEnabled( false );
      wbSchema.setEnabled( false );
    }
  }

  private void setDateTimeFormat() {
    if ( wSpecifyFormat.getSelection() ) {
      wAddDate.setSelection( false );
      wAddTime.setSelection( false );
    }

    wDateTimeFormat.setEnabled( wSpecifyFormat.getSelection() );
    wlDateTimeFormat.setEnabled( wSpecifyFormat.getSelection() );
    wAddDate.setEnabled( !( wSpecifyFormat.getSelection() ) );
    wlAddDate.setEnabled( !( wSpecifyFormat.getSelection() ) );
    wAddTime.setEnabled( !( wSpecifyFormat.getSelection() ) );
    wlAddTime.setEnabled( !( wSpecifyFormat.getSelection() ) );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {

    if( input.getOutputTypeId() >= 0 && input.getOutputTypeId() < OUTPUT_TYPE_DESC.length ) {
      wOutputType.setText( OUTPUT_TYPE_DESC[input.getOutputTypeId()] );
    }
    if ( input.getFileName() != null ) {
      wFilename.setText( input.getFileName() );
    }
    wOutputField.setText( Const.NVL( input.getOutputFieldName(), "" ) );
    if( input.getSchemaFileName() != null ) {
    	wSchema.setText( input.getSchemaFileName() );
      updateSchema();
    }

    if ( input.getNamespace() != null )
    {
      wNamespace.setText( input.getNamespace() );
    }
    if ( input.getDoc() != null )
    {
      wDoc.setText( input.getDoc() );
    }
    if ( input.getRecordName() != null )
    {
      wRecordName.setText( input.getRecordName() );
    }

    wCreateParentFolder.setSelection( input.getCreateParentFolder() );
    wCreateSchemaFile.setSelection( input.getCreateSchemaFile() );
    wWriteSchemaFile.setSelection( input.getWriteSchemaFile() );
    wCompression.setText( Const.NVL( input.getCompressionType(), "" ) );

    wAddDate.setSelection( input.getDateInFilename() );
    wAddTime.setSelection( input.getTimeInFilename() );
    wDateTimeFormat.setText( Const.NVL( input.getDateTimeFormat(), "" ) );
    wSpecifyFormat.setSelection( input.getSpecifyingFormat() );

    wAddStepnr.setSelection( input.getStepNrInFilename() );
    wAddPartnr.setSelection( input.getPartNrInFilename() );
    wAddToResult.setSelection( input.getAddToResultFiles() );
    logDebug( "getting fields info..." );

    for ( int i = 0; i < input.getOutputFields().length; i++ ) {
      AvroOutputField field = input.getOutputFields()[i];

      TableItem item = wFields.table.getItem( i );
      if ( field.getName() != null ) {
        item.setText( 1, field.getName() );
      }
      if( field.getAvroName() != null ) {
    	item.setText( 2, field.getAvroName() );
      }
      if ( field.getAvroTypeDesc() != null ) {
        item.setText( 3, field.getAvroTypeDesc() );
      }
      if( field.getNullable() )
      {
        item.setText( 4, "Y" );
      } else {
        item.setText( 4, "N" );
      }
    }

    wFields.optWidth( true );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    avroSchema = null;

    input.setChanged( backupChanged );

    dispose();
  }

  private void getInfo( AvroOutputMeta tfoi ) {
    tfoi.setOutputTypeById( wOutputType.getSelectionIndex() );
    tfoi.setFileName( wFilename.getText() );
    tfoi.setOutputFieldName( wOutputField.getText() );
    tfoi.setSchemaFileName( wSchema.getText() );
    tfoi.setCreateSchemaFile( wCreateSchemaFile.getSelection() );
    tfoi.setWriteSchemaFile( wWriteSchemaFile.getSelection() );
    tfoi.setNamespace( wNamespace.getText() );
    tfoi.setRecordName( wRecordName.getText() );
    tfoi.setDoc( wDoc.getText() );
    tfoi.setCompressionType( wCompression.getText() );
    tfoi.setCreateParentFolder( wCreateParentFolder.getSelection() );
    tfoi.setStepNrInFilename( wAddStepnr.getSelection() );
    tfoi.setPartNrInFilename( wAddPartnr.getSelection() );
    tfoi.setDateInFilename( wAddDate.getSelection() );
    tfoi.setTimeInFilename( wAddTime.getSelection() );
    tfoi.setDateTimeFormat( wDateTimeFormat.getText() );
    tfoi.setSpecifyingFormat( wSpecifyFormat.getSelection() );
    tfoi.setAddToResultFiles( wAddToResult.getSelection() );
    int i;
    // Table table = wFields.table;

    int nrfields = wFields.nrNonEmpty();

    tfoi.allocate( nrfields );

    for ( i = 0; i < nrfields; i++ ) {
      AvroOutputField field = new AvroOutputField();

      TableItem item = wFields.getNonEmpty( i );
      field.setName( item.getText( 1 ) );
      field.setAvroName( item.getText( 2 ) );
      field.setAvroType( item.getText( 3 ) );
      boolean nullable = "Y".equalsIgnoreCase( item.getText( 4 ) );
      if( item.getText(4).isEmpty() )
      {
        nullable = true;
      }
      field.setNullable( nullable );

      //CHECKSTYLE:Indentation:OFF
      tfoi.getOutputFields()[i] = field;
    }
  }

  private void ok() {
    avroSchema = null;
    if ( Const.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value

    getInfo( input );

    dispose();
  }

  private void updateTypes() {
      try {
        RowMetaInterface r = transMeta.getPrevStepFields( stepname );
        if ( r != null ) {
          TableItemInsertListener listener = new TableItemInsertListener() {
            public boolean tableItemInserted( TableItem tableItem, ValueMetaInterface v ) {

              return true;
            }
          };
          updateTypes( r, wFields, 1, 2, new int[] { 3 } );
        }
      } catch ( KettleException ke ) {
        new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
          .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ke );
      }
  }

  private void get() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null ) {
        TableItemInsertListener listener = new TableItemInsertListener() {
          public boolean tableItemInserted( TableItem tableItem, ValueMetaInterface v ) {

            return true;
          }
        };
        getFieldsFromPreviousAvro( r, wFields, 1, new int[] { 1 }, new int[] { 2 }, new int[] { 3 } , listener );
      }
    } catch ( KettleException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
        .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ke );
    }

  }

  public void updateTypes( RowMetaInterface row, TableView tableView, int nameColumn, int avroNameColumn, int[] typeColumn )
  {
    boolean createSchemaFile = wCreateSchemaFile.getSelection();
    if( validSchema || createSchemaFile ) {
      Table table = tableView.table;

      boolean typesPopulated = false;
      for ( int i = 0; i < table.getItemCount(); i++ ) {
        TableItem tableItem = table.getItem( i );

        if ( !typesPopulated ) {
          for ( int c = 0; c < typeColumn.length; c++ ) {
            if ( !Const.isEmpty( tableItem.getText( typeColumn[ c ] ) ) ) {
              typesPopulated = true;
              break;
            }
          }
          if ( typesPopulated ) {
            break;
          }
        }

      }

      int choice = 0;

      if ( typesPopulated ) {
        //Ask what we should sdo with the existing data
        MessageDialog md =
          new MessageDialog( tableView.getShell(),
            BaseMessages.getString( PKG, "AvroOutputDialog.GetTypesChoice.Title" ),
            null,
            BaseMessages.getString( PKG, "AvroOutputDialog.GetTypesChoice.Message" ),
            MessageDialog.WARNING, new String[] {
            BaseMessages.getString( PKG, "AvroOutputDialog.GetTypesChoice.AddNew" ),
            BaseMessages.getString( PKG, "AvroOutputDialog.GetTypesChoice.ReplaceAll" ),
            BaseMessages.getString( PKG, "AvroOutputDialog.GetTypesChoice.Cancel" ) }, 0 );
        MessageDialog.setDefaultImage( GUIResource.getInstance().getImageSpoon() );
        int idx = md.open();
        choice = idx & 0xFF;
      }

      if ( choice == 2 || choice == 255 ) {
        return; //nothing to do
      }

      for ( int i = 0; i < table.getItemCount(); i++ ) {
        TableItem tableItem = table.getItem( i );
        String avroName = !( tableItem.getText( 2 ).isEmpty() ) ? tableItem.getText( 2 ) : tableItem.getText( 1 );
        String streamName = tableItem.getText( 1 );
        logBasic("Stream name is "+streamName );
        if ( !( avroName.isEmpty() ) && !( createSchemaFile ) ) {
          Schema fieldSchema = getFieldSchema( avroName );
          if ( fieldSchema != null ) {
            String[] types = AvroOutputField.mapAvroType( fieldSchema, fieldSchema.getType() );
            if ( types.length > 0 ) {
              for ( int c = 0; c < typeColumn.length; c++ ) {
                if ( choice == 0 && !Const.isEmpty( tableItem.getText( typeColumn[ c ] ) ) ) {
                  //do nothing
                } else {
                  tableItem.setText( typeColumn[ c ], types[ 0 ] );
                }
              }
            } else if ( choice == 1 ) {
              for ( int c = 0; c < typeColumn.length; c++ ) {
                tableItem.setText( typeColumn[ c ], "" );
              }
            }
          } else if ( choice == 1 ) {
            for ( int c = 0; c < typeColumn.length; c++ ) {
              tableItem.setText( typeColumn[ c ], "" );
            }
          }
        } else if( createSchemaFile && !( streamName.isEmpty() ) ) { //create schema file
          logBasic("Create File");
          ValueMetaInterface v = row.searchValueMeta( streamName );
          logBasic( v != null ? v.getName() : "Value Meta is null" );
          String avroType = "";
          if( v != null ) {
            avroType = AvroOutputField.getAvroTypeDesc( AvroOutputField.getDefaultAvroType( v.getType() ) );
          }
          for ( int aTypeColumn : typeColumn ) {
            if ( choice == 1 || ( choice == 0 && ( tableItem.getText( aTypeColumn ).isEmpty() ) ) ) {
              tableItem.setText( aTypeColumn, avroType );
            }
          }
        }

      }
    } else {
      MessageDialog md =
        new MessageDialog( tableView.getShell(),
          BaseMessages.getString( PKG, "AvroOutputDialog.GetTypesChoice.BadSchema.Title" ),
          null,
          BaseMessages.getString( PKG, "AvroOutputDialog.GetTypesChoice.BadSchema.Message"),
          MessageDialog.WARNING, new String[] {
            BaseMessages.getString( PKG, "System.Button.OK" )}, 0);
      MessageDialog.setDefaultImage( GUIResource.getInstance().getImageSpoon() );
      int idx = md.open();
    }
  }

  public void getFieldsFromPreviousAvro( RowMetaInterface row, TableView tableView, int keyColumn,
		    int[] nameColumn, int[] avroNameColumn, int[] avroTypeColumn, TableItemInsertListener listener ) {
		    if ( row == null || row.size() == 0 ) {
		      return; // nothing to do
		    }

		    Table table = tableView.table;

		    // get a list of all the non-empty keys (names)
		    //
		    List<String> keys = new ArrayList<String>();
		    for ( int i = 0; i < table.getItemCount(); i++ ) {
		      TableItem tableItem = table.getItem( i );
		      String key = tableItem.getText( keyColumn );
		      if ( !Const.isEmpty( key ) && keys.indexOf( key ) < 0 ) {
		        keys.add( key );
		      }
		    }

		    int choice = 0;

		    if ( keys.size() > 0 ) {
		      // Ask what we should do with the existing data in the step.
		      //
		      MessageDialog md =
		        new MessageDialog( tableView.getShell(),
		          BaseMessages.getString( PKG, "BaseStepDialog.GetFieldsChoice.Title" ), // "Warning!"
		          null,
		          BaseMessages.getString( PKG, "BaseStepDialog.GetFieldsChoice.Message", "" + keys.size(), "" + row.size() ),
		          MessageDialog.WARNING, new String[] {
		            BaseMessages.getString( PKG, "BaseStepDialog.AddNew" ),
		            BaseMessages.getString( PKG, "BaseStepDialog.Add" ),
		            BaseMessages.getString( PKG, "BaseStepDialog.ClearAndAdd" ),
		            BaseMessages.getString( PKG, "BaseStepDialog.Cancel" ), }, 0 );
		      MessageDialog.setDefaultImage( GUIResource.getInstance().getImageSpoon() );
		      int idx = md.open();
		      choice = idx & 0xFF;
		    }

		    if ( choice == 3 || choice == 255 ) {
		      return; // Cancel clicked
		    }

		    if ( choice == 2 ) {
		      tableView.clearAll( false );
		    }

		    for ( int i = 0; i < row.size(); i++ ) {
		      ValueMetaInterface v = row.getValueMeta( i );

		      boolean add = true;

		      if ( choice == 0 ) { // hang on, see if it's not yet in the table view

		        if ( keys.indexOf( v.getName() ) >= 0 ) {
		          add = false;
		        }
		      }

		      if ( add ) {
		        TableItem tableItem = new TableItem( table, SWT.NONE );

            for ( int aNameColumn : nameColumn ) {
              tableItem.setText( aNameColumn, Const.NVL( v.getName(), "" ) );
            }
            if( validSchema && !wCreateSchemaFile.getSelection() ) {
              for ( int anAvroNameColumn : avroNameColumn ) {
                for( int o = 0; o < avroFieldNames.length; o++ ) {
                  String fieldName = avroFieldNames[o];
                  String matchName = fieldName;
                  if( fieldName != null && fieldName.startsWith( "$." ) )
                  {
                    matchName = fieldName.substring( 2 );
                  }
                  if ( matchName.equalsIgnoreCase( Const.NVL( v.getName(), "" ) ) ) {
                    if( fieldName != null && fieldName.length() > 0 )
                    {
                      fieldName = "$."+matchName;
                    }
                    tableItem.setText( anAvroNameColumn, Const.NVL( fieldName, "" ) );
                    try {

                      Schema fieldSchema = getFieldSchema( fieldName );
                      String[] avroType = AvroOutputField.mapAvroType( fieldSchema, fieldSchema.getType() );
                      if( avroType.length>0 ) {
                        for ( int anAvroTypeColumn : avroTypeColumn ) {
                          tableItem.setText( anAvroTypeColumn, avroType[0] );
                        }
                      }
                    } catch ( Exception ex ) {
                    }
                    break;
                  }
                }
              }
            } else if ( wCreateSchemaFile.getSelection() )
            {
              for( int aNameColumn : nameColumn ) {
                tableItem.setText( aNameColumn, Const.NVL( v.getName(), "" ) );
              }

              String avroName = v.getName();
              if( avroName != null && avroName.length() > 0 ) {
                avroName = "$." + avroName;
              }
              for( int anAvroNameColumn : avroNameColumn ) {
               tableItem.setText( anAvroNameColumn, Const.NVL( avroName, "" ) );
              }

              String avroType = AvroOutputField.getAvroTypeDesc( AvroOutputField.getDefaultAvroType( v.getType() ) );
              for( int anAvroTypeColumn : avroTypeColumn )
              {
                tableItem.setText( anAvroTypeColumn, Const.NVL( avroType, "" ) );
              }
            }

		        if ( listener != null ) {
		          if ( !listener.tableItemInserted( tableItem, v ) ) {
		            tableItem.dispose(); // remove it again
		          }
		        }
		      }
		    }
		    tableView.removeEmptyRows();
		    tableView.setRowNums();
		    tableView.optWidth( true );
		  }


}
