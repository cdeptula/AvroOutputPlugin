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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.text.DateFormatSymbols;
import java.text.SimpleDateFormat;

/**
 * @author Inquidia Consulting
 */
public class AvroOutputData extends BaseStepData implements StepDataInterface {
  public int splitnr;

  public int[] fieldnrs;

  public SimpleDateFormat daf;
  public DateFormatSymbols dafs;

  public OutputStream writer;
  
  public OutputStream schemaWriter;

  public SimpleDateFormat defaultDateFormat;
  public DateFormatSymbols defaultDateFormatSymbols;

  public RowMetaInterface outputRowMeta;

  public Schema avroSchema;

  public DatumWriter<GenericRecord> datumWriter;

  public DataFileWriter<GenericRecord> dataFileWriter;

  public EncoderFactory encoderFactory;

  public BinaryEncoder binaryEncoder;

  public JsonEncoder jsonEncoder;

  public ByteArrayOutputStream byteArrayOutputStream;

  public AvroOutputData() {
    super();

     daf = new SimpleDateFormat();
    dafs = new DateFormatSymbols();

    defaultDateFormat = new SimpleDateFormat();
    defaultDateFormatSymbols = new DateFormatSymbols();

  }
}
