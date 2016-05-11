
/*
 * MIT License
 *
 * Copyright (c) 2016 siyengar
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.rassee.omniture.pig;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.*;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.rassee.omniture.hadoop.mapreduce.OmnitureDataFileRecordReader;
import org.rassee.omniture.hadoop.mapreduce.OmnitureDataFileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Properties;


/**
 * A Pig custom loader for reading and parsing raw Omniture daily hit data files (hit_data.tsv).
 *
 * @author Mike Sukmanowsky (<a href="mailto:mike.sukmanowsky@gmail.com">mike.sukmanowsky@gmail.com</a>)
 */
public class OmnitureDataLoader extends LoadFunc implements LoadMetadata {
    private static Logger logger = LoggerFactory.getLogger(OmnitureDataLoader.class);

    private static final String DATE_TIME_FORMAT = "yyy-MM-dd HH:mm:ss";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern(DATE_TIME_FORMAT);
    private final String schema;
    private int fieldCount = 0;

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory bagFactory = BagFactory.getInstance();
    private OmnitureDataFileRecordReader reader;
    private String udfcSignature = null;
    private ResourceFieldSchema[] fields;

    public OmnitureDataLoader() {
        schema = DefaultSchemaGenerator.getInstance().generatePigSchema();
        fieldCount = StringUtils.countMatches(schema, ",");
    }

    public OmnitureDataLoader(String localSchemaJsonFile) throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {
        schema = new LocalFileBasedSchemaGenerator(localSchemaJsonFile).generatePigSchema();
        fieldCount = StringUtils.countMatches(schema, ",");
    }

    @Override
    public void setUDFContextSignature(String signature) {
        udfcSignature = signature;
    }

    @Override
    /**
     * Provide a new OmnitureDataFileInputFormat for RecordReading.
     * @return a new OmnitureDataFileInputFormat()
     */
    public InputFormat<LongWritable, Text> getInputFormat() throws IOException {
        return new OmnitureDataFileInputFormat(this.fieldCount);
    }

    @Override
    /**
     * Sets the location of the data file for the call to this custom loader.  This is assumed to be an HDFS path
     * and thus FileInputFormat is used.
     */
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {
        this.reader = (OmnitureDataFileRecordReader) reader;
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(this.schema));
        fields = schema.getFields();
    }

    @Override
    public Tuple getNext() throws IOException {
        Tuple tuple;
        Text value;
        Iterable<String> valueIterable;
        Iterator<String> valueIterator;
        int numberOfTabs;

        try {
            // Read the next key-value pair from the record reader.  If it's
            // finished, return null
            if (!reader.nextKeyValue()) return null;

            value = reader.getCurrentValue();

            valueIterable = Splitter.on('\t').split(value.toString());
            numberOfTabs = Iterables.size(valueIterable);
            valueIterator = valueIterable.iterator();
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }

        // Create a new Tuple optimized for the number of fields that we know we'll need
        tuple = tupleFactory.newTuple(numberOfTabs + 1);

        if (numberOfTabs != fields.length) {
            logger.error("skipping row - did not find expected tabs in row - expected {}, found {}", fields.length, numberOfTabs);
        } else {
            int fieldIndex = 0;
            while (valueIterator.hasNext()) {
                String val = valueIterator.next().trim();
                ResourceFieldSchema field = fields[fieldIndex];

                //field name starts with prop then
                //
                switch (field.getType()) {
                    case DataType.INTEGER:
                        if (StringUtils.isBlank(val)) {
                            tuple.set(fieldIndex, null);
                        } else {
                            try {
                                tuple.set(fieldIndex, Integer.parseInt(val));
                            } catch (NumberFormatException nfe1) {
                                // Throw a more descriptive message
                                throw new NumberFormatException("Error while trying to parse " + val + " into an Integer for field [fieldindex= " + fieldIndex + "] " + field.getName() + "\n" + value.toString());
                            }
                        }
                        break;
                    case DataType.DATETIME:
                        if (StringUtils.isBlank(val)) {
                            tuple.set(fieldIndex, null);
                        } else {
                            DATE_TIME_FORMATTER.parseDateTime(val);
                        }
                        break;
                    case DataType.CHARARRAY:
                        tuple.set(fieldIndex, val);
                        break;
                    case DataType.LONG:
                        if (StringUtils.isBlank(val)) {
                            tuple.set(fieldIndex, null);
                        } else {
                            try {
                                tuple.set(fieldIndex, Long.parseLong(val));
                            } catch (NumberFormatException nfe2) {
                                throw new NumberFormatException("Error while trying to parse " + val + " into a Long for field " + field.getName() + "\n" + value.toString());
                            }
                        }
                        break;
                    case DataType.BIGDECIMAL:
                        if (StringUtils.isBlank(val)) {
                            tuple.set(fieldIndex, null);
                        } else {
                            try {
                                tuple.set(fieldIndex, new BigDecimal(val));
                            } catch (NumberFormatException nfe2) {
                                throw new NumberFormatException("Error while trying to parse " + val + " into a BigDecimal for field " + field.getName() + "\n" + value.toString());
                            }
                        }
                        break;
                    case DataType.BAG:
                        if (field.getName().equals("event_list")) {
                            DataBag bag = bagFactory.newDefaultBag();
                            String[] events = val.split(",");

                            if (events == null) {
                                tuple.set(fieldIndex, null);
                            } else {
                                for (int j = 0; j < events.length; j++) {
                                    Tuple t = tupleFactory.newTuple(1);
                                    if (events[j] == "") {
                                        t.set(0, null);
                                    } else {
                                        t.set(0, events[j]);
                                    }
                                    bag.add(t);
                                }
                                tuple.set(fieldIndex, bag);
                            }
                        } else {
                            throw new IOException("Can not process bags for the field " + field.getName() + ". Can only process for the event_list field.");
                        }
                        break;
                    default:
                        throw new IOException("Unexpected or unknown type in input schema (Omniture fields should be int, chararray or long): " + field.getType());
                }

                fieldIndex++;
            }
        }

        return tuple;
    }

    public ResourceSchema getSchema(String location, Job job) throws IOException {
        // The schema for hit_data.tsv won't change for quite sometime and when it does, this class should be updated

        ResourceSchema s = new ResourceSchema(Utils.getSchemaFromString(schema));

        // Store the schema to our UDF context on the backend (is this really necessary considering it's private static final?)
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p = udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        p.setProperty("pig.omnituretextloader.schema", schema);

        return s;
    }

    /**
     * Not currently used, but could later on be used to partition based on hit_time_gmt perhaps.
     */
    public String[] getPartitionKeys(String location, Job job) throws IOException {
        // TODO: Build out partition keys based on hit_time_gmt
        return null;
    }

    /**
     * Not used in this class.
     *
     * @return null
     */
    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        return null;
    }

    /**
     * Not currently used, but could later on be used to partition based on hit_time_gmt perhaps.
     */
    public void setPartitionFilter(Expression arg0) throws IOException {
        // TODO: Build out partition keys based on hit_time_gmt

    }
}