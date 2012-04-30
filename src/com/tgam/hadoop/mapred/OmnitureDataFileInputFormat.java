package com.tgam.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * A custom input format for dealing with Omniture's hit_data.tsv daily data feed files.
 * @author Mike Sukmanowsky (<a href="mailto:mike.sukmanowsky@gmail.com">mike.sukmanowsky@gmail.com</a>)
 * 
 */
class OmnitureDataFileInputFormat extends TextInputFormat {
	
	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf job, Reporter reporter) throws IOException {
		return new OmnitureDataFileRecordReader((FileSplit)inputSplit, job);
	}
}