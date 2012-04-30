package com.tgam.hadoop.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * A custom input format for dealing with Omniture's hit_data.tsv daily data feed files.
 * @author Mike Sukmanowsky (<a href="mailto:mike.sukmanowsky@gmail.com">mike.sukmanowsky@gmail.com</a>)
 *
 */
public class OmnitureDataFileInputFormat extends TextInputFormat {
	
	@Override
	public OmnitureDataFileRecordReader createRecordReader(InputSplit split, TaskAttemptContext tac) {
		return new OmnitureDataFileRecordReader();
	}
	
	@Override
	public boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		return codec == null;
	}
}
