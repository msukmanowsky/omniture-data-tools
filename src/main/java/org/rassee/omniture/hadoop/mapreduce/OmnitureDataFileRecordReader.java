/*
 * MIT License
 *
 * Copyright (c) 2016 mikes
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

package org.rassee.omniture.hadoop.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.rassee.omniture.hadoop.util.EscapedLineReader;


/**
 * A record reader for splits generated by an Omniture hit_data.tsv daily data file.
 *
 * @author Mike Sukmanowsky (<a href="mailto:mike.sukmanowsky@gmail.com">mike.sukmanowsky@gmail.com</a>)
 */
public class OmnitureDataFileRecordReader extends RecordReader<LongWritable, Text> {

    private static final Log LOG = LogFactory.getLog(OmnitureDataFileRecordReader.class);

    private int maxLineLength;
    private long start;
    private long pos;
    private long end;
    private CompressionCodecFactory compressionCodecs;
    private EscapedLineReader lineReader;
    private LongWritable key;
    private Text value;
    private int numberOfFields;

    public OmnitureDataFileRecordReader(int numberOfFields) {
        this.numberOfFields = numberOfFields;
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.escapedlinereader.maxlength", Integer.MAX_VALUE);
        this.start = split.getStart();
        this.end = start + split.getLength();
        final Path file = split.getPath();
        this.compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // Open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            lineReader = new EscapedLineReader(codec.createInputStream(fileIn), job);
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            lineReader = new EscapedLineReader(fileIn, job);
        }
        if (skipFirstLine) {
            start += lineReader.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    /**
     * Returns the current line.  All instances of \\t, \\n and \\r\n are replaced by a space.
     * @return the value last read when nextKeyValue() was called.
     * @throws IOException
     * @throws InterruptedException
     */
    public Text getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public void close() throws IOException {
        if (lineReader != null) {
            lineReader.close();
        }

    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }


    @Override
    /**
     * Reads the next record in the split.
     * @throws IOException
     * @throws InterruptedException
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {
        String line;

        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);

        if (value == null) {
            value = new Text();
        }

        int bytesRead = 0;
        int countTabs = 0;
        // Stay within the split
        while (pos < end) {
            bytesRead = lineReader.readLine(value, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));

            // If we didn't read anything, then we're done
            if (bytesRead == 0) {
                break;
            }
            // Modify the line that's returned by the EscapedLineReader so that the tabs won't be an issue to split on
            // Remember that in Java a \\\\ within a string regex = one backslash
            line = value.toString().replaceAll("\\\\\t", "").replaceAll("\\\\(\n|\r|\r\n)", "");
            countTabs = StringUtils.countMatches(line, "\t");
            value.set(line);

            // Move the position marker
            pos += bytesRead;

            // Ensure that we didn't read more than we were supposed to and that we don't have a bogus line which should be skipped
            if (bytesRead < maxLineLength && countTabs == numberOfFields) {
                break;
            }

            if (numberOfFields != countTabs) {
                // TODO: Implement counters to track number of skipped lines, possibly only available via map and reduce functions
                LOG.warn("Skipped line at position " + (pos - bytesRead) + " with incorrect number of fields (expected " + numberOfFields + " but found " + countTabs + ")");
            } else {
                // Otherwise the line is too long and we need to skip this line
                LOG.warn("Skipped line of size " + bytesRead + " at position " + (pos - bytesRead));
            }
        }

        // Check to see if we actually read a line and return appropriate boolean
        if (bytesRead == 0 || countTabs != numberOfFields) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }

}
