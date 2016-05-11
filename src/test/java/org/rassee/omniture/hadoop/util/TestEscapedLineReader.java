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

package org.rassee.omniture.hadoop.util;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.junit.*;

import static org.junit.Assert.*;

public class TestEscapedLineReader {

    private EscapedLineReader reader;
    private ByteArrayInputStream in;

    private EscapedLineReader reader1;
    private ByteArrayInputStream in1;

    @Before
    public void setUp() throws FileNotFoundException {
        StringBuilder b = new StringBuilder();
        b.append("1\\\r\\\n,2\\\n,3\\\r,4\n");
        b.append("1\\\r\\\n,2\\\n,3\\\r,4\r\n");
        b.append("1\\\r\\\n,2\\\n,3\\\r\\\t,4\r");

        in = new ByteArrayInputStream(b.toString().getBytes());
        reader = new EscapedLineReader(in);

        StringBuilder b1 = new StringBuilder();
        b1.append("1\\\r\\\n\t2\\\n\t3\\\r\t4\n");
        b1.append("1\\\r\\\n\t2\\\n\t3\\\r\t4\r\n");
        b1.append("1\\\r\\\n\t2\\\n\t3\\\r\\\t\t4\r");

        in1 = new ByteArrayInputStream(b1.toString().getBytes());
        reader1 = new EscapedLineReader(in1);

    }

    @Test
    public void testStuff() throws IOException {
        Text text = new Text();
        while (reader.readLine(text) > 0) {
            String line = text.toString().replaceAll("\\\\\t", "").replaceAll("\\\\(\n|\r|\r\n)", "");
            String[] fields = line.split(",", -1);
            System.out.println(line);
            assertEquals(fields[0], "1");
            assertEquals(fields[1], "2");
            assertEquals(fields[2], "3");
            assertEquals(fields[3], "4");
        }
    }

    @Test
    public void testStuff1() throws IOException {
        Text text = new Text();
        while (reader1.readLine(text) > 0) {
            String line = text.toString().replaceAll("\\\\\t", "").replaceAll("\\\\(\n|\r|\r\n)", "");
            String[] fields = line.split("\t", -1);
            System.out.println(line);
            assertEquals(fields[0], "1");
            assertEquals(fields[1], "2");
            assertEquals(fields[2], "3");
            assertEquals(fields[3], "4");
        }
    }


}