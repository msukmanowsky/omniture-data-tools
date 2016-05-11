/*
 * Copyright $year [Satish Iyengar]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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