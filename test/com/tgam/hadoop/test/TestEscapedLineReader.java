package com.tgam.hadoop.test;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.junit.*;

import com.tgam.hadoop.util.EscapedLineReader;

import static org.junit.Assert.*;

public class TestEscapedLineReader {
	
	private EscapedLineReader reader;
	private ByteArrayInputStream in;
	
	@Before
	public void setUp() throws FileNotFoundException {
		StringBuilder b = new StringBuilder();
		b.append("1\\\r\n2\\\n3\\\r4\n");
		b.append("1\\\r\n2\\\n3\\\r4\r\n");
		b.append("1\\\r\n2\\\n3\\\r4\r");

		in = new ByteArrayInputStream(b.toString().getBytes());
		reader = new EscapedLineReader(in);
	}
	
	@Test
	public void testStuff() throws IOException {
		Text text = new Text();
		while(reader.readLine(text) > 0) {
			String line = text.toString().replaceAll("\\\\(\n|\r|\r\n)", " ");
			String[] fields = line.split(" ", -1);
			
			assertEquals(fields[0], 1);
			assertEquals(fields[1], 2);
			assertEquals(fields[2], 3);
			assertEquals(fields[3], 4);
			assertEquals(fields.length, 4);
		}
	}
}