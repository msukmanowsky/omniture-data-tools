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

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.FileNotFoundException;

import static org.junit.Assert.assertEquals;

/**
 * Created by siyengar on 5/10/16.
 */
public class TestLocalFileBasedSchemaGenerator {
    @Test
    public void testSchema1() throws FileNotFoundException {
        SchemaGenerator generator = new LocalFileBasedSchemaGenerator("src/test/resources/schema1.json");
        assertEquals(generator.getColumnHeadersDelimiter(), "\t");
        assertEquals(generator.generatePigSchema().split(",").length, 554);
        assertEquals(generator.getColumnHeaders().split("\t").length, 554);

        assertEquals(StringUtils.countMatches(generator.generatePigSchema(), "long"), 60);
    }
}
