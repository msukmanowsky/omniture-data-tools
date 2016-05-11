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
