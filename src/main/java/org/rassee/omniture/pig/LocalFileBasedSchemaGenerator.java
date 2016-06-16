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

import com.google.common.collect.Sets;
import com.google.gson.Gson;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Pig schema generator for Omniture raw data
 * Created by siyengar on 4/19/16.
 */
public class LocalFileBasedSchemaGenerator extends AbstractSchemaGenerator {

    private final SchemaDefinition definition;

    public LocalFileBasedSchemaGenerator(String schemaDefinitionFile) throws FileNotFoundException {
        Gson gson = new Gson();
        definition = gson.fromJson(new FileReader(schemaDefinitionFile), SchemaDefinition.class);
        checkArgument(definition != null, "Invalid Schema definition. Schema definition must follow SchemaDefinition.java");
        checkArgument(definition.columnHeaderDelimiter != null, "Invalid Delimiter for column headers");
        checkArgument(definition.columnHeaders != null, "Invalid Column Headers");
    }

    @Override
    public Set<String> getLongColumns() {
        if (definition.longColumns != null) {
            return Sets.newHashSet(definition.longColumns);
        }
        return Sets.newHashSet();
    }

    @Override
    public Set<String> getDecimalColumns() {
        if (definition.decimalColumns != null) {
            return Sets.newHashSet(definition.decimalColumns);
        }
        return Sets.newHashSet();
    }

    @Override
    public Set<String> getTimestampColumns() {
        if (definition.timestampColumns != null) {
            return Sets.newHashSet(definition.timestampColumns);
        }
        return Sets.newHashSet();
    }

    @Override
    public String getColumnHeaders() {
        return definition.columnHeaders;
    }

    @Override
    public String getColumnHeadersDelimiter() {
        return definition.columnHeaderDelimiter;
    }
}
