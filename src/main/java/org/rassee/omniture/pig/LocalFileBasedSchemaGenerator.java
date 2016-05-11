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

    public Set<String> getLongColumns() {
        if (definition.longColumns != null) {
            return Sets.newHashSet(definition.longColumns);
        }
        return Sets.newHashSet();
    }

    public Set<String> getDecimalColumns() {
        if (definition.decimalColumns != null) {
            return Sets.newHashSet(definition.decimalColumns);
        }
        return Sets.newHashSet();
    }

    public Set<String> getTimestampColumns() {
        if (definition.timestampColumns != null) {
            return Sets.newHashSet(definition.timestampColumns);
        }
        return Sets.newHashSet();
    }

    public String getColumnHeaders() {
        return definition.columnHeaders;
    }

    public String getColumnHeadersDelimiter() {
        return definition.columnHeaderDelimiter;
    }
}
