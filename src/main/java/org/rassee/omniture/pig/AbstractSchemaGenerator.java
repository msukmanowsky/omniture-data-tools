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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Set;

/**
 * Created by siyengar on 5/10/16.
 */
public abstract class AbstractSchemaGenerator implements SchemaGenerator {
    //A simple Set based lookup that simply defines columns for each data types
    //It is important to use right data type if we know for sure. This offers efficient storage of output in parquet or orc format
    private final static Set<String> timestampColumns = Sets.newHashSet("date_time");
    private final static Set<String> longColumns = Sets.newHashSet("browser_height", "browser_width", "search_engine",
            "post_browser_height", "post_browser_width", "browser", "color", "country", "language", "resolution", "visit_search_engine", "geo_dma", "post_search_engine", "search_page_num",
            "click_action_type", "click_context_type", "click_sourceid", "connection_type", "curr_factor",
            "cust_hit_time_gmt", "daily_visitor", "duplicate_purchase", "exclude_hit", "first_hit_time_gmt", "hit_source",
            "hit_time_gmt", "hourly_visitor", "javascript", "language", "last_hit_time_gmt", "last_purchase_num",
            "last_purchase_time_gmt", "mobile_id", "monthly_visitor", "new_visit", "page_event", "paid_search", "post_page_event",
            "post_browser_height", "post_browser_width", "post_cust_hit_time_gmt",
            "post_visid_type", "prev_page", "quarterly_visitor", "ref_type", "secondary_hit",
            "sourceid", "userid", "va_closer_id", "va_finder_id", "va_instance_event", "va_new_engagement",
            "visid_timestamp", "visid_type", "visit_num", "visit_page_num", "visit_start_time_gmt", "weekly_visitor", "yearly_visitor", "os", "user_hash");
    private final static Set<String> decimalColumns = Sets.newHashSet();
    private final static Set<String> bagOfStringColumns = Sets.newHashSet();

    public Set<String> getLongColumns() {
        return longColumns;
    }

    public Set<String> getDecimalColumns() {
        return decimalColumns;
    }

    public Set<String> getTimestampColumns() {
        return timestampColumns;
    }

    public String generatePigSchema() {
        String[] schemaArray = getColumnHeaders().split(getColumnHeadersDelimiter());
        List<String> finalSchema = Lists.newArrayList();
        for (String s : schemaArray) {
            String colName = s.trim().toLowerCase();

            if (bagOfStringColumns.contains(colName)) {
                finalSchema.add(String.format("%s:bag{t:(v:chararray)}", colName));
            } else if (getTimestampColumns().contains(colName)) {
                finalSchema.add(String.format("%s:datetime", colName));
            } else if (getDecimalColumns().contains(colName)) {
                finalSchema.add(String.format("%s:bigdecimal", colName));
            } else if (getLongColumns().contains(colName)) {
                finalSchema.add(String.format("%s:long", colName));
            } else {
                finalSchema.add(String.format("%s:chararray", colName));
            }
        }

        //Generate schema for pig
        return StringUtils.join(finalSchema, ",");
    }


    public abstract String getColumnHeaders();

    public abstract String getColumnHeadersDelimiter();
}
