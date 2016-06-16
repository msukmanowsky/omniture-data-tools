
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
    private static final Set<String> timestampColumns = Sets.newHashSet("date_time");
    private static final Set<String> longColumns = Sets.newHashSet("browser_height", "browser_width", "search_engine",
            "post_browser_height", "post_browser_width", "browser", "color", "country", "language", "resolution", "visit_search_engine", "geo_dma", "post_search_engine", "search_page_num",
            "click_action_type", "click_context_type", "click_sourceid", "connection_type", "curr_factor",
            "cust_hit_time_gmt", "daily_visitor", "duplicate_purchase", "exclude_hit", "first_hit_time_gmt", "hit_source",
            "hit_time_gmt", "hourly_visitor", "javascript", "language", "last_hit_time_gmt", "last_purchase_num",
            "last_purchase_time_gmt", "mobile_id", "monthly_visitor", "new_visit", "page_event", "paid_search", "post_page_event",
            "post_browser_height", "post_browser_width", "post_cust_hit_time_gmt",
            "post_visid_type", "prev_page", "quarterly_visitor", "ref_type", "secondary_hit",
            "sourceid", "userid", "va_closer_id", "va_finder_id", "va_instance_event", "va_new_engagement",
            "visid_timestamp", "visid_type", "visit_num", "visit_page_num", "visit_start_time_gmt", "weekly_visitor", "yearly_visitor", "os", "user_hash");
    private static final Set<String> decimalColumns = Sets.newHashSet();
    private static final Set<String> bagOfStringColumns = Sets.newHashSet();

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
