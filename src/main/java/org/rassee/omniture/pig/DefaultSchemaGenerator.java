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

/**
 * Pig schema generator for Omniture raw data
 * Created by siyengar on 4/19/16.
 */
public class DefaultSchemaGenerator extends AbstractSchemaGenerator {
    private static final String columnHeaders = "accept_language\tbrowser\tbrowser_height\tbrowser_width\tc_color\tcampaign\tcarrier\tchannel\tclick_action\tclick_action_type\tclick_context\tclick_context_type\tclick_sourceid\tclick_tag\tcode_ver\tcolor\tconnection_type\tcookies\tcountry\tct_connect_type\tcurr_factor\tcurr_rate\tcurrency\tcust_hit_time_gmt\tcust_visid\tdaily_visitor\tdate_time\tdomain\tduplicate_events\tduplicate_purchase\tduplicated_from\tef_id\tevar1\tevar2\tevar3\tevar4\tevar5\tevar6\tevar7\tevar8\tevar9\tevar10\tevar11\tevar12\tevar13\tevar14\tevar15\tevar16\tevar17\tevar18\tevar19\tevar20\tevar21\tevar22\tevar23\tevar24\tevar25\tevar26\tevar27\tevar28\tevar29\tevar30\tevar31\tevar32\tevar33\tevar34\tevar35\tevar36\tevar37\tevar38\tevar39\tevar40\tevar41\tevar42\tevar43\tevar44\tevar45\tevar46\tevar47\tevar48\tevar49\tevar50\tevar51\tevar52\tevar53\tevar54\tevar55\tevar56\tevar57\tevar58\tevar59\tevar60\tevar61\tevar62\tevar63\tevar64\tevar65\tevar66\tevar67\tevar68\tevar69\tevar70\tevar71\tevar72\tevar73\tevar74\tevar75\tevar76\tevar77\tevar78\tevar79\tevar80\tevar81\tevar82\tevar83\tevar84\tevar85\tevar86\tevar87\tevar88\tevar89\tevar90\tevar91\tevar92\tevar93\tevar94\tevar95\tevar96\tevar97\tevar98\tevar99\tevar100\tevent_list\texclude_hit\tfirst_hit_page_url\tfirst_hit_pagename\tfirst_hit_referrer\tfirst_hit_time_gmt\tgeo_city\tgeo_country\tgeo_dma\tgeo_region\tgeo_zip\thier1\thier2\thier3\thier4\thier5\thit_source\thit_time_gmt\thitid_high\thitid_low\thomepage\thourly_visitor\tip\tip2\tj_jscript\tjava_enabled\tjavascript\tlanguage\tlast_hit_time_gmt\tlast_purchase_num\tlast_purchase_time_gmt\tmcvisid\tmobile_id\tmobileaction\tmobileappid\tmobilecampaigncontent\tmobilecampaignmedium\tmobilecampaignname\tmobilecampaignsource\tmobilecampaignterm\tmobiledayofweek\tmobiledayssincefirstuse\tmobiledayssincelastuse\tmobiledevice\tmobilehourofday\tmobileinstalldate\tmobilelaunchnumber\tmobileltv\tmobilemessageid\tmobilemessageonline\tmobileosversion\tmobileresolution\tmonthly_visitor\tmvvar1\tmvvar2\tmvvar3\tnamespace\tnew_visit\tos\tp_plugins\tpage_event\tpage_event_var1\tpage_event_var2\tpage_event_var3\tpage_type\tpage_url\tpagename\tpaid_search\tpartner_plugins\tpersistent_cookie\tplugins\tpointofinterest\tpointofinterestdistance\tpost_browser_height\tpost_browser_width\tpost_campaign\tpost_channel\tpost_cookies\tpost_currency\tpost_cust_hit_time_gmt\tpost_cust_visid\tpost_ef_id\tpost_evar1\tpost_evar2\tpost_evar3\tpost_evar4\tpost_evar5\tpost_evar6\tpost_evar7\tpost_evar8\tpost_evar9\tpost_evar10\tpost_evar11\tpost_evar12\tpost_evar13\tpost_evar14\tpost_evar15\tpost_evar16\tpost_evar17\tpost_evar18\tpost_evar19\tpost_evar20\tpost_evar21\tpost_evar22\tpost_evar23\tpost_evar24\tpost_evar25\tpost_evar26\tpost_evar27\tpost_evar28\tpost_evar29\tpost_evar30\tpost_evar31\tpost_evar32\tpost_evar33\tpost_evar34\tpost_evar35\tpost_evar36\tpost_evar37\tpost_evar38\tpost_evar39\tpost_evar40\tpost_evar41\tpost_evar42\tpost_evar43\tpost_evar44\tpost_evar45\tpost_evar46\tpost_evar47\tpost_evar48\tpost_evar49\tpost_evar50\tpost_evar51\tpost_evar52\tpost_evar53\tpost_evar54\tpost_evar55\tpost_evar56\tpost_evar57\tpost_evar58\tpost_evar59\tpost_evar60\tpost_evar61\tpost_evar62\tpost_evar63\tpost_evar64\tpost_evar65\tpost_evar66\tpost_evar67\tpost_evar68\tpost_evar69\tpost_evar70\tpost_evar71\tpost_evar72\tpost_evar73\tpost_evar74\tpost_evar75\tpost_evar76\tpost_evar77\tpost_evar78\tpost_evar79\tpost_evar80\tpost_evar81\tpost_evar82\tpost_evar83\tpost_evar84\tpost_evar85\tpost_evar86\tpost_evar87\tpost_evar88\tpost_evar89\tpost_evar90\tpost_evar91\tpost_evar92\tpost_evar93\tpost_evar94\tpost_evar95\tpost_evar96\tpost_evar97\tpost_evar98\tpost_evar99\tpost_evar100\tpost_event_list\tpost_hier1\tpost_hier2\tpost_hier3\tpost_hier4\tpost_hier5\tpost_java_enabled\tpost_keywords\tpost_mobileaction\tpost_mobileappid\tpost_mobilecampaigncontent\tpost_mobilecampaignmedium\tpost_mobilecampaignname\tpost_mobilecampaignsource\tpost_mobilecampaignterm\tpost_mobiledayofweek\tpost_mobiledayssincefirstuse\tpost_mobiledayssincelastuse\tpost_mobiledevice\tpost_mobilehourofday\tpost_mobileinstalldate\tpost_mobilelaunchnumber\tpost_mobileltv\tpost_mobilemessageid\tpost_mobilemessageonline\tpost_mobileosversion\tpost_mobileresolution\tpost_mvvar1\tpost_mvvar2\tpost_mvvar3\tpost_page_event\tpost_page_event_var1\tpost_page_event_var2\tpost_page_event_var3\tpost_page_type\tpost_page_url\tpost_pagename\tpost_pagename_no_url\tpost_partner_plugins\tpost_persistent_cookie\tpost_pointofinterest\tpost_pointofinterestdistance\tpost_product_list\tpost_prop1\tpost_prop2\tpost_prop3\tpost_prop4\tpost_prop5\tpost_prop6\tpost_prop7\tpost_prop8\tpost_prop9\tpost_prop10\tpost_prop11\tpost_prop12\tpost_prop13\tpost_prop14\tpost_prop15\tpost_prop16\tpost_prop17\tpost_prop18\tpost_prop19\tpost_prop20\tpost_prop21\tpost_prop22\tpost_prop23\tpost_prop24\tpost_prop25\tpost_prop26\tpost_prop27\tpost_prop28\tpost_prop29\tpost_prop30\tpost_prop31\tpost_prop32\tpost_prop33\tpost_prop34\tpost_prop35\tpost_prop36\tpost_prop37\tpost_prop38\tpost_prop39\tpost_prop40\tpost_prop41\tpost_prop42\tpost_prop43\tpost_prop44\tpost_prop45\tpost_prop46\tpost_prop47\tpost_prop48\tpost_prop49\tpost_prop50\tpost_prop51\tpost_prop52\tpost_prop53\tpost_prop54\tpost_prop55\tpost_prop56\tpost_prop57\tpost_prop58\tpost_prop59\tpost_prop60\tpost_prop61\tpost_prop62\tpost_prop63\tpost_prop64\tpost_prop65\tpost_prop66\tpost_prop67\tpost_prop68\tpost_prop69\tpost_prop70\tpost_prop71\tpost_prop72\tpost_prop73\tpost_prop74\tpost_prop75\tpost_purchaseid\tpost_referrer\tpost_s_kwcid\tpost_search_engine\tpost_socialaccountandappids\tpost_socialassettrackingcode\tpost_socialauthor\tpost_socialcontentprovider\tpost_socialfbstories\tpost_socialfbstorytellers\tpost_socialinteractioncount\tpost_socialinteractiontype\tpost_sociallanguage\tpost_sociallatlong\tpost_sociallikeadds\tpost_socialmentions\tpost_socialowneddefinitioninsighttype\tpost_socialowneddefinitioninsightvalue\tpost_socialowneddefinitionmetric\tpost_socialowneddefinitionpropertyvspost\tpost_socialownedpostids\tpost_socialownedpropertyid\tpost_socialownedpropertyname\tpost_socialownedpropertypropertyvsapp\tpost_socialpageviews\tpost_socialpostviews\tpost_socialpubcomments\tpost_socialpubposts\tpost_socialpubrecommends\tpost_socialpubsubscribers\tpost_socialterm\tpost_socialtotalsentiment\tpost_state\tpost_survey\tpost_t_time_info\tpost_tnt\tpost_tnt_action\tpost_transactionid\tpost_video\tpost_videoad\tpost_videoadinpod\tpost_videoadplayername\tpost_videoadpod\tpost_videochannel\tpost_videocontenttype\tpost_videopath\tpost_videoplayername\tpost_videosegment\tpost_visid_high\tpost_visid_low\tpost_visid_type\tpost_zip\tprev_page\tproduct_list\tproduct_merchandising\tprop1\tprop2\tprop3\tprop4\tprop5\tprop6\tprop7\tprop8\tprop9\tprop10\tprop11\tprop12\tprop13\tprop14\tprop15\tprop16\tprop17\tprop18\tprop19\tprop20\tprop21\tprop22\tprop23\tprop24\tprop25\tprop26\tprop27\tprop28\tprop29\tprop30\tprop31\tprop32\tprop33\tprop34\tprop35\tprop36\tprop37\tprop38\tprop39\tprop40\tprop41\tprop42\tprop43\tprop44\tprop45\tprop46\tprop47\tprop48\tprop49\tprop50\tprop51\tprop52\tprop53\tprop54\tprop55\tprop56\tprop57\tprop58\tprop59\tprop60\tprop61\tprop62\tprop63\tprop64\tprop65\tprop66\tprop67\tprop68\tprop69\tprop70\tprop71\tprop72\tprop73\tprop74\tprop75\tpurchaseid\tquarterly_visitor\tref_domain\tref_type\treferrer\tresolution\ts_kwcid\ts_resolution\tsampled_hit\tsearch_engine\tsearch_page_num\tsecondary_hit\tservice\tsocialaccountandappids\tsocialassettrackingcode\tsocialauthor\tsocialcontentprovider\tsocialfbstories\tsocialfbstorytellers\tsocialinteractioncount\tsocialinteractiontype\tsociallanguage\tsociallatlong\tsociallikeadds\tsocialmentions\tsocialowneddefinitioninsighttype\tsocialowneddefinitioninsightvalue\tsocialowneddefinitionmetric\tsocialowneddefinitionpropertyvspost\tsocialownedpostids\tsocialownedpropertyid\tsocialownedpropertyname\tsocialownedpropertypropertyvsapp\tsocialpageviews\tsocialpostviews\tsocialpubcomments\tsocialpubposts\tsocialpubrecommends\tsocialpubsubscribers\tsocialterm\tsocialtotalsentiment\tsourceid\tstate\tstats_server\tt_time_info\ttnt\ttnt_action\ttnt_post_vista\ttransactionid\ttruncated_hit\tua_color\tua_os\tua_pixels\tuser_agent\tuser_hash\tuser_server\tuserid\tusername\tva_closer_detail\tva_closer_id\tva_finder_detail\tva_finder_id\tva_instance_event\tva_new_engagement\tvideo\tvideoad\tvideoadinpod\tvideoadplayername\tvideoadpod\tvideochannel\tvideocontenttype\tvideopath\tvideoplayername\tvideosegment\tvisid_high\tvisid_low\tvisid_new\tvisid_timestamp\tvisid_type\tvisit_keywords\tvisit_num\tvisit_page_num\tvisit_referrer\tvisit_search_engine\tvisit_start_page_url\tvisit_start_pagename\tvisit_start_time_gmt\tweekly_visitor\tyearly_visitor\tzip";

    private static DefaultSchemaGenerator schemaGenerator = null;

    private DefaultSchemaGenerator() {

    }

    public static SchemaGenerator getInstance() {
        if (schemaGenerator == null) {
            schemaGenerator = new DefaultSchemaGenerator();
        }
        return schemaGenerator;
    }

    @Override
    public String getColumnHeaders() {
        return columnHeaders;
    }

    @Override
    public String getColumnHeadersDelimiter() {
        return "\t";
    }
}
