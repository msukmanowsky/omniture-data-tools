# OmnitureDataFileInputFormat

## what is it
A Hadoop input format for Omniture daily data files (hit_data.tsv).  Works identically to TextInputFormat except for the fact that it uses a EscapedLineReader which gets around Omniture's pesky escaped tabs and newlines.

It supports both the older style `InputFormat` API `org.apache.hadoop.mapred` as well as the newer `org.apache.hadoop.mapreduce` API which allows compatibility with hive 0.7.0 which doesn't suppor the newer `InputFormat` API.

## keys and values

* Keys are `LongWritable` and represent the byte offset of the record in the file.
* Values are `Text` with all instances of \\t, \\n or \\r\n replaced with a space (so that hive/pig can correctly split the columns)

## to build
Include
  
* hadoop-0.20.2-core.jar
* commons-logging-1.0.4.jar

in your classpath (sample provided in this project, but you'll likely need to change the path) and then create your JARs and make them available to your Hadoop cluster.

## usage in hive
To use `OmnitureDataFileInputFormat` in Hive, simply specify it during your create statement.

    CREATE TABLE (...)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
    STORED AS INPUTFORMAT 'com.tgam.hadoop.OmnitureDataFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat';

## usage in pig
To use `OmnitureDataFileInputFormat` in a Pig script, you will have to register both the InputFormat as well as the UDF [OmnitureTextLoader](http://github.com/msukmanowsky/OmnitureTextLoader).

    REGISTER path/to/jar/OmnitureDataInputFileFormat.jar
    REGISTER path/to/jar/OmnitureTextLoader.jar
    ...