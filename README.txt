This project is a indexing extension for hbase. But it is not compatiable with new version of HBase.

Current status:

Now I'm working with HBase 0.98 by using coprocessor.

Working Features:

* Ability to create manage and query with pre-defined table indexes.

Installation:
 Drop the jar in the classpath of your application
 
Configuration: 
 To enable indexing use the above configuration except for the following replacements:
<property>
	<name>hbase.coprocessor.region.classes</name>
	<value>org.apache.hadoop.hbase.regionserver.tableindexed.IndexRegionObserver</value>
</property>property
  
