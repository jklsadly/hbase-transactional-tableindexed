This project is a transactional and indexing extension for hbase. 

Current status:

Recently updated to work the HBase 0.89.0-SNAPSHOT trunk. Most tests pass but until HBase trunk stabilizes no guarantee that this project is stable either.

Working Features:

* Ability to create manage and query with pre-defined table indexes.
* Ability to perform multiple HBase operations within serialized and atomic JTA transactions.

Known limitations:
* Currently combining non-transactional puts and transactional operations on the same table may have undesirable side-effects. We still need to test and account for this.
* Doing a transactional Delete.addColumn() behaves like a Delete.addColumns() so it isn't possible to just delete the latest version of a cell.
* Transactional support for batch operations (multi(MultiAction)) not implemented.
* We need better documentation!

Installation:
 Drop the jar in the classpath of your application
 
Configuration: 
To enable the extension in hbase-site.xml: 

<property>
    <name>hbase.regionserver.class</name>
    <value>org.apache.hadoop.hbase.ipc.TransactionalRegionInterface</value>
</property>
<property>
    <name>hbase.regionserver.impl</name>
    <value>org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegionServer</value>
</property> 
<property>
    <name>hbase.hregion.impl</name>
    <value>org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegion</value>
</property> 
<property>
    <name>hbase.hlog.splitter.impl</name>
    <value>org.apache.hadoop.hbase.regionserver.transactional.THLogSplitter</value>
</property>
 
 
 To further enable indexing use the above configuration except for the following replacements:
 <property>
    <name>hbase.regionserver.class</name>
    <value>org.apache.hadoop.hbase.ipc.IndexedRegionInterface</value>
</property>
<property>
    <name>hbase.regionserver.impl</name>
    <value>org.apache.hadoop.hbase.regionserver.tableindexed.IndexedRegionServer</value>
</property>
<property>
    <name>hbase.hregion.impl</name>
    <value>org.apache.hadoop.hbase.regionserver.tableindexed.IndexedRegion</value>
</property> 
 
  
 Also, currently you have to manually create the GLOBAL_TRX_LOG table with HBaseBackedTransactionLogger.createTable() before you start using any transactions.
 
 For more details, looks at the package.html in the appropriate client package of the source. 
