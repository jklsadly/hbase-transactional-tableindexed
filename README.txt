This project is a transactional and indexing extension for hbase. 

Current status:

Recently updated to work the HBase 0.89.0-SNAPSHOT trunk. Most tests pass but until HBase trunk stabilizes no guarantee that this project is stable either.

Working Features:

* Ability to create manage and query with pre-defined table indexes.
* Ability to perform multiple HBase operations with ACID transactions.

Known limitations:
* Currently combining non-transactional puts and transactional operations on the same table may have undesirable side-effects. We still need to test and account for this.
* Doing a transactional Delete.addColumn() behaves like a Delete.addColumns() so it isn't possible to just delete the latest version of a cell.
* We need better documentation!

Installation:
 Drop the jar in the classpath of your application
 
Configuration: 
 One of the two regionserver extensions must be turned on by setting the appropriate configuration
 (in hbase-site.xml). 
 
 To enable the indexing extension: set 
 'hbase.regionserver.class' to 'org.apache.hadoop.hbase.ipc.IndexedRegionInterface' 
 and 
 'hbase.regionserver.impl' to 'org.apache.hadoop.hbase.regionserver.tableindexed.IndexedRegionServer'
 
 To enable the transactional extension (which includes the indexing): set 
 'hbase.regionserver.class' to 'org.apache.hadoop.hbase.ipc.TransactionalRegionInterface' 
  and
 'hbase.regionserver.impl' to 'org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegionServer'
and
 'hbase.hlog.splitter.impl' to 'org.apache.hadoop.hbase.regionserver.transactional.THLogSplitter'
 
 Currently you have to manually create the GLOBAL_TRX_LOG table with HBaseBackedTransactionLogger.createTable() before you start using any transactions.
 
 For more details, looks at the package.html in the appropriate client package of the source. 
