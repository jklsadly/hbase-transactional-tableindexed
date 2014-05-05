package org.apache.hadoop.hbase.regionserver.tableindexed;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.tableindexed.IndexSpecification;
import org.apache.hadoop.hbase.client.tableindexed.IndexedTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Index Coprocessor
 * update the index table when user exec insert,update,detete on the indexed data
 * 旧版本的的HRegionInterface舍弃掉了，因此此处使用Coprocessor来实现这个功能
 * @author wudi 2014-05-05
 *
 */
public class IndexRegionObserver extends BaseRegionObserver{

	private static final Log LOG = LogFactory.getLog(IndexRegionObserver.class);
	
	private static final HTablePool tablePool= new HTablePool();
	private SortedMap<byte[], byte[]> oldColumnValues = null;	

	private HTableInterface getIndexTable(final IndexSpecification index,ObserverContext<RegionCoprocessorEnvironment> c)
		      throws IOException {
		LOG.debug("put_table_name="+c.getEnvironment().getRegion().getRegionInfo()
		        .getTableName());
		LOG.debug("index_table_name="+index.getIndexedTableName(c.getEnvironment().getRegion().getRegionInfo()
		        .getTableName()));
		return tablePool.getTable(index.getIndexedTableName(c.getEnvironment().getRegion().getRegionInfo()
		        .getTableName()));
	}
	

	private void putTable(final HTableInterface t) throws IOException {
		if (t == null) {
		      return;
		}
		tablePool.putTable(t);//MYC: ADD try catch
	}

	private Collection<IndexSpecification> getIndexes(IndexedTableDescriptor indexTableDescriptor) {
		return indexTableDescriptor.getIndexes();
	}
	
	private void updateIndexes(final Put put,ObserverContext<RegionCoprocessorEnvironment> c)
		      throws IOException {
		List<IndexSpecification> indexesToUpdate = new LinkedList<IndexSpecification>();
		//added by wudi 2014-05-05
		IndexedTableDescriptor indexTableDescriptor = 
				new IndexedTableDescriptor(c.getEnvironment().getRegion().getTableDesc());
		// Find the indexes we need to update
		for (IndexSpecification index : getIndexes(indexTableDescriptor)) {
			if (possiblyAppliesToIndex(index, put)) {
				indexesToUpdate.add(index);
			}
		}
		LOG.debug("table_name="+c.getEnvironment().getRegion().getTableDesc().getTableName());
		LOG.debug("index_size="+indexesToUpdate.size());
		if (indexesToUpdate.size() == 0) {
		   return;
		}

		NavigableSet<byte[]> neededColumns = getColumnsForIndexes(indexesToUpdate);
		NavigableMap<byte[], byte[]> newColumnValues = getColumnsFromPut(put);

	    Get oldGet = new Get(put.getRow());
	    for (byte[] neededCol : neededColumns) {
//		      oldGet.addColumn(neededCol);
	    	 byte [][] fq = KeyValue.parseColumn(neededCol);
	         if (fq.length > 1 && fq[1] != null && fq[1].length > 0) {
	             oldGet.addColumn(fq[0], fq[1]);
	          } else {
	             oldGet.addFamily(fq[0]);
	          }
	    }

	    Result oldResult = c.getEnvironment().getRegion().get(oldGet);
	    LOG.debug("oldResult="+oldResult.size());
	    // Add the old values to the new if they are not there
	    if (oldResult != null && oldResult.raw() != null) {
	      for (KeyValue oldKV : oldResult.raw()) {
	        byte[] column = KeyValue.makeColumn(oldKV.getFamily(),
	            oldKV.getQualifier());
	        if (!newColumnValues.containsKey(column)) {
	          newColumnValues.put(column, oldKV.getValue());
	        }
	      }
	    }else{
	    	LOG.debug("oldResult=NULL");
	    }

	    Iterator<IndexSpecification> indexIterator = indexesToUpdate.iterator();
	    while (indexIterator.hasNext()) {
	      IndexSpecification indexSpec = indexIterator.next();
	      if (!IndexMaintenanceUtils.doesApplyToIndex(indexSpec, newColumnValues)) {
	        indexIterator.remove();
	      }
	    }

	    SortedMap<byte[], byte[]> oldColumnValues = convertToValueMap(oldResult);

	    for (IndexSpecification indexSpec : indexesToUpdate) {
	      updateIndex(indexSpec, put, newColumnValues, oldColumnValues,c);
	    }
	  }

	  // FIXME: This call takes place in an RPC, and requires an RPC. This makes for
	  // a likely deadlock if the number of RPCs we are trying to serve is >= the
	  // number of handler threads.
	  private void updateIndex(final IndexSpecification indexSpec, final Put put,
	      final NavigableMap<byte[], byte[]> newColumnValues,
	      final SortedMap<byte[], byte[]> oldColumnValues,
	      ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
	    Delete indexDelete = makeDeleteToRemoveOldIndexEntry(indexSpec,
	        put.getRow(), oldColumnValues);
	    Put indexPut = makeIndexUpdate(indexSpec, put.getRow(), newColumnValues);

	    HTableInterface indexTable = getIndexTable(indexSpec,c);
	    try {
	      if (indexDelete != null
	          && !Bytes.equals(indexDelete.getRow(), indexPut.getRow())) {
	        // Only do the delete if the row changed. This way we save the put after
	        // delete issues in HBASE-2256
	        LOG.debug("Deleting old index row ["
	            + Bytes.toString(indexDelete.getRow()) + "]. New row is ["
	            + Bytes.toString(indexPut.getRow()) + "].");
	        indexTable.delete(indexDelete);
	      } else if (indexDelete != null) {
	        LOG.debug("Skipping deleting index row ["
	            + Bytes.toString(indexDelete.getRow())
	            + "] because it has not changed.");
	      }
	      indexTable.put(indexPut);
	    } finally {
	      putTable(indexTable);
	    }
	  }

	  /** Return the columns needed for the update. */
	  private NavigableSet<byte[]> getColumnsForIndexes(
	      final Collection<IndexSpecification> indexes) {
	    NavigableSet<byte[]> neededColumns = new TreeSet<byte[]>(
	        Bytes.BYTES_COMPARATOR);
	    for (IndexSpecification indexSpec : indexes) {
	      for (byte[] col : indexSpec.getAllColumns()) {
	        neededColumns.add(col);
	      }
	    }
	    return neededColumns;
	  }

	  private Delete makeDeleteToRemoveOldIndexEntry(
	      final IndexSpecification indexSpec, final byte[] row,
	      final SortedMap<byte[], byte[]> oldColumnValues) throws IOException {
	    for (byte[] indexedCol : indexSpec.getIndexedColumns()) {
	      if (!oldColumnValues.containsKey(indexedCol)) {
	        LOG.debug("Index [" + indexSpec.getIndexId()
	            + "] not trying to remove old entry for row ["
	            + Bytes.toString(row) + "] because col ["
	            + Bytes.toString(indexedCol) + "] is missing");
	        return null;
	      }
	    }

	    byte[] oldIndexRow = indexSpec.getKeyGenerator().createIndexKey(row,
	        oldColumnValues);
	    LOG.debug("Index [" + indexSpec.getIndexId() + "] removing old entry ["
	        + Bytes.toString(oldIndexRow) + "]");
	    return new Delete(oldIndexRow);
	  }

	  private NavigableMap<byte[], byte[]> getColumnsFromPut(final Put put) {
	    NavigableMap<byte[], byte[]> columnValues = new TreeMap<byte[], byte[]>(
	        Bytes.BYTES_COMPARATOR);
	    for (List<KeyValue> familyPuts : put.getFamilyMap().values()) {
	      for (KeyValue kv : familyPuts) {
	        columnValues.put(
	            KeyValue.makeColumn(kv.getFamily(), kv.getQualifier()),
	            kv.getValue());
	      }
	    }
	    return columnValues;
	  }

	  /**
	   * Ask if this put *could* apply to the index. It may actually apply if some
	   * of the columns needed are missing.
	   * 
	   * @param indexSpec
	   * @param put
	   * @return true if possibly apply.
	   */
	  private boolean possiblyAppliesToIndex(final IndexSpecification indexSpec,
	      final Put put) {
	    for (List<KeyValue> familyPuts : put.getFamilyMap().values()) {
	      for (KeyValue kv : familyPuts) {
	        if (indexSpec.containsColumn(KeyValue.makeColumn(kv.getFamily(),
	            kv.getQualifier()))) {
	          return true;
	        }
	      }
	    }
	    return false;
	  }

	  private Put makeIndexUpdate(final IndexSpecification indexSpec,
	      final byte[] row, final SortedMap<byte[], byte[]> columnValues)
	      throws IOException {
	    Put indexUpdate = IndexMaintenanceUtils.createIndexUpdate(indexSpec, row,
	        columnValues);
	    LOG.debug("Index [" + indexSpec.getIndexId() + "] adding new entry ["
	        + Bytes.toString(indexUpdate.getRow()) + "] for row ["
	        + Bytes.toString(row) + "]");

	    return indexUpdate;

	  }
	  
	  private SortedMap<byte[], byte[]> convertToValueMap(final Result result) {
	    SortedMap<byte[], byte[]> currentColumnValues = new TreeMap<byte[], byte[]>(
	        Bytes.BYTES_COMPARATOR);

	    if (result == null || result.raw() == null) {
	      return currentColumnValues;
	    }
	    List<KeyValue> list = result.list();
	    if (list != null) {
	      for (KeyValue kv : result.list()) {
	        currentColumnValues.put(
	            KeyValue.makeColumn(kv.getFamily(), kv.getQualifier()),
	            kv.getValue());
	      }
	    }
	    return currentColumnValues;
	  }
	
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, 
			Put put, WALEdit edit, Durability durability){
		LOG.debug("prePut");		
		try {
			LOG.debug("update index----start");
			updateIndexes(put,c);
			LOG.debug("update index----stop");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.debug(e.getStackTrace().toString());
		}
	}
	
	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, 
			Put put, WALEdit edit, Durability durability){		
		LOG.debug("postPut");
	}
	
	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, 
			Delete delete, WALEdit edit, Durability durability){		
		LOG.debug("preDelete");
		// First look at the current (to be the old) state.
	    //SortedMap<byte[], byte[]> oldColumnValues = null;
	    //added by wudi 	    
		try {
			IndexedTableDescriptor indexTableDescriptor = 
					new IndexedTableDescriptor(c.getEnvironment().getRegion().getTableDesc());
		    
		    if (!getIndexes(indexTableDescriptor).isEmpty()) {
		      // Need all columns
		      NavigableSet<byte[]> neededColumns = getColumnsForIndexes(getIndexes(indexTableDescriptor));
		      LOG.debug("needColumns="+neededColumns.size());
		      Get get = new Get(delete.getRow());
		      for (byte[] col : neededColumns) {
//		        get.addColumn(col);
		    	  byte [][] fq = KeyValue.parseColumn(col);
		          if (fq.length > 1 && fq[1] != null && fq[1].length > 0) {
		              get.addColumn(fq[0], fq[1]);
		           } else {
		              get.addFamily(fq[0]);
		           }
		      }

		      Result oldRow;
		      oldRow = c.getEnvironment().getRegion().get(get);
		      LOG.debug("oldRow="+oldRow.size());
		      oldColumnValues = convertToValueMap(oldRow);
		      LOG.debug("oldColunmValues="+oldColumnValues.size());
		      LOG.debug("oldColunmValues is"+oldColumnValues.toString());
		    }else{
		    	LOG.debug("Index=Empty");
		    }
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	      	     	    
	}
	
	@Override
	public void postDelete(ObserverContext<RegionCoprocessorEnvironment> c, 
			Delete delete, WALEdit edit, Durability durability){		
		LOG.debug("postDelete");
		IndexedTableDescriptor indexTableDescriptor;
		try {
			indexTableDescriptor = 
					new IndexedTableDescriptor( c.getEnvironment().getRegion().getTableDesc());
			if (!getIndexes(indexTableDescriptor).isEmpty()) {
			      Get get = new Get(delete.getRow());
			      LOG.debug(Bytes.toString(delete.getRow()));
			      LOG.debug("post oldColunmValues="+oldColumnValues.size());
			      LOG.debug("post oldColunmValues is"+oldColumnValues.toString());
			      // Rebuild index if there is still a version visible.
			      Result currentRow = c.getEnvironment().getRegion().get(get);
			      SortedMap<byte[], byte[]> currentColumnValues = convertToValueMap(currentRow);
			      LOG.debug("post currentColumnValues is"+currentColumnValues.toString());
			      for (IndexSpecification indexSpec : getIndexes(indexTableDescriptor)) {
			        Delete indexDelete = null;
			        if (IndexMaintenanceUtils.doesApplyToIndex(indexSpec, oldColumnValues)) {
			          indexDelete = makeDeleteToRemoveOldIndexEntry(indexSpec,
			              delete.getRow(), oldColumnValues);
			        }
			        Put indexPut = null;
			        if (IndexMaintenanceUtils.doesApplyToIndex(indexSpec,
			            currentColumnValues)) {
			          indexPut = makeIndexUpdate(indexSpec, delete.getRow(),
			              currentColumnValues);
			        }
			        
			        if (indexPut == null && indexDelete == null) {
			          LOG.debug("indexDelete and indexPut is NULL");				     
			          continue;
			        }

			        HTableInterface indexTable = getIndexTable(indexSpec,c);
			        LOG.debug("indexTable="+Bytes.toString(indexTable.getTableName()));
			        try {
			          if (indexDelete != null
			              && (indexPut == null || !Bytes.equals(indexDelete.getRow(),
			                  indexPut.getRow()))) {
			            // Only do the delete if the row changed. This way we save the put
			            // after delete issues in HBASE-2256
			            LOG.debug("Deleting old index row ["
			                + Bytes.toString(indexDelete.getRow()) + "].");
			            indexTable.delete(indexDelete);
			          } else if (indexDelete != null) {
			            LOG.debug("Skipping deleting index row ["
			                + Bytes.toString(indexDelete.getRow())
			                + "] because it has not changed.");

			            for (byte[] indexCol : indexSpec.getAdditionalColumns()) {
			              byte[][] parsed = KeyValue.parseColumn(indexCol);
			              List<KeyValue> famDeletes = delete.getFamilyMap().get(parsed[0]);
			              if (famDeletes != null) {
			                for (KeyValue kv : famDeletes) {
			                  if (Bytes.equals(indexCol,
			                      KeyValue.makeColumn(kv.getFamily(), kv.getQualifier()))) {
			                    LOG.debug("Need to delete this specific column: "
			                        + Bytes.toString(KeyValue.makeColumn(kv.getFamily(),
			                            kv.getQualifier())));
			                    Delete columnDelete = new Delete(indexDelete.getRow());
			                    columnDelete.deleteColumns(kv.getFamily(),
			                        kv.getQualifier());
			                    indexTable.delete(columnDelete);
			                  }
			                }

			              }
			            }
			          }

			          if (indexPut != null) {
			            indexTable.put(indexPut);
			          }
			        } finally {			        	
			          putTable(indexTable);
			        }
			      }
			    }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
