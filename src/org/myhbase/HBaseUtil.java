package org.myhbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * a simple implemention of CRUD of the HBase,please import the essential libs of hbase before use.
 * @author Xiao Qingsong
 *
 */
public class HBaseUtil {

	private Configuration conf = null;

	public HBaseUtil() {
		conf = HBaseConfiguration.create();
		conf.set("hbase.master", "myhadoop:60000");
		conf.set("hbase.zookeeper.quorum", "myhadoop");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

	}

	/**
	 * create a table 
	 * @param tableName the table name 
	 * @param familys   the family name
	 * @throws Exception
	 */
	public void createTable(String tableName, String[] familys) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("table already exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("create table " + tableName + " ok.");
		}
	}

	/**
	 * delete a table 
	 * @param tableName the table name
	 * @throws Exception
	 */
	public void deleteTable(String tableName) throws Exception {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("delete table " + tableName + " ok.");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * add a record to the hbase
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier the column name
	 * @param value the value
	 * @throws Exception
	 */
	public void addRecord(String tableName, String rowKey, String family,
			String qualifier, String value) throws Exception {
		try {
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(family.getBytes(), qualifier.getBytes(),
					value.getBytes());
			table.put(put);
			System.out.println("insert recored " + rowKey + " to table "
					+ tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * delete a record 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @throws IOException
	 */
	public void delRecord(String tableName, String rowKey,String family,String qualifier) throws IOException {
		HTable table = new HTable(conf, tableName);
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(rowKey.getBytes());
		del.deleteColumn(family.getBytes(), qualifier.getBytes());
		list.add(del);
		table.delete(list); 
		System.out.println("del recored " + rowKey + " ok.");
	}

	/**
	 * query a record according to the given rowkey
	 * @param tableName
	 * @param rowKey
	 * @throws IOException
	 */
	public Result getOneRecord(String tableName, String rowKey)
			throws IOException {
		HTable table = new HTable(conf, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get); 
		return rs;
	}

	/**
	 * query all records of a table
	 * @param tableName
	 */
	public ResultScanner getAllRecord(String tableName) {
		try {
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s); 
			return ss;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

 
}