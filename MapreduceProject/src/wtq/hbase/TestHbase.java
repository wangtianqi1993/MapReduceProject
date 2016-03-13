package wtq.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
public class TestHbase {
    public static final String TABLE_NAME = "userwtq";
    public static final String FAMILY_NAME="info";
    public static final String COLUMN_NAME="age";
    public static final String  ROW_KEY="xiaoming";
   
    public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException
    {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
    conf.set("hbase.zookeeper.quorum", "localhost");
    
    HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
    createTable(hbaseAdmin);
    HTable htable = new HTable(conf,TABLE_NAME);
    putRecord(htable);
    getRecord(htable);
    scanTable(htable);
    }
    
    private static void scanTable(HTable htable) throws IOException
    {
    	System.out.println("遍历结果如下:");
    	Scan scan = new Scan();
    	ResultScanner resultScanner = htable.getScanner(scan);
    	for(Result result : resultScanner)
    	{
    		byte[] value = result.getValue(FAMILY_NAME.getBytes(), COLUMN_NAME.getBytes());
    		System.out.println(new String(value));
    	}
    }
    private static void putRecord(HTable htable) throws IOException{
    	Put put = new Put(ROW_KEY.getBytes());
    	put.add(FAMILY_NAME.getBytes(),COLUMN_NAME.getBytes(),"25".getBytes());
    	htable.put(put);
    	System.out.println("insert a record");
    }
    
    private static void getRecord(HTable htable) throws IOException
    {
    	Get get  = new Get(ROW_KEY.getBytes());
    	Result result = htable.get(get);
    	byte[] value = result.getValue(FAMILY_NAME.getBytes(), COLUMN_NAME.getBytes());
    	System.out.println("the result is" + new String(value));
    }
    
    private static void createTable (HBaseAdmin hbaseAdmin) throws IOException {
    	if(!hbaseAdmin.tableExists(TABLE_NAME)){
    		HTableDescriptor hTableDescriptor = new HTableDescriptor(TABLE_NAME);
    	    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(FAMILY_NAME);
    	    hTableDescriptor.addFamily(hColumnDescriptor);
    	    hbaseAdmin.createTable(hTableDescriptor);
    	}
    }
    private static void dropTable(HBaseAdmin hbaseAdmin) throws IOException
    {
    	if  (hbaseAdmin.tableExists(TABLE_NAME)){
    		hbaseAdmin.disableTable(TABLE_NAME);
    		hbaseAdmin.deleteTable(TABLE_NAME);
    	}
    }
} 
