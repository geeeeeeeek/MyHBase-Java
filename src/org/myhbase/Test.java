package org.myhbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

public class Test {

	public static void main(String args[]){
		String tableName = "scores";
		String[] familys = { "grade", "course" };
		HBaseUtil hb=new HBaseUtil();
		try{
			System.out.println("------------------create table---------------------");
			hb.createTable(tableName, familys);  
			
			System.out.println("------------------add 4 records---------------------");
            hb.addRecord(tableName, "tom", familys[1], "Math", "90");  
            hb.addRecord(tableName, "tom", familys[1], "English", "95");  
            hb.addRecord(tableName, "daniel", familys[1],"Math","88"); 
            hb.addRecord(tableName, "daniel", familys[1],"Chinese","89"); 
            
            System.out.println("------------------delete a record---------------------");
            hb.delRecord(tableName, "tom", familys[1],"Math");

            System.out.println("------------------query a record---------------------");
            Result result=hb.getOneRecord(tableName, "daniel");
            for (KeyValue kv : result.raw()) {
            	String rowkey=new String(kv.getRow());
            	String family=new String(kv.getFamily());
            	String qualifier=new String(kv.getFamily());
            	String value=new String(kv.getValue()); 
				System.out.println(rowkey+"  "+family+":"+qualifier+" "+value); 
		    } 

            System.out.println("------------------query all records---------------------");
            ResultScanner rs=hb.getAllRecord(tableName);
            for (Result r : rs) {
				for (KeyValue kv : r.raw()) {
					String rowkey=new String(kv.getRow());
	            	String family=new String(kv.getFamily());
	            	String qualifier=new String(kv.getFamily());
	            	String value=new String(kv.getValue()); 
					System.out.println(rowkey+"  "+family+":"+qualifier+" "+value); 
				}
		    } 
            
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
