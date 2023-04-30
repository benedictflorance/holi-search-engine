package cis5550.jobs;

import static cis5550.kvs.Row.readFrom;
import java.io.RandomAccessFile;
import cis5550.kvs.Row;

public class IdfIndex {
	
	public static void main(String args[]) {
		 try {
			 final Integer N =462821;
			 RandomAccessFile index = new RandomAccessFile("/Users/namitashukla/Desktop/indexfilter.table", "rw");
			 RandomAccessFile file = new RandomAccessFile("/Users/namitashukla/Desktop/w-metrics.table", "rw");
	         while(true){
	             Row row = readFrom(index);
	             if(row==null)
	            	 break;
	             String urls = row.get("url");
	             if(urls == null)
	                continue;
	             String rowkey = row.key();
	             Integer df = urls.split(",").length + 1;
		         Double idf = Math.log(1.0 * N / df);
		         Row newRow = new Row(rowkey);
		         newRow.put("df",String.valueOf(df));
		         newRow.put("idf",String.valueOf(idf));
	             file.write(newRow.toByteArray());
	             file.writeBytes("\n");
	         }
		 }
		 catch(Exception e) {
			 e.printStackTrace();
		 }

	}
}
