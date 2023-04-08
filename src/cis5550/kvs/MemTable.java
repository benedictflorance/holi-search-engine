package cis5550.kvs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MemTable implements Table {
	Map<String, Row> data;
	String id;
	RandomAccessFile log;
	File tableFile;
	String dir;
	
	public MemTable(String tKey, String dir) throws FileNotFoundException {
		this.id = tKey;
		this.dir = dir;
		this.data = new ConcurrentHashMap<String, Row>();
		this.tableFile = new File(dir + "/" + tKey + ".table");
		this.log = new RandomAccessFile(tableFile, "rw");
	}
	public synchronized void putRow(String rKey, Row row) throws IOException {
		data.put(rKey, row);
		long offset = log.length();
		byte[] rowContent = row.toByteArray();
		log.seek(offset);
		log.write(rowContent);
		byte[] lf = {10};
		log.write(lf);
	}
	public Row getRow(String rKey) {
		return data.get(rKey);
	}
	public Row getRowForDisplay(String rKey) {
		return getRow(rKey);
	}
	public boolean persistent() {
		return false;
	}
	public int numRows() {
		return data.size();
	}
	public  Set<String> getRowKeys() {
		return data.keySet();
	}
	public String getKey() {
		return id;
	}
	public boolean rename(String tKey) {
		id = tKey;
		return true;
	}
	public synchronized void delete() throws IOException {
		data.clear();
		log.close();
		tableFile.delete();
	}
	public synchronized void collectGarbage() throws IOException {
		log.close();
		tableFile.delete();
		this.tableFile = new File(dir + "/" + id + ".table");
		this.log = new RandomAccessFile(tableFile, "rw");
		for (String tKey : data.keySet()) {
			Row r = data.get(tKey);
			long offset = log.length();
			byte[] rowContent = r.toByteArray();
			log.seek(offset);
			log.write(rowContent);
			byte[] lf = {10};
			log.write(lf);
		}
	}
	
	public Map<String, Row> getAllData() {
		return data;
	}
}
