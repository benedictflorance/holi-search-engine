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
		log.seek(offset);
		log.write(row.toByteArray());
		log.writeBytes("\n");
	}
	public synchronized Row getRow(String rKey) {
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
	public synchronized Set<String> getRowKeys() {
		return data.keySet();
	}
	public String getKey() {
		return id;
	}
	public synchronized boolean rename(String tKey) {
		id = tKey;
		return true;
	}
	public synchronized void delete() throws IOException {
		data.clear();
		log.close();
		tableFile.delete();
	}
	public synchronized void collectGarbage() throws IOException {
		log.setLength(0);
		for (String tKey : data.keySet()) {
			Row r = data.get(tKey);
			long offset = log.length();
			log.seek(offset);
			log.write(r.toByteArray());
			log.writeBytes("\n");
		}
	}
	
	public synchronized Map<String, Row> getAllData() {
		return data;
	}
}