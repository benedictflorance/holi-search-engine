package cis5550.kvs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cis5550.jobs.Sort;
import cis5550.webserver.Server;

public class AppendOnly implements Table {
	BufferedOutputStream bos;
	File tableFile;
	String id;
	String dir;
	long numRows;
	
	
	public AppendOnly(String tKey, String dir) throws IOException {
		this.tableFile = new File(dir + "/" + tKey + ".appendOnly");
		this.tableFile.createNewFile();
		this.bos = new BufferedOutputStream(new FileOutputStream(tableFile));
		this.id = tKey;
		this.dir = dir;
		numRows = 0;
	}

	public AppendOnly(String tKey, String dir, File logFile) throws Exception {
		this.tableFile = logFile;
		this.bos = new BufferedOutputStream(new FileOutputStream(tableFile));
		this.id = tKey;
		this.dir = dir;
		recover();
	}
	
	public AppendOnly(String tKey, String dir, Map<String, Row> data) throws Exception {
		this(tKey, dir);
		for (String rKey : data.keySet()) {
			putRow(rKey, data.get(rKey));
		}
		
	}
	
	private synchronized void recover() throws Exception {

	}

	public synchronized void putRow(String rKey, Row row) throws Exception {
		try {
			byte[] lf = {10};
			bos.write(row.toByteArray());
			bos.write(lf);
			numRows++;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public synchronized Row getRowForDisplay(String rKey) throws Exception {
		return null;
	}
	
	public synchronized boolean existRow(String rKey) {
		return false;
	}

	public synchronized Row getRow(String rKey) throws Exception {
		return null;
	}
	
	public Row getRowNoLock(String rKey) throws Exception {
		return null;
	}

	public boolean persistent() {
		return true;
	}
	
	public int numRows() {
		return 0;
	}
	
	public synchronized Set<String> getRowKeys() {
		return new HashSet<String>();
	}
	
	public String getKey() {
		return id;
	}
	
	public synchronized boolean rename(String tKey) throws IOException {
		if (tKey.equals(id)) {
			// Do nothing if new key is the same as the old key.
			return true;
		}
		boolean success = false;
		id = tKey;
		File newTable = new File(dir + "/" + id + ".table");
		success = tableFile.renameTo(newTable);
		tableFile = newTable;
		return success;
	}

	public synchronized void delete() throws IOException {
		try {
			Files.delete(tableFile.toPath());
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.tableFile = null;
		
	}
	public synchronized void collectGarbage() throws Exception {
		// AppendOnly does not perform garbage collection.
	}

	public synchronized void putBatch(List<Row> batch, Server server) {
		try {
			for (Row temp : batch) {
				putRow(temp.key(), temp);
			}
			this.bos.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public synchronized File reduce() throws Exception {
		int numDivision = 8;
		List<File> sorts = Sort.divideAndSort(tableFile, numDivision);
		while (numDivision != 1) {
			List<File> merges = new ArrayList<File>();
			for (int i = 0; i < numDivision; i += 2) {
				File merge = new File(dir + "/sort-" + numDivision + "-" + i + "-" + (i + 1) + ".table");
				Sort.merge(sorts.get(i), sorts.get(i + 1), merge);
				merges.add(merge);
			}
			sorts = merges;
			numDivision /= 2;
		}
		File collapse = new File(dir + "/" + id + ".table");
		Sort.collapse(sorts.get(0), collapse);
		return collapse;
	}
}