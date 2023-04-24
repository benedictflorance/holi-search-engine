package cis5550.jobs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import cis5550.kvs.Row;

public class Collapse {
	public static void main(String[] args) throws Exception {
		File in = new File ("/Users/seankung/upenn/cis555/holi-search-engine/worker1/168230258879611bf3e7b-1bfc-427e-bd50-a75d256eb591.appendOnly");
		File out = new File ("/Users/seankung/upenn/cis555/holi-search-engine/worker1/finish.table");
		out.createNewFile();
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(out));
		byte[] lf = {10};
		RandomAccessFile log = new RandomAccessFile (in, "r");
		System.out.println(log.length());
		Set<String> done = new HashSet<String>();
		while (log.getFilePointer() < log.length()) {
			float progress =  ((float) log.getFilePointer() / (float) log.length()) * 100.f;
			System.out.println("Progress: " + log.getFilePointer() + "/" + log.length() + " = " + progress + "%");
			Map<String, Row> rows = new HashMap<String, Row>();
			while (rows.size() < 2000 && log.getFilePointer() < log.length()) {
				Row r = Row.readFrom(log);
				if (r == null) {
					break;
				}
				if (done.contains(r.key())) {
					continue;
				}
				if (rows.containsKey(r.key())) {
					Row existing = rows.get(r.key());
					for (String c : r.columns()) {
						existing.put(c, r.get(c));
					}
				} else {
					rows.put(r.key(), r);
					done.add(r.key());
				}
			}
			System.out.println("Collected 2000 rows in memory");
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(in));
			bis.skip(log.getFilePointer());
			while (bis.available() > 0) {
				Row p = Row.readFrom(bis);
				if (p == null) {
					break;
				}
				if (rows.containsKey(p.key())) {
					Row existing = rows.get(p.key());
					for (String c : p.columns()) {
						existing.put(c, p.get(c));
					}
				}
			}
			bis.close();
			for (Entry<String, Row> e : rows.entrySet()) {
				bos.write(e.getValue().toByteArray());
				bos.write(lf);
			}
		}
		log.close();
		bos.flush();
		bos.close();
		
	}
}
