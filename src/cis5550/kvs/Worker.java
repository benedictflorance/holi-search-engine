
package cis5550.kvs;
import static cis5550.webserver.Server.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.webserver.*;

public class Worker extends cis5550.generic.Worker {

	Map<String, Table> tables;
	String id;
	String dir;
	long lastReq;
	
	public Worker() {
		tables = new ConcurrentHashMap<String, Table>();
		lastReq = System.currentTimeMillis();
	}
	
	public String readID(File idFile) {
		try {
			FileReader fr = new FileReader(idFile);
			char[] buf = new char[128];
			int num = fr.read(buf);
			fr.close();
			return new String(buf, 0, num);
		}  catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public String makeID() {
		Random rand = new Random();
		StringBuilder sb = new StringBuilder(5);
		for (int i = 0; i < 5; i++) {
			char c = (char) (rand.nextInt(26) + 97);
			sb.append(c);
		}
		return sb.toString();
		
	}
	
	public synchronized String putTable(Request req, Response res) throws Exception {
		if (!tables.containsKey(req.params("table"))) {
			tables.put(req.params("table"), new MemTable(req.params("table"), dir));
		}
		Table t = tables.get(req.params("table"));
		Row row = t.getRowForDisplay(req.params("row"));
		if (row == null) {
			row = new Row(req.params("row"));
		}
		row.put(req.params("col"), req.bodyAsBytes());
		t.putRow(req.params("row"), row);
		res.status(200, "OK");
		return "OK";
	}
	
	
	public synchronized String getTable(Request req, Response res) throws Exception {
		if (!tables.containsKey(req.params("table"))) {
			res.status(404, "Not Found");
			return "Not Found";
		}
		Table t = tables.get(req.params("table"));
		Row row = t.getRow(req.params("row"));
		if (row == null) {
			res.status(404, "Not Found");
			return "Not Found";
		}
		byte[] content = row.getBytes(req.params("col"));
		if (content == null) {
			res.status(404, "Not Found");
			return "Not Found";
		}
		res.bodyAsBytes(content);
		res.status(200, "OK");
		return null;
	}
	
	public void definePut() throws Exception {
		put("/data/:table/:row/:col", (req, res) -> {
			updateAccessTime();
			return putTable(req, res);
		});
		
		put("/persist/:table", (req, res) -> {
			updateAccessTime();
			String tKey = req.params("table");
			if (!tables.containsKey(tKey)) {
				tables.put(tKey, new PersistentTable(tKey, dir));
				res.status(200, "OK");
				return "OK";
			}
			if (!tables.get(tKey).persistent()) {
				// convert to persistent
				MemTable mt = (MemTable) tables.get(tKey);
				Map<String, Row> data = mt.getAllData();
				Table pt =  new PersistentTable(tKey, dir, data);
				tables.remove(tKey);
				tables.put(tKey, pt);
				res.status(200, "OK");
				return "OK";
			}
			// Already persistent
			res.status(403, "Forbidden");
			return "403 Forbidden";
		});
		
		put("/data/:table", (req, res) -> {
			updateAccessTime();
			if (!tables.containsKey(req.params("table"))) {
				tables.put(req.params("table"), new MemTable(req.params("table"), dir));
			}
			Table t = tables.get(req.params("table"));
			InputStream bis = new ByteArrayInputStream(req.bodyAsBytes());
			while (bis.available() > 0) {
				Row r = Row.readFrom(bis);
				t.putRow(r.key, r);
			}
			return "OK";
		});
		
		put("/rename/:table", (req, res) -> {
			updateAccessTime();
			if (!tables.containsKey(req.params("table")) || req.body() == null) {
				res.status(404, "Not Found");
				return "Not Found";
			}
			if (req.body().length() == 0) {
				res.status(403, "Forbidden");
				return "Rename requested but no new name found in body";
			}
			// Get original table
			Table t = tables.get(req.params("table"));
			// Copy table to new table with new name
			tables.put(req.body(), t);
			// Remove old table
			tables.remove(t.getKey());
			// Set new table to new name
			boolean success = t.rename(req.body());
			if (!success) {
				res.status(500, "Internal Server Error");
				return "Rename failed";
			}
			res.status(200, "OK");
			return "OK";
		});

		put("/delete/:table", (req, res) -> {
			updateAccessTime();
			if (!tables.containsKey(req.params("table"))) {
				res.status(404, "Nor Found");
				return "Not Found";
			}
			Table t = tables.get(req.params("table"));
			t.delete();
			tables.remove(req.params("table"));
			return "OK";
		});
	}
	
	public void defineGet() throws Exception {
		get("/", (req, res) -> {
			updateAccessTime();
			res.status(200, "OK");
			res.header("Content-Type", "text/html");
			StringBuilder ret = new StringBuilder();
			ret.append("<h2>Worker " + id + "</h2>");
			ret.append("<html><table><tr><th>Table Key</th><th>Number of Rows</th></tr>");
			for (String t : tables.keySet()) {
				Table tb = tables.get(t);
				ret.append("<tr><td><a href=\"view/" + t + "\">" + t + "</a></td><td>" + String.valueOf(tb.numRows()) +"</td>");
				if (tb.persistent()) {
					ret.append("<td>persistent</td>");
				}
				ret.append("</tr>");
			}
			ret.append("</table></html>");
			return ret.toString();
		});
		
		get("/view/:table", (req, res) -> {
			updateAccessTime();
			String tkey = req.params("table");
			if (!tables.containsKey(tkey)) {
				res.status(404, "Not Found");
				return "Not Found";
			}
			
			StringBuilder ret = new StringBuilder();
			ret.append("<h2>View Table: " + tkey + "</h2><html><table><tr><th>Row Key</th>");
			Table tb = tables.get(tkey);
			Set<String> cols = new HashSet<String>();
			// set a column for each column in each row.
			Set<String> rows =  tb.getRowKeys();
			List<String> rowKeysSorted = new ArrayList<String>();
			for (String r : rows) {
				rowKeysSorted.add(r);
			}
			Collections.sort(rowKeysSorted);	
			String startRow = req.queryParams("fromRow");
			int offset = startRow == null? 0 : Integer.parseInt(startRow);
			for (int i = 0; i + offset < rowKeysSorted.size() && i < 10; i++) {
				Row row = tb.getRowForDisplay(rowKeysSorted.get(i + offset));
				for (String c : row.columns()) {
					if (!cols.contains(c)) {
						ret.append("<th>" + c + "</th>");
						cols.add(c);
					}
				}
			}
			ret.append("</tr>");
			// if the row contains the column, show value
			// if not, leave empty.
			for (int i = 0; i + offset < rowKeysSorted.size() && i < 10; i++) {
				ret.append("<tr><td>" + rowKeysSorted.get(i + offset) + "</td>");
				Row row = tb.getRowForDisplay(rowKeysSorted.get(i + offset));
				for (String c : cols) {
					String val = row.get(c);
					if (val == null) {
						ret.append("<td> </td>");
					} else {
						ret.append("<td>" + val + "</td>");
					}
				}
			}
			ret.append("</table></html>");
			if (rowKeysSorted.size() - offset > 10) {
				ret.append("<a href=\"/view/" + tkey + "?fromRow=" + String.valueOf(offset + 10) + "\">Next</a>");
			}
			res.status(200, "OK");
			res.header("Content-Type", "text/html");
			return ret.toString();
		});

		get("/table", (req, res) -> {
			updateAccessTime();
			StringBuilder ret = new StringBuilder();
			for (String t : tables.keySet()) {
				ret.append(t + "\n");
			}
			res.header("Content-Type", "text/plain");
			return ret.toString();
		});

		get("/data/:table/:row/:col", (req, res) -> {
			updateAccessTime();
			return getTable(req, res);
		});

		get("/data/:table/:row",  (req, res) -> {
			updateAccessTime();
			if (!tables.containsKey(req.params("table"))) {
				res.status(404, "Not Found");
				return "Not Found";
			}
			Table t = tables.get(req.params("table"));
			Row r = t.getRow(req.params("row"));
			if (r == null) {
				res.status(404, "Not Found");
				return "Not Found";
			}
			byte[] content = r.toByteArray();
			res.status(200, "OK");
			res.header("Content-Length", String.valueOf(content.length));
			res.bodyAsBytes(content);
			return null;
		});
		
		get("/data/:table", (req, res) -> {
			updateAccessTime();
			if (!tables.containsKey(req.params("table"))) {
				res.status(404, "Not Found");
				System.out.println(req.params("table") + "Not Found");
				return "Not Found";
			}
			Table t = tables.get(req.params("table"));
			String start = req.queryParams("startRow");
			String end = req.queryParams("endRowExclusive");
			byte[] lf = {10};
			for (String rKey : t.getRowKeys()) {
				if (start != null) {
					if (rKey.compareTo(start) < 0) {
						continue;
					}
				}
				if (end != null) {
					if (rKey.compareTo(end) >= 0) {
						continue;
					}
				}
				Row r = t.getRow(rKey);
				res.write(r.toByteArray());
				res.write(lf);
			}
			res.write(lf);
			return null;
		});
		
		get("/count/:table", (req, res) -> {
			updateAccessTime();
			if (!tables.containsKey(req.params("table"))) {
				res.status(404, "Not Found");
				return "Not Found";
			}
			Table t = tables.get(req.params("table"));
			String ret = String.valueOf(t.numRows());
			res.status(200, "OK");
			res.header("Content-Type", "text/plain");
			res.header("Content-Length", String.valueOf(ret.length()));
			return String.valueOf(t.numRows());
		});
		
	}
	
	public void recover() throws Exception {
		File d = new File(dir);
		File[] l = d.listFiles();
		if (l == null) {
			return;
		}
		for (File f : l) {
			String[] name = splitExt(f.getName());
			if (name == null) {
				continue;
			}
			if (!name[1].equals(".table")) {
				continue;
			}
			Table rec = new PersistentTable(name[0], dir, f);
			tables.put(name[0], rec);
		}
	}
	
	public String[] splitExt(String name) {
		int i = name.indexOf('.');
		if (i == -1 || i == 0) {
			return null;
		}
		String[] ret = new String[2];
		ret[0] = name.substring(0, i);
		ret[1] = name.substring(i);
		return ret;
	}
	
	public void updateAccessTime() {
		lastReq = System.currentTimeMillis();
	}
	
	private class GarbageCollector extends Thread {
		public GarbageCollector() { }
		public void run() {
			while (true) {
				try {
					sleep(60000);
					if (System.currentTimeMillis() - lastReq > 60000) {
						for (String tKey : tables.keySet()) {
							tables.get(tKey).collectGarbage();
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
		}
	}
	
	public static void main(String[] args) {
		try {
			Worker wk = new Worker();
			wk.dir = args[1];
			File idFile = new File(wk.dir + "/id");
			if (idFile.exists()) {
				wk.id = wk.readID(idFile);
			} else {
				wk.id = wk.makeID();
				new File(wk.dir).mkdir();
				idFile = new File(wk.dir + "/id");
				FileWriter fw = new FileWriter(idFile);
				fw.write(wk.id);
				fw.close();
			}
			wk.recover();
			int port = Integer.parseInt(args[0]);
			port(port);
			startPingThread(args[2], wk.id, port);
			wk.definePut();
			wk.defineGet();
			Thread garbageCollector = wk.new GarbageCollector();
			garbageCollector.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}