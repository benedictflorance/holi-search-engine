/*
package cis5550.kvs;
import static cis5550.webserver.Server.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
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
		Row row = t.getRow(req.params("row"));
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
	
	public void definePut() {
		put("/data/:table/:row/:col", (req, res) -> {
			updateAccessTime();
			return putTable(req, res);
		});
		
		put("/persist/:table", (req, res) -> {
			updateAccessTime();
			String tKey = req.params("table");
			if (!tables.containsKey(tKey)) {
				tables.put(tKey, new PersistentTable(dir, tKey));
				res.status(200, "OK");
				return "OK";
			}
			if (!tables.get(tKey).persistent()) {
				// convert to persistent
				return "OK";
			}
			res.status(403, "Forbidden");
			return "403 Forbidden";
		});
		
		put("/data/:table", (req, res) -> {
			updateAccessTime();
			if (!tables.containsKey(req.params("table"))) {
				tables.put(req.params("table"), new PersistentTable(dir, req.params("table")));
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
			if (!tables.containsKey(req.params("table"))) {
				res.status(404, "Nor Found");
				return "Not Found";
			}
			Table t = tables.get(req.params("table"));
			tables.put(req.body(), t);
			tables.remove(t.getKey());
			t.setKey(req.body());
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
				Row row = tb.getRow(rowKeysSorted.get(i + offset));
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
				Row row = tb.getRow(rowKeysSorted.get(i + offset));
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
			Table rec = new PersistentTable(dir, f, name[0]);
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
	
	public void collectGarbage() throws Exception {
		for (String tKey : tables.keySet()) {
			tables.get(tKey).collectGarbage();
		}
	}
	
	private class GarbageCollector extends Thread {
		public GarbageCollector() { }
		public void run() {
			while (true) {
				try {
					sleep(10000);
					if (System.currentTimeMillis() - lastReq > 10000) {
						collectGarbage();
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
			wk.id = idFile.exists()? wk.readID(idFile) : wk.makeID();
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
*/
package cis5550.kvs;

import static cis5550.webserver.Server.port;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static cis5550.webserver.Server.*;

import cis5550.tools.HTTP;
import cis5550.tools.Logger;


public class Worker extends cis5550.generic.Worker{
	private static final Logger logger= Logger.getLogger(Worker.class);
	public static int port;
	public static String storageDir;
	public static String masterAddr;
	public static String id;
	public static Map<String, Map<String, Row>> KVStore = new ConcurrentHashMap<>();
	public static Map<String, RandomAccessFile> tableLogMap = new ConcurrentHashMap<>();
	public static Set<String> persistentTables = new TreeSet<String>();
	public static long lastRequestTime = System.currentTimeMillis();
	static List<WorkerEntry> workers =  Collections.synchronizedList(new ArrayList<>());
	static List<WorkerEntry> replicationWorkers= Collections.synchronizedList(new ArrayList<>());
	static List<WorkerEntry> maintenanceWorkers= Collections.synchronizedList(new ArrayList<>());
	
	public static void main(String args[]) throws Exception {
		if (args.length != 3) {
            System.err.println("Usage: Worker <port> <storageDir> <masterIp:masterPort>");
            System.exit(1);
        }
		port = Integer.parseInt(args[0]);
		port(port);
		storageDir = args[1];
		
		
		masterAddr = args[2];
        String[] masterIpPort = args[2].split(":");
        if (masterIpPort.length != 2) {
            System.err.println("Invalid master address: " + args[2]);
        }
        
        Path idFilePath = Paths.get(storageDir+"/id");
        
        if (Files.exists(idFilePath)) {
        	logger.info("reading id");
            try {
                id = new String(Files.readAllBytes(idFilePath), StandardCharsets.UTF_8).trim();
            } catch (IOException e) {
                System.err.println("Error reading ID file: " + e.getMessage());
                id = generateRandomId();
            }
        } else {
        	new File(storageDir).mkdir();
        	
        	logger.info("writing id");
            id = generateRandomId();
            try {
                Files.write(idFilePath,id.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                System.err.println("Error writing ID file: " + e.getMessage());
            }
        }
        
        startPingThread();
        
        startRecovery();
        
        garbageCollection();
        
        downloadWorkers();
        
        maintenance();
        
        put("/data/:tableName/:rowKey/:columnName", (req,res)-> {
        	lastRequestTime = System.currentTimeMillis();
        	String tableName = req.params("tableName");
            String rowKey = req.params("rowKey");
            String columnName = req.params("columnName");
            
            byte[] columnValue = req.bodyAsBytes();
            
            String ifColumn = req.queryParams("ifcolumn");
            String equals = req.queryParams("equals");
            
            if (ifColumn != null && equals != null) {
                // If both are present, check if the specified column exists and has the specified value
                if (!KVStore.containsKey(tableName) || !KVStore.get(tableName).containsKey(rowKey) || 
                		KVStore.get(tableName).get(rowKey).get(ifColumn) ==null||
                		!KVStore.get(tableName).get(rowKey).get(ifColumn).equals(equals)) {
                  // If not, return "FAIL"
                  res.status(200, "OK");
                  return "FAIL";
                }
              }
            
            putRow(tableName, rowKey, columnName, columnValue);
            
            //forward the request to the two replication workers
            for(WorkerEntry w: replicationWorkers) {
            	logger.info("I am sending req to replicas");
            	logger.info(w.address);
            	HTTP.doRequest("PUT", "http://"+w.address+"/replicate/" + tableName + "/" + rowKey + "/" + columnName, columnValue);
            }
            
            // Return OK
            res.status(200,"OK");
            res.body("OK");
            return "OK";
        });
        
        put("/replicate/:tableName/:rowKey/:columnName", (req,res)-> {
        	logger.info("Received Replication Request");
        	
        	lastRequestTime = System.currentTimeMillis();
        	String tableName = req.params("tableName");
            String rowKey = req.params("rowKey");
            String columnName = req.params("columnName");
            
            byte[] columnValue = req.bodyAsBytes();
            
            putRow(tableName, rowKey, columnName, columnValue);
            
            // Return OK
            res.status(200,"OK");
            res.body("OK");
            return "OK";
        });
        
        get("/data/:tableName/:rowKey/:columnName", (req,res)-> {
        	lastRequestTime = System.currentTimeMillis();
            String tableName = req.params("tableName");
            String rowKey = req.params("rowKey");
            String columnName = req.params("columnName");

            // Check if the table, row, and column exist in the data structure
            if (!KVStore.containsKey(tableName)) {
                res.status(404,"Not Found");
                return "404";
            }
            Map<String, Row> rows = KVStore.get(tableName);
            if (!rows.containsKey(rowKey)) {
            	res.status(404,"Not Found");
            	return "404";
            }
            
            Row row = getRow(tableName,rowKey);
            
            if (!row.columns().contains(columnName)) {
            	res.status(404,"Not Found");
            	return "404";
            }
            
            if(req.queryParams("version") != null)
            {
            	Integer version= Integer.parseInt(req.queryParams("version"));
            	res.header("Version", version.toString());
            	byte[] columnValue = row.VgetBytes(columnName,version);
            	if(columnValue==null) {
            		res.status(404,"Not Found");
                	return "404";
            	}
            	res.bodyAsBytes(columnValue);
            	
            }
            else {
            	 // Get the value of the column and set it as the response body
            	res.header("Version", row.version().toString());
                byte[] columnValue = row.getBytes(columnName);
                if(columnValue==null) {
            		res.status(404,"Not Found");
                	return "404";
            	}
                res.bodyAsBytes(columnValue);
            }
            
            res.status(200, "OK");
            return null;
        	
        });
        
        
        get("/",(req,res)-> {
        	lastRequestTime = System.currentTimeMillis();
        	res.status(200, "OK");
			res.type("text/html");
        	StringBuilder html = new StringBuilder();
            html.append("<html><head><title>Table List</title></head><body>");
            html.append("<h1>Table List</h1>");
            html.append("<table><tr><th>Table Name</th><th>Number of Keys</th><th>Persistent</th></tr><tbody>");
            for (Entry<String, Map<String, Row>> entry : KVStore.entrySet()) {
                html.append("<tr>");
                html.append("<td><a href=\"/view/").append(entry.getKey()).append("\">").append(entry.getKey()).append("</td>").append("</a><td>")
                .append(entry.getValue().size()).append("</td><td>");
                if(persistentTables.contains(entry.getKey()))
                	html.append("persistent");
                html.append("</td>");
                html.append("</tr>");
            }
            html.append("</tbody></table></body></html>");
            return html.toString();
        });
        
       
        get("/view/:tableName", (req,res)->{
        	lastRequestTime = System.currentTimeMillis();
        	String tableName = req.params("tableName");
        	
        	if (!KVStore.containsKey(tableName)) {
                res.status(404,"Not Found");
                return "404";
            }
        	
        	Integer fromRow = 0;
            String fromRowParam = req.queryParams("fromRow");
            if (fromRowParam!=null) {
                try {
                    fromRow = Integer.parseInt(fromRowParam);
                } catch (NumberFormatException e) {
                    // Invalid fromRow parameter, ignore
                }
            }
            
        	//sort rows and sort columns
        	Map<String, Row> rowkeyMap = KVStore.get(tableName);
        	List<String> rowKey = new ArrayList<String>();
        	for (Entry<String, Row> entry : rowkeyMap.entrySet()) {
        		rowKey.add(entry.getKey());
        	}
        	Collections.sort(rowKey,String.CASE_INSENSITIVE_ORDER);
        	
        	
        	//get actual row objects from persistent tables
        	List<String> colKey = new ArrayList<String>();
        	Map<String, Row> pRowkeyMap = new ConcurrentHashMap<>();
            if(persistentTables.contains(tableName)) {
            	
            	for (Entry<String, Row> entry : rowkeyMap.entrySet()) {
	            	String start = entry.getValue().get("pos");	
	            	RandomAccessFile rf = tableLogMap.get(tableName);
	            	rf.seek(Integer.parseInt(start));
	            	Row pRow = Row.readFrom(rf);
	            	pRowkeyMap.put(entry.getKey(), pRow);
	            	Set<String> colSet = pRow.columns();
            		for (String col : colSet)
            			colKey.add(col);
            	}
            }
            else {
            	for (Entry<String, Row> entry : rowkeyMap.entrySet()) {
            		Set<String> colSet = entry.getValue().columns();
            		for (String col : colSet)
            			colKey.add(col);
            	}
            }

        	Collections.sort(colKey,String.CASE_INSENSITIVE_ORDER);
        	colKey = colKey.stream()
                    .distinct()
                    .collect(Collectors.toList());
        	
        	res.status(200, "OK");
			res.type("text/html");
		    StringBuilder tableHtml = new StringBuilder();
		    tableHtml.append("<html><head><title>Table View</title></head><body>");
            tableHtml.append("<h1>").append(tableName).append("</h1>");
		    tableHtml.append("<div><table>\n");
		    tableHtml.append("<tr>\n");
		    tableHtml.append("<th></th>\n");
		    
		    List<String> pageColKey = new ArrayList<String>();
		    Integer NUM_ROWS = 10;
		    Integer START_FROM = fromRow;
		    for (String r : rowKey) {
			      if(START_FROM!=0) {
			    	  START_FROM--;
			    	  continue; 
			      }
			      if(NUM_ROWS==0)
			    	  break;
			      Row rowObj = null;
			      if(persistentTables.contains(tableName))
			    	  rowObj= pRowkeyMap.get(r);
			      else 
			    	  rowObj=rowkeyMap.get(r);
			      for (String c : colKey) { 
			    	  if(rowObj.get(c)!=null && !pageColKey.contains(c)) {
			    		  pageColKey.add(c);
			    		  //tableHtml.append("<th>").append(c).append("</th>\n");
			    	  }
			      }
			      NUM_ROWS--;
			}
		    Collections.sort(pageColKey,String.CASE_INSENSITIVE_ORDER);
		    for(String c:pageColKey) {
		    	tableHtml.append("<th>").append(c).append("</th>\n");
		    }

		    tableHtml.append("</tr>\n");
		    
		    NUM_ROWS = 10;
		    START_FROM = fromRow;
		    for (String r : rowKey) {
		      if(START_FROM!=0) {
		    	  START_FROM--;
		    	  continue; 
		      }
		      if(NUM_ROWS==0)
		    	  break;
		      tableHtml.append("<tr>\n");
		      tableHtml.append("<td>").append(r).append("</td>\n");
		      Row rowObj = null;
		      if(persistentTables.contains(tableName))
		    	  rowObj= pRowkeyMap.get(r);
		      else 
		    	  rowObj=rowkeyMap.get(r);
		      for (String c : pageColKey) { 
		    	  String value = rowObj.get(c);
		    	  if(value!=null)
		    		  tableHtml.append("<td>").append(value).append("</td>\n");
		    	  else
		    		  tableHtml.append("<td>").append("").append("</td>\n");
		      }
		      tableHtml.append("</tr>\n");
		      NUM_ROWS--;
		    }
		    tableHtml.append("</table></div>");

		    if(fromRow+10<rowKey.size())
		    	tableHtml.append("<div><a href=\"/view/").append(tableName).append("?fromRow=").append(fromRow+10).append("\">").append("Next").append("</a></div>");
		    
		    tableHtml.append("</body></html>");
		    
		    return tableHtml;
        });
        
        
        put("/persist/:tableName", (req,res) -> {
        	lastRequestTime = System.currentTimeMillis();
        	String tableName = req.params("tableName");
        	if(persistentTables.contains(tableName)|| KVStore.containsKey(tableName)) {
        		res.status(403,"Forbidden");
        		res.type("text/html");
        		return "403 Forbidden. Persistent table already exists.";
        	}
        	
        	persistentTables.add(tableName);
        	
        	//Create an empty persistent table
        	if (tableName!=null && !KVStore.containsKey(tableName)) 
        		KVStore.put(tableName, new ConcurrentHashMap<>());
        	
        	String path = storageDir + File.separator + tableName + ".table";
        	File f = new File(path);
        	if(!f.exists())
        		f.createNewFile();
        	
        	RandomAccessFile rf = new RandomAccessFile(path,"rw");
        	
        	tableLogMap.put(tableName, rf);
        	res.status(200,"OK");
        	res.type("text/plain");
        	for(WorkerEntry w: workers)
        	{	if(!w.id.equals(id))
        			HTTP.doRequest("PUT", "http://"+w.address+"/replicate/persist/" + tableName, null);
        	}	
        	return "OK";
        });
        
        put("/replicate/persist/:tableName", (req,res) -> {
        	lastRequestTime = System.currentTimeMillis();
        	String tableName = req.params("tableName");
        	if(persistentTables.contains(tableName)|| KVStore.containsKey(tableName)) {
        		res.status(403,"Forbidden");
        		res.type("text/html");
        		return "403 Forbidden. Persistent table already exists.";
        	}
        	
        	persistentTables.add(tableName);
        	
        	//Create an empty persistent table
        	if (tableName!=null && !KVStore.containsKey(tableName)) 
        		KVStore.put(tableName, new ConcurrentHashMap<>());
        	
        	String path = storageDir + File.separator + tableName + ".table";
        	File f = new File(path);
        	if(!f.exists())
        		f.createNewFile();
        	
        	RandomAccessFile rf = new RandomAccessFile(path,"rw");
        	
        	tableLogMap.put(tableName, rf);
        	res.status(200,"OK");
        	res.type("text/plain");
        	
        	return "OK";
        });
        
        
        get("/tables",(req,res)->{
        	lastRequestTime = System.currentTimeMillis();
        	String result="";
        	for (Entry<String, Map<String, Row>> entry : KVStore.entrySet()) {
        		result+=entry.getKey()+"\n";
        	}
        	res.status(200,"OK");
        	res.type("text/plain");
        	res.header("Content-Length",String.valueOf(result.length()));
        	res.body(result);
        	return result;
        });
        
        put("/rename/:tableName", (req,res) ->{
        	lastRequestTime = System.currentTimeMillis();
        	
        	String tableName = req.params("tableName");
        	if (tableName!=null && !KVStore.containsKey(tableName)) {
        		res.status(404,"Not Found");
        		return "404";
        	}
        	
        	String newTableName = req.body();
        	if(KVStore.containsKey(newTableName)) {
        		res.status(409,"Table Name Already Exists");
        		return "409";
        	}
        	
        	KVStore.put(newTableName, KVStore.get(tableName));
        	KVStore.remove(tableName);
        	
        	
        	if(persistentTables.contains(tableName)) {
        		tableLogMap.put(newTableName, tableLogMap.get(tableName));
        		tableLogMap.remove(tableName);
        	}
        	
        	String path = storageDir + File.separator + tableName + ".table";
        	File f = new File(path);
        	String newPath = storageDir + File.separator + newTableName + ".table";
        	f.renameTo(new File(newPath));
        	
        	for(WorkerEntry w: workers)
        	{	if(!w.id.equals(id))
        			HTTP.doRequest("PUT", "http://"+w.address+"/replicate/rename/" + tableName, null);
        	}	

        	res.status(200,"OK");
        	res.type("text/plain");
        	return "OK";
        });
        
        put("/replicate/rename/:tableName", (req,res) ->{
        	lastRequestTime = System.currentTimeMillis();
        	
        	String tableName = req.params("tableName");
        	if (tableName!=null && !KVStore.containsKey(tableName)) {
        		res.status(404,"Not Found");
        		return "404";
        	}
        	
        	String newTableName = req.body();
        	if(KVStore.containsKey(newTableName)) {
        		res.status(409,"Table Name Already Exists");
        		return "409";
        	}
        	
        	KVStore.put(newTableName, KVStore.get(tableName));
        	KVStore.remove(tableName);
        	
        	
        	if(persistentTables.contains(tableName)) {
        		tableLogMap.put(newTableName, tableLogMap.get(tableName));
        		tableLogMap.remove(tableName);
        	}
        	
        	String path = storageDir + File.separator + tableName + ".table";
        	File f = new File(path);
        	String newPath = storageDir + File.separator + newTableName + ".table";
        	f.renameTo(new File(newPath));

        	res.status(200,"OK");
        	res.type("text/plain");
        	return "OK";
        });

        put("/delete/:tableName", (req,res) ->{
        	lastRequestTime = System.currentTimeMillis();
        	
        	String tableName = req.params("tableName");
        	if (tableName!=null && !KVStore.containsKey(tableName)) {
        		res.status(404,"Not Found");
        		return "404";
        	}
        	KVStore.remove(tableName);
        	
        	
        	if(persistentTables.contains(tableName)) {
        		persistentTables.remove(tableName);
        		tableLogMap.remove(tableName);
        	}
        	
        	String path = storageDir + File.separator + tableName + ".table";
        	File f = new File(path);
        	f.delete();
        	
        	for(WorkerEntry w: workers)
        	{	if(!w.id.equals(id))
        			HTTP.doRequest("PUT", "http://"+w.address+"/replicate/delete/" + tableName, null);
        	}
        	
        	res.status(200,"OK");
        	res.type("text/plain");
        	return "OK";
        });
        
        put("/replicate/delete/:tableName", (req,res) ->{
        	lastRequestTime = System.currentTimeMillis();
        	
        	String tableName = req.params("tableName");
        	if (tableName!=null && !KVStore.containsKey(tableName)) {
        		res.status(404,"Not Found");
        		return "404";
        	}
        	KVStore.remove(tableName);
        	
        	
        	if(persistentTables.contains(tableName)) {
        		persistentTables.remove(tableName);
        		tableLogMap.remove(tableName);
        	}
        	
        	String path = storageDir + File.separator + tableName + ".table";
        	File f = new File(path);
        	f.delete();
        	
        	res.status(200,"OK");
        	res.type("text/plain");
        	return "OK";
        });
        

        get("/data/:tableName/:rowKey", (req,res)-> {
        	lastRequestTime = System.currentTimeMillis();
            String tableName = req.params("tableName");
            String rowKey = req.params("rowKey");

            // Check if the table, row, and column exist in the data structure
            if (!KVStore.containsKey(tableName)) {
                res.status(404,"Not Found");
                return "404";
            }
            Map<String, Row> rows = KVStore.get(tableName);
            if (!rows.containsKey(rowKey)) {
            	res.status(404,"Not Found");
            	return "404";
            }
            logger.info("Received get row for " + rowKey);
            Row row = getRow(tableName,rowKey);
            logger.info(row.toString());
            res.bodyAsBytes(row.toByteArray());
            res.status(200, "OK");
            return null;
        	
        });
        
       
        get("/data/:tableName", (req,res)-> {
        	lastRequestTime = System.currentTimeMillis();
            String tableName = req.params("tableName");
            String startRow = req.queryParams("startRow");
            String endRowExclusive = req.queryParams("endRowExclusive");
            // Check if the table, row, and column exist in the data structure
            if (!KVStore.containsKey(tableName)) {
                res.status(404,"Not Found");
                return "404";
            }
            Map<String, Row> rows = KVStore.get(tableName);
            byte[] result = new byte[0];
            res.type("text/plain");
        	res.status(200, "OK");
            for (Entry<String, Row> entry : rows.entrySet()) {
        		if((startRow==null || startRow.compareTo(entry.getKey())<=0)&&
        				(endRowExclusive==null || endRowExclusive.compareTo(entry.getKey())>0)){
        			Row row = getRow(tableName,entry.getKey());
        			res.write(joinByteArray(result, row.toByteArray(),"\n".getBytes()));
        		}
        	}
            res.write("\n".getBytes());
            return null;
        });
        
        
        get("/hashdata/:tableName", (req,res)-> {
        	logger.info("Received Maintenance Request");
        	lastRequestTime = System.currentTimeMillis();
            String tableName = req.params("tableName");
            String startRow = req.queryParams("startRow");
            String endRowExclusive = req.queryParams("endRowExclusive");
            // Check if the table, row, and column exist in the data structure
            if (!KVStore.containsKey(tableName)) {
                res.status(404,"Not Found");
                return "404";
            }
            Map<String, Row> rows = KVStore.get(tableName);
            res.type("text/plain");;
        	res.status(200, "OK");
        	String result = "";
        	for (Entry<String, Row> entry : rows.entrySet()) {
        		if((startRow==null || startRow.compareTo(entry.getKey())<=0)&&
        				(endRowExclusive==null || endRowExclusive.compareTo(entry.getKey())>0)){
        			Row row = getRow(tableName,entry.getKey());
        			//send row key
        			result += entry.getKey();
        			result += ",";
        			result+=row.toString().hashCode();
        			result+="\n";
        		}
        	}
            result+="\n";
            res.body(result);
            return null;
        });
        
        put("/data/:tableName", (req,res)-> {
        	lastRequestTime = System.currentTimeMillis();
        	 String tableName = req.params("tableName");
        	 if (tableName!=null && !KVStore.containsKey(tableName)) {
                KVStore.put(tableName, new ConcurrentHashMap<>());
                String path = storageDir + File.separator + tableName + ".table";
             	File f = new File(path);
             	if(!f.exists())
             		f.createNewFile();
             }

             Map<String, Row> rows = KVStore.get(tableName);
             
             InputStream is = new ByteArrayInputStream(req.bodyAsBytes());
             Row row = Row.readFrom(is);
             while(row!=null) {
            	 if(persistentTables.contains(tableName)) {
  					RandomAccessFile rf = tableLogMap.get(tableName);
  					String rowKey= row.key();
  					rows.put(rowKey, new Row(rowKey));
  	            	Row pRow = rows.get(rowKey);
  	            	pRow.put("pos", String.valueOf(rf.length()));
  	            	//serialize it and store it in the file
  	            	synchronized(rf) {
  	            		rf.seek(rf.length());
  		            	rf.write(row.toByteArray());
  		            	rf.write("\n".getBytes(Charset.forName("UTF-8")));
  	            	}
  				}
  				else {
  					String rowKey= row.key();
  			        rows.put(rowKey,row);
  			        String path = storageDir + File.separator + tableName + ".table";
  			        File f = new File(path);
  	             	RandomAccessFile rf = new RandomAccessFile(f, "rw");
  	             	synchronized(rf) {
  	             		rf.seek(rf.length());
  	                 	rf.write(row.toByteArray());
  	                 	rf.write("\n".getBytes(Charset.forName("UTF-8")));
  	             	}
  	             	rf.close();
  				}
            	row = Row.readFrom(is); 
             }

 			res.status(200, "OK");
            res.type("text/plain");
        	return "OK";
        	
        });
        
        get("/count/:tableName", (req,res)-> {
        	lastRequestTime = System.currentTimeMillis();
        	 String tableName = req.params("tableName");

             // Check if the table, row, and column exist in the data structure
             if (!KVStore.containsKey(tableName)) {
                 res.status(404,"Not Found");
                 return "404";
             }
             Map<String, Row> rows = KVStore.get(tableName);
             res.status(200, "OK");
             res.type("text/plain");
             res.body(String.valueOf(rows.size()));
             return null;
        });
	}
	
	public static Row getRow(String tableName, String rowKey) throws Exception {
		 Map<String, Row> rows = KVStore.get(tableName);
		 Row row = rows.get(rowKey);
		 if(persistentTables.contains(tableName)) {
         	String start = row.get("pos");
         	RandomAccessFile rf = tableLogMap.get(tableName);
         	if(start!=null)
         	{
         		rf.seek(Integer.parseInt(start));
         		row = Row.readFrom(rf);
         	}
         }
		 return row;
	}
	
	public static void putRow(String tableName, String rowKey, String columnName, byte[] columnValue) throws Exception {
		// Check if the table already exists in the data structure
        if (tableName!=null && !KVStore.containsKey(tableName)) {
            KVStore.put(tableName, new ConcurrentHashMap<>());
            String path = storageDir + File.separator + tableName + ".table";
        	File f = new File(path);
        	if(!f.exists())
        		f.createNewFile();
        }
        
        // Check if the row already exists in the table
        Map<String, Row> rows = KVStore.get(tableName);
        if (!rows.containsKey(rowKey)) {
            rows.put(rowKey, new Row(rowKey));
        }
        
        Row row = rows.get(rowKey);
        
        if(persistentTables.contains(tableName)) {
        	RandomAccessFile rf = tableLogMap.get(tableName);
        	//When an existing row is modified
        	//it is read from the file, updated, 
        	//and then the newly modified row is appended at the end of the file, 
        	//and the position in memory is updated.
        	String start = row.get("pos");
        	Row pRow;
        	//it is a new row
        	if(start==null) {
        		pRow = new Row(rowKey);
        		pRow.put(columnName, columnValue);
        	}
        	//else read the existing row
        	else {
        		rf.seek(Integer.parseInt(start));
            	pRow = Row.readFrom(rf);
            	pRow.put(columnName, columnValue);
            	
        	}
        	//update the position of the row in memory
        	row.put("pos", String.valueOf(rf.length()));
        	synchronized(rf) {
        		rf.seek(rf.length());
            	rf.write(pRow.toByteArray());
            	rf.write("\n".getBytes(Charset.forName("UTF-8")));
        	}
        }
        else {
            row.put(columnName, columnValue);
            row.Vput(columnName, columnValue);
            String path = storageDir + File.separator + tableName + ".table";
        	File f = new File(path);
        	if(!f.exists())
        		f.createNewFile();
        	RandomAccessFile rf = new RandomAccessFile(f, "rw");
        	synchronized(rf) {
        		rf.seek(rf.length());
            	rf.write(row.toByteArray());
            	rf.write("\n".getBytes(Charset.forName("UTF-8")));
        	}
        	rf.close();
        }
	}
	
    private static String generateRandomId() {
        // generate a random ID of five lower-case letters
        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            char c = (char) ('a' + rand.nextInt(26));
            sb.append(c);
        }
        return sb.toString();
    }
    
	public static void startPingThread() {
		
		Thread daemonThread = new Thread(() -> {
            while (true) {
                try {
                    // create a URL for http://xxx/ping?id=yyy&port=zzz and call getContent() on it
                    String pingUrl = "http://" + masterAddr + "/ping?id=" + id + "&port=" + port;
                    HTTP.doRequest("GET", pingUrl,null);
                    // wait for the required interval
                    Thread.sleep(5000);
                } catch (Exception e) {
                    System.err.println("Error sending /ping request: " + e.getMessage());
                }
            }
        });
		daemonThread.start();
	}
	
	public static void startRecovery() {
		logger.info("Starting Recovery");
		File storageDirFile = new File(storageDir);
		File[] files = storageDirFile.listFiles();
		
		for (File file : files) {
		    if (file.isFile() && file.getName().endsWith(".table")) {
		        try {
		        	String tableName;
		        	int dotIndex = file.getName().lastIndexOf('.');
	                if(dotIndex == -1) 
	                	tableName = file.getName();
	                else 
	                	tableName = file.getName().substring(0, dotIndex);
	                
	                
	                if (tableName!=null && !KVStore.containsKey(tableName)) 
		        		KVStore.put(tableName, new ConcurrentHashMap<>());
	                
	                persistentTables.add(tableName);
	                RandomAccessFile tableFile = new RandomAccessFile(file, "rw");
	                tableLogMap.put(tableName, tableFile);
	                
	                Map<String, Row> rows = KVStore.get(tableName);
	                logger.info("tableName " + tableName);               
	                long offSet = 0;
	                Row row = Row.readFrom(tableFile);
	                while(row!=null) {
	    				String rowKey= row.key();
	    				if (!rows.containsKey(rowKey)) {
			                rows.put(rowKey, new Row(rowKey));
			            }
	    				Row newRow = rows.get(rowKey);
	    				newRow.put("pos", String.valueOf(offSet));
	    				offSet = tableFile.getFilePointer();
	    				row = Row.readFrom(tableFile);
	                }

		        } catch (Exception e) {
		            // Handle the exception
		        	System.out.println(e.toString());
		        }
		    }
		}
	}
	
    public static byte[] joinByteArray(byte[] byte1, byte[] byte2, byte[] byte3) {
    	if(byte3==null)
    		return ByteBuffer.allocate(byte1.length + byte2.length)
                .put(byte1)
                .put(byte2)
                .array();
    	else
    		return ByteBuffer.allocate(byte1.length + byte2.length + byte3.length)
                    .put(byte1)
                    .put(byte2)
                    .put(byte3)
                    .array();
    }
    
    static void garbageCollection() {
    	
    	Thread daemonThread = new Thread(() -> {
            while (true) {
                try {
                	
                	if(System.currentTimeMillis()-lastRequestTime>10000) {
                		logger.info("Starting garbage collection");
                		
                		//for persistent files
                		 for (Entry<String, RandomAccessFile> entry : tableLogMap.entrySet()) {
                			String tableName = entry.getKey();
                			String path = storageDir + File.separator +tableName + ".table";
                	        File f = new File(path);
                			String temppath = storageDir + File.separator +tableName + "temp.table";
                	        File tempf = new File(temppath);
                	        if(!tempf.exists())
                	        	tempf.createNewFile();
                	        
                	        
                	        RandomAccessFile newrf = new RandomAccessFile(temppath,"rw");
                	        RandomAccessFile rf = entry.getValue();
                	        
                	        Map<String, Row> rows = KVStore.get(tableName);
                	        synchronized(rf) {
	                            for (Entry<String, Row> r : rows.entrySet()) {

	                            		String start = r.getValue().get("pos");	
	                                    if(start!=null) {
	                                    	rf.seek(Integer.parseInt(start));
	 	                                    Row pRow = Row.readFrom(rf);
	 	                                    if(pRow!=null) {
		 	                                    r.getValue().put("pos",String.valueOf(newrf.length()));
		 	                                    newrf.seek(newrf.length());
		 	                                    newrf.write(pRow.toByteArray());
		 	                                    newrf.write("\n".getBytes());
	 	                                    }
	                                    }
	                        	}
	                            f.delete();
	                            tempf.renameTo(f);
	                            tableLogMap.put(tableName,newrf);
                	        }
                		 }
                		 
                		 //for non persistent files
                		 File storageDirFile = new File(storageDir);
                		 File[] files = storageDirFile.listFiles();
                			
                		for (File file : files) {
                				String tableName = null;
                			    if (file.isFile() && file.getName().endsWith(".table")) {
	                			    int dotIndex = file.getName().lastIndexOf('.');
	                		        if(dotIndex == -1) 
	                		            tableName = file.getName();
	                		        else 
	                		            tableName = file.getName().substring(0, dotIndex);
                			    }
                			    if(!persistentTables.contains(tableName)) {
                			    	String path = storageDir + File.separator +tableName + ".table";
                        	        File f = new File(path);
                        			String temppath = storageDir + File.separator +tableName + "temp.table";
                        	        File tempf = new File(temppath);
                        	        if(!tempf.exists())
                        	        	tempf.createNewFile();
                        	        
                        	        RandomAccessFile newrf = new RandomAccessFile(temppath,"rw");
                        	        Map<String, Row> rows = KVStore.get(tableName);
        	                        for (Entry<String, Row> r : rows.entrySet()) {
        	                        	Row row = getRow(tableName, r.getKey());
        	                        	newrf.write(row.toByteArray());
        	                        }
        	                        newrf.close(); 
        	                        f.delete();
    	                            tempf.renameTo(f);
                			    }
                		}
                			       
                	}
                	Thread.sleep(10000);
                    
                } catch (Exception e) {
                    //System.err.println("Error collection Garbage " + e.getMessage());
                    try {
						Thread.sleep(10000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
                }
            }
        });
		daemonThread.start();
    }
    
    static class WorkerEntry implements Comparable<WorkerEntry> {
        String address;
        String id;

        WorkerEntry(String addressArg, String idArg) {
          address = addressArg;
          id = idArg;
        }

        public int compareTo(WorkerEntry e) {
          return id.compareTo(e.id);
        }
      };

    synchronized static void downloadWorkers() {
    	logger.info("Downloading Workers");
    	
    	Thread daemonThread = new Thread(() -> {
            while (true) {
            
              String result;
				try {
					result = new String(HTTP.doRequest("GET", "http://"+masterAddr+"/workers", null).body());
					String[] pieces = result.split("\n");
		            int numWorkers = Integer.parseInt(pieces[0]);
		             workers.clear();
		             for (int i=0; i<numWorkers; i++) {
		                String[] pcs = pieces[1+i].split(",");
		                workers.add(new WorkerEntry(pcs[1], pcs[0]));
		             }
		             Collections.sort(workers);
		             replicationWorkers.clear();
		             
		             Integer NUM_WORKERS=2;
		             for(WorkerEntry w: workers) {
		            	 if(id.compareTo(w.id)<0 && NUM_WORKERS>0) {
		            		 NUM_WORKERS--;
		            		 replicationWorkers.add(w);
		            	 }
		             }
		             //wrap around 
		             if(replicationWorkers.size()<2) {
		            	 for(WorkerEntry w: workers) {
			            	 if(NUM_WORKERS>0&& id.compareTo(w.id)!=0) {
			            		 NUM_WORKERS--;
			            		 replicationWorkers.add(w);
			            	 }
			             }
		             }
		             for(WorkerEntry w: replicationWorkers) {
		            	 if(w.id.equals(id))
		            		 replicationWorkers.remove(w);
		             }
		             Thread.sleep(5000);
				} catch (Exception e) {
					e.printStackTrace();
				}
              
            }
        });
		daemonThread.start();

      }
    
    synchronized static void maintenance() {
    	logger.info("Replica Maintenance");
    	
    	Thread daemonThread = new Thread(() -> {
            while (true) {
              String result;
              Map<String, String> hashRow =  new ConcurrentHashMap<>();
				try {

		             Collections.sort(workers,Collections.reverseOrder());
		             
		             Integer NUM_WORKERS=2;
		             maintenanceWorkers.clear();
		             for(WorkerEntry w: workers) {
		            	 if(id.compareTo(w.id)!=0) {
		            		 continue;
		            	 }
		            	 if(NUM_WORKERS>0&&id.compareTo(w.id)!=0) {
		            		 maintenanceWorkers.add(w);
		            		 NUM_WORKERS--;
		            	 }
		            	 
		             }
		             //wrap around 
		             if(maintenanceWorkers.size()<2) {
		            	 for(WorkerEntry w: workers) {
			            	 if(NUM_WORKERS>0&&id.compareTo(w.id)!=0) {
			            		 NUM_WORKERS--;
			            		 maintenanceWorkers.add(w);
			            	 }
			             }
		             }
		             List<String> tableNames = new ArrayList<>();
		             for (WorkerEntry w: maintenanceWorkers) {
		            	 //get tables
		            	 result = new String(HTTP.doRequest("GET", "http://"+w.address+"/tables", null).body());
		            	 String[] pieces = result.split("\n");
		            	 int numTables=0;
				         numTables= pieces.length;
				         for (int i=0; i<numTables; i++) {
				                tableNames.add(pieces[i]);
				         }
				         //get hashed data
				         for (String t: tableNames) {
								result = new String(HTTP.doRequest("GET", "http://"+w.address+"/hashdata/"+t, null).body());
								logger.info("Hashed Data for table" + t);
								logger.info(result);
								
								String[] pieces2 = result.split("\n");
					            int numRows= pieces2.length;
					            hashRow.clear();
					            for (int i=0; i<numRows; i++) {
					            	String [] pcs = new String[0];
					            	pcs = pieces2[i].split(",");
					            	if(pcs.length==2) 
					                	hashRow.put(pcs[0], pcs[1]);
					            	
					            }
					            
					            for (Entry<String,String> entry : hashRow.entrySet()) {
					            	//get row and update
						        	 System.out.println("rowkey: "+entry.getKey());
						        	 System.out.println("rowkey hash value: "+entry.getValue());     	 
//					            	  try {
						            	 if(KVStore.get(t)!=null && KVStore.get(t).get(entry.getKey())!=null){
						            		 String inMemHash = String.valueOf(KVStore.get(t).get(entry.getKey()).toString().hashCode());
								        	 if(inMemHash.equals(entry.getValue())) {
								        		 logger.info("hashMatches");
								        		 continue;
								        	 }
								        	 byte[] newRow = HTTP.doRequest("GET", "http://"+w.address+"/data/"+t+"/"+entry.getKey(), null).body();
								        	 if(newRow!=null) 
								        	 {
								        		InputStream targetStream = new ByteArrayInputStream(newRow);
								        		String path = storageDir + File.separator + t + ".table";
//							                 	File f = new File(path);
							                 	Row row = Row.readFrom(targetStream);
									        	KVStore.get(t).put(entry.getKey(),row);
									        	
//							 	             	RandomAccessFile rf = new RandomAccessFile(f, "rw");
							 	             	FileOutputStream rf = new FileOutputStream(path, true);
							 	             
							 	             	synchronized(rf) {
							 	                 	rf.write(row.toByteArray());
							 	                 	rf.write("\n".getBytes(Charset.forName("UTF-8")));
							 	             	}
							 	             	rf.close();
								        	 }
						            	 }
						            	 else if(KVStore.get(t)==null) {
						            		 KVStore.put(t, new ConcurrentHashMap<>());
						            		 String path = storageDir + File.separator + t + ".table";
//						                 	 File f = new File(path);
//						                 	 if(!f.exists())
//						                 		f.createNewFile();
						                 	 
						                 	 byte[] newRow = HTTP.doRequest("GET", "http://"+w.address+"/data/"+t+"/"+entry.getKey(), null).body();
								        	 if(newRow!=null) 
									         {
									        	InputStream targetStream = new ByteArrayInputStream(newRow);
									        	Row row = Row.readFrom(targetStream);
									        	KVStore.get(t).put(entry.getKey(),row);
									        	
//							 	             	RandomAccessFile rf = new RandomAccessFile(f, "rw");
									        	FileOutputStream rf = new FileOutputStream(path, true);
							 	             	synchronized(rf) {
							 	                 	rf.write(row.toByteArray());
							 	                 	rf.write("\n".getBytes(Charset.forName("UTF-8")));
							 	             	}
							 	             	rf.close();
									         }
						            	 }
						            	 else if(KVStore.get(t).get(entry.getKey())==null) {
						            		 byte[] newRow = HTTP.doRequest("GET", "http://"+w.address+"/data/"+t+"/"+entry.getKey(), null).body();
						            		 if(newRow!=null) 
									         {
									        	InputStream targetStream = new ByteArrayInputStream(newRow);
									        	Row row = Row.readFrom(targetStream);
									        	KVStore.get(t).put(entry.getKey(),row);
									        	String path = storageDir + File.separator + t + ".table";
//							                 	File f = new File(path);
//							 	             	RandomAccessFile rf = new RandomAccessFile(f, "rw");
							                 	FileOutputStream rf = new FileOutputStream(path, true);
							 	             	synchronized(rf) {
							 	                 	rf.write(row.toByteArray());
							 	                 	rf.write("\n".getBytes(Charset.forName("UTF-8")));
							 	             	}
							 	             	rf.close();
									         }
						            	 }
//							        }catch (Exception e) {
//										e.printStackTrace();
//						            }
					            }
			             }
		             }
		             Thread.sleep(30000);    
				} catch (Exception e) {
					e.printStackTrace();
				}
            }
        });
		daemonThread.start();

      }

}


