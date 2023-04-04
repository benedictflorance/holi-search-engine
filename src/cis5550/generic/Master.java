package cis5550.generic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.tools.Logger;

import static cis5550.webserver.Server.*;

public class Master {
	
	private static final Logger logger= Logger.getLogger(Master.class);
	public static Map<String,Worker> activeWorkers = new ConcurrentHashMap<String, Worker> ();
	
	public static Vector<String> getWorkers(){
		Vector<String> result = new Vector<String>();
		for (Map.Entry<String,Worker> entry : activeWorkers.entrySet()) {
			result.add(entry.getValue().getIp()+ ":"+ entry.getValue().getPort());
		}
		return result;
	}
	
	public static String workerTable() {
		StringBuilder html = new StringBuilder();
        html.append("<html><head><title>KVS Master</title></head><body>");
        html.append("<h1>KVS Master</h1>");
        html.append("<table><thead><tr><th>ID</th><th>IP</th><th>Port</th><th>Link</th></tr></thead><tbody>");
        for (Map.Entry<String,Worker> entry : activeWorkers.entrySet()) {
            html.append("<tr>");
            html.append("<td><a href=\"http://").append(entry.getValue().getIp()).append(":").append(entry.getValue().getPort()).append("/\">").append(entry.getKey()).append("</a></td>");
            html.append("<td><a href=\"http://").append(entry.getValue().getIp()).append(":").append(entry.getValue().getPort()).append("/\">").append(entry.getValue().getIp()).append("</a></td>");
            html.append("<td><a href=\"http://").append(entry.getValue().getIp()).append(":").append(entry.getValue().getPort()).append("/\">").append(entry.getValue().getPort()).append("</a></td>");
            html.append("<td><a href=\"http://").append(entry.getValue().getIp()).append(":").append(entry.getValue().getPort()).append("/\">").append("http://").append(entry.getValue().getIp()).append(":").append(entry.getValue().getPort()).append("</a></td>");
            html.append("</tr>");
        }
        html.append("</tbody></table></body></html>");
        return html.toString();
	}
	
	public static void registerRoutes() throws Exception {
		
		get("/ping", (req, res) ->{ 
			if(req.queryParams("id")==null || req.queryParams("port")==null) {
				res.status(400, "Bad Request");
				return "400";
			}
			
			if(activeWorkers.get(req.queryParams("id"))!=null) {
				Worker worker = activeWorkers.get(req.queryParams("id"));
				worker.setPort(req.queryParams("port"));
				worker.setIp(req.ip());
				worker.setLastAccessedTime(System.currentTimeMillis());
				activeWorkers.replace(req.queryParams("id"), worker); 
				
			}
			else {
				Worker worker = new Worker();
				worker.setPort(req.queryParams("port"));
				worker.setIp(req.ip());
				worker.setLastAccessedTime(System.currentTimeMillis());
				activeWorkers.put(req.queryParams("id"), worker); 
			}
				
			res.status(200, "OK");
			res.body("OK");
			return "OK";
			});
		
			
		get("/workers", (req, res) ->{ 
			res.status(200, "OK");
			return convertWorkerstoString();
			});
	}
	
	public static String convertWorkerstoString() {
		String result = "";
		result += activeWorkers.size()+"\n";
		for (Map.Entry<String,Worker> entry : activeWorkers.entrySet()) 
            result+=entry.getKey()+","+ entry.getValue().getIp()+ ":"+ entry.getValue().getPort()+"\n";
		
		return result;
	}
}


