package cis5550.webserver;
import java.net.*;
import java.net.URLDecoder;
import java.util.Map;
import java.util.HashMap;
import java.text.SimpleDateFormat;
import java.util.Date;
import cis5550.webserver.ThreadPool.Task;

import java.io.*;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ServerSocketFactory;
import javax.net.ssl.*;
import java.security.*;

public class Server {
	static Server server;
	static boolean flag;
	static Map<String, HashMap<String, Route>> routingTable;
	static Map<String, Session> sessions;
	private int port;
	private int securePort;
	String directory;
	private ThreadPool threadPool;
	public int busyThread = 0;
	public volatile boolean keep_running = true;

	public Server(int port) {
		server = null;
		flag = false;
		directory = null;
		this.port = port;
		routingTable = new HashMap<String, HashMap<String, Route>>();
		sessions = new ConcurrentHashMap<String, Session>();
		threadPool = new ThreadPool(10000);
	}

	public Server(int port, String directory) {
		server = null;
		flag = false;
		this.port = port;
		this.directory = directory;
		routingTable = new HashMap<String, HashMap<String, Route>>();
		threadPool = new ThreadPool(10000);
	}
	
	public class ServerTask implements Task {
		Connection conn;
		public ServerTask(Connection conn) {
			this.conn = conn;
		}
		public void run() {
			try {
				handleConnection(conn);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class staticFiles {
		public static void location(String s) {
			server.directory = s;
		}
	}
	
	private void send304(Connection conn) throws Exception {
		String msg = "HTTP/1.1 304 Not Modified";
		conn.respond(msg + "\r\n");
		conn.respond("Content-Type: text/plain\r\n");
		conn.respond("Server: XXX\r\n");
		conn.respond("Content-Length: " + String.valueOf(msg.length() + 1) + "\r\n\r\n");
		conn.respond(msg + "\n");
	}

	private void send400(Connection conn) throws Exception {
		String msg = "HTTP/1.1 400 Bad Request";
		conn.respond(msg + "\r\n");
		conn.respond("Content-Type: text/plain\r\n");
		conn.respond("Server: XXX\r\n");
		conn.respond("Content-Length: " + String.valueOf(msg.length() + 1) + "\r\n\r\n");
		conn.respond(msg + "\n");
	}

	private void send403(Connection conn) throws Exception {
		String msg = "HTTP/1.1 403 Forbidden";
		conn.respond(msg + "\r\n");
		conn.respond("Content-Type: text/plain\r\n");
		conn.respond("Server: XXX\r\n");
		conn.respond("Content-Length: " + String.valueOf(msg.length() + 1) + "\r\n\r\n");
		conn.respond(msg + "\n");
	}


	private void send404(Connection conn) throws Exception {
		String msg = "HTTP/1.1 404 Not Found";
		conn.respond(msg + "\r\n");
		conn.respond("Content-Type: text/plain\r\n");
		conn.respond("Server: XXX\r\n");
		conn.respond("Content-Length: " + String.valueOf(msg.length() + 1) + "\r\n\r\n");
		conn.respond(msg + "\n");
	}

	private void send405(Connection conn) throws Exception {
		String msg = "HTTP/1.1 405 Not allowed";
		conn.respond(msg + "\r\n");
		conn.respond("Content-Type: text/plain\r\n");
		conn.respond("Server: XXX\r\n");
		conn.respond("Content-Length: " + String.valueOf(msg.length() + 1) + "\r\n\r\n");
		conn.respond(msg + "\n");
	}
	
	private void send500(Connection conn) throws Exception {
		String msg = "HTTP/1.1 500 Internal Server Error";
		conn.respond(msg + "\r\n");
		conn.respond("Content-Type: text/plain\r\n");
		conn.respond("Server: XXX\r\n");
		conn.respond("Content-Length: " + String.valueOf(msg.length() + 1) + "\r\n\r\n");
		conn.respond(msg + "\n");
	}


	private static void send501(Connection conn) throws Exception {
		String msg = "HTTP/1.1 501 Not Implemented";
		conn.respond(msg + "\r\n");
		conn.respond("Content-Type: text/plain\r\n");
		conn.respond("Server: XXX\r\n");
		conn.respond("Content-Length: " + String.valueOf(msg.length() + 1) + "\r\n\r\n");
		conn.respond(msg + "\n");
	}

	private static void send505(Connection conn) throws Exception {
		String msg = "HTTP/1.1 505 HTTP Version Not Supported";
		conn.respond(msg + "\r\n");
		conn.respond("Content-Type: text/plain\r\n");
		conn.respond("Server: XXX\r\n");
		conn.respond("Content-Length: " + String.valueOf(msg.length() + 1) + "\r\n\r\n");
		conn.respond(msg + "\n");
	}
	
	private void noRoute(String[] method, String header, Connection conn) throws Exception {
		if (method[0].toLowerCase().equals("get")) {
			handleGet(method, header, conn);
		} else if (method[0].toLowerCase().equals("head")) {
			handleHead(method, conn);
		}
	}
	
	private static Map<String, String> collectHeaders(String[] lines) {
		Map<String, String> reqHeaders = new HashMap<String, String>();
		for (int i = 1; i < lines.length; i++) {
			String[] name_value = lines[i].split(":");
			String name = name_value[0].trim().toLowerCase();
			String value = name_value[1].trim();
			reqHeaders.put(name, value);
		}
		return reqHeaders;
	}
	
	private static Route collectPathParameters(Map<String, String> pathParam, Map<String, Route> paths, String url) throws UnsupportedEncodingException {
		Route ret = null;
		int q = url.indexOf("?");
		if (q >= 0) {
			url = url.substring(0, q);
		}
		for (String pat : paths.keySet()) {
			boolean match = true;
			String[] patSplit = pat.split("/");
			String[] urlSplit = url.split("/");
			if (patSplit.length != urlSplit.length) {
				continue;
			}
			for (int i = 0; i < patSplit.length; i++) {
				if (patSplit[i].equals(urlSplit[i])) {
					continue;
				}
				if (patSplit[i].startsWith(":")) {
					pathParam.put(URLDecoder.decode(patSplit[i].substring(1), "UTF-8"), URLDecoder.decode(urlSplit[i], "UTF-8"));
					continue;
				}
				match = false;
				break;
			}
			if (match) {
				ret = paths.get(pat);
				break;
			} 
			pathParam.clear();
		}
		return ret;
	}
	
	private static void collectQueryParametersFromHeader(Map<String, String> queryParams, Map<String, Route> paths, String url) throws UnsupportedEncodingException {
		int questionMark = url.indexOf("?");
		if (questionMark == -1) {
			return;
		}
		String[] queries = url.substring(questionMark + 1).split("&");
		for (String pair : queries) {
			String[] kv = pair.split("=");
			if (kv.length == 1) {
				queryParams.put(URLDecoder.decode(kv[0], "UTF-8"), "");
			} else {
				queryParams.put(URLDecoder.decode(kv[0], "UTF-8"), URLDecoder.decode(kv[1], "UTF-8"));
			}
		}
		return;
	}
	
	private static void collectQueryParametersFromBody(Map<String, String> queryParams, Map<String, String> reqHeaders, byte[] body) throws UnsupportedEncodingException {
		if (!reqHeaders.containsKey("content-type")) {
			return;
		}
		if (!reqHeaders.get("content-type").equals("application/x-www-form-urlencoded")) {
			return;
		}
		String bodyString = new String(body);
		String[] queries = bodyString.split("&");
		for (String pair : queries) {
			String[] kv = pair.split("=");
			queryParams.put(URLDecoder.decode(kv[0], "UTF-8"), URLDecoder.decode(kv[1], "UTF-8"));
		}
		return;
	}
	
	private Session getSessionFromHeader(Map<String, String> reqHeaders) {
		if (!reqHeaders.containsKey("cookie")) {
			return null;
		}
		String[] cookies = reqHeaders.get("cookie").split("; ");
		for (String cookie : cookies) {
			String[] kv = cookie.split("=");
			if (!kv[0].equals("SessionID")) {
				continue;
			}
			if (!sessions.containsKey(kv[1])) {
				continue;
			}
			SessionImpl sess = (SessionImpl) sessions.get(kv[1]);
			if (System.currentTimeMillis() - sess.lastAccessedTime() > sess.maxActiveInterval * 1000) {
				continue;
			}
			sess.lastAccessedTime = System.currentTimeMillis();
			return sess;
		}
		return null;
	}

	private void handleHeader(String header, Connection conn) throws Exception {
		String[] lines = header.split("\r\n");
		Map<String, String> reqHeaders = collectHeaders(lines);
		String[] method = lines[0].split(" ");
		if (method.length < 3) {
			send400(conn);
			return;
		}
		if (!method[0].toLowerCase().equals("get") && !method[0].toLowerCase().equals("put") && !method[0].toLowerCase().equals("post") && !method[0].toLowerCase().equals("head")) {
			send501(conn);
			return;
		}
		if (!method[2].toLowerCase().equals("http/1.1")) {
			send505(conn);
			return;
		}
		// If there is a content length, read the content.
		byte[] body_bytes = null;
		String contentLength = reqHeaders.get("content-length");
		if (contentLength != null) {
			int len = Integer.parseInt(contentLength);
			body_bytes = conn.readLength(len);
		}
		if (!routingTable.containsKey(method[0].toUpperCase())) {
			noRoute(method, header, conn);
			return;
		}
		Map<String, Route> paths = routingTable.get(method[0].toUpperCase());
		Map<String, String> pathParams = new HashMap<String, String>();
		Map<String, String> queryParams = new HashMap<String, String>();
		Route route = collectPathParameters(pathParams, paths, method[1]);
		collectQueryParametersFromHeader(queryParams, paths, method[1]);
		collectQueryParametersFromBody(queryParams, reqHeaders, body_bytes);
		if (route == null) {
			noRoute(method, header, conn);
			return;
		}
		Session sess = getSessionFromHeader(reqHeaders);
		RequestImpl req = new RequestImpl(method[0], method[1], method[2], reqHeaders, queryParams, pathParams, (InetSocketAddress) conn.getRmoteSocketAddress(), body_bytes, this, sess);
		ResponseImpl res = new ResponseImpl(conn);
		try {
			Object ret = route.handle(req, res);
			if (req.newSession) {
				sessions.put(req.session.id(), req.session);
				res.header("Set-Cookie", "SessionID=" + req.session.id());
			}
			if (res.written) {
				conn.close();
				return;
			}
			if (ret != null) {
				String body = ret.toString();
				res.header("Content-Length", String.valueOf(body.length()));
				conn.respond("HTTP/1.1 " + String.valueOf(res.statusCode) + " " + res.reasonPhrase + "\r\n");
				for (String headerName : res.headers.keySet()) {
					conn.respond(headerName + ": " + res.headers.get(headerName) + "\r\n");
				}
				conn.respond("\r\n");
				conn.respond(body);
				return;
			}
			if (res.body != null) {
				conn.respond("HTTP/1.1 " + String.valueOf(res.statusCode) + " " + res.reasonPhrase + "\r\n");
				res.header("Content-Length", String.valueOf(res.body.length));
				for (String headerName : res.headers.keySet()) {
					conn.respond(headerName + ": " + res.headers.get(headerName) + "\r\n");
				}
				conn.respond("\r\n");
				conn.send(res.body, res.body.length);
				return;
			}
			conn.respond("HTTP/1.1 " + String.valueOf(res.statusCode) + " " + res.reasonPhrase + "\r\n");
			for (String headerName : res.headers.keySet()) {
				conn.respond(headerName + ": " + res.headers.get(headerName) + "\r\n");
			}
			conn.respond("\r\n");
		} catch (Exception e) {
			e.printStackTrace();
			if (!res.written) {
				send500(conn);
				return;
			}
			conn.close();
		}
	}

	private void sendFile(File file, Connection conn) throws Exception {
		InputStream fis = new FileInputStream(file);
		byte[] content = new byte[4096];
		int n = fis.read(content);
		while (n >= 0) {
			conn.send(content, n);
			n = fis.read(content);
		}
		fis.close();
	}
	
	private boolean sendFileRange(File file, Connection conn, long start, long end) throws Exception {
		if (end < start) {
			return false;
		}
		RandomAccessFile raf = new RandomAccessFile(file, "r");
		long size = end - start + 1;
		int num_batch = (int) size / 4096;
		for (int i = 0; i < num_batch; i++) {
			byte[] buf = new byte[4096];
			raf.readFully(buf, (int) start + 4096 * i, 4096);
			conn.send(buf, 4096);
		}
		int last_batch = (int) size % 4096;
		byte[] last_buf = new byte[last_batch];
		raf.readFully(last_buf, (int) start + 4096 * num_batch, last_batch);
		raf.close();
		return true;
	}
	
	public String getContentType(String uri) {
		int i = uri.indexOf(".");
		if (i == -1) {
			return "application/octet-stream";
		}
		String ext = uri.substring(i);
		if (ext.equals(".txt")) {
			return "text/plain";
		}
		if (ext.equals(".html")) {
			return "text/html";
		}
		if (ext.equals(".jpg") || ext.equals(".jpeg")) {
			return "image/jpeg";
		}
		return "application/octet-stream";
	}

	private void handleGet(String[] method, String header, Connection conn) throws Exception {
		if (method[1].contains("..")) {
			send403(conn);
			return;
		}
		if (directory == null) {
			send404(conn);
			return;
		}
		String path = this.directory + "/" + method[1];
		File target = new File(path);
		if (!target.exists()) {
			send404(conn);
			return;
		}
		int if_mod = header.toLowerCase().indexOf("if-modified-since:");
		if (if_mod != -1) {
			Scanner scnr = new Scanner(header.substring(if_mod + "if-modified-since:".length()));
			String date = scnr.nextLine().toLowerCase().trim();
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
			Date request_time = simpleDateFormat.parse(date);
			Date file_time = new Date(target.lastModified());
			if (file_time.before(request_time)) {
				send304(conn);
				return;
			}
		}
		long size = target.length();
		int range = header.toLowerCase().indexOf("range:");
		if (range == -1) {
			String type = getContentType(method[1]);
			conn.respond("HTTP/1.1 200 OK\r\n");
			conn.respond("Content-Type: " + type + "\r\n");
			conn.respond("Server: XXX\r\n");
			conn.respond("Content-Length: " + String.valueOf(size) + "\r\n\r\n");
			sendFile(target, conn);
		} else {
			Scanner scnr = new Scanner(header.substring(range + "range:".length()));
			String byte_range = scnr.nextLine().toLowerCase().trim();
			scnr.close();
			int equal_sign = byte_range.indexOf("=");
			if (equal_sign == -1) {
				send400(conn);
				return;
			}
			String range_num = byte_range.substring(equal_sign + 1).trim();
			String[] start_end = range_num.split("-");
			long start = Long.parseLong(start_end[0].trim());
			long end = Long.parseLong(start_end[1].trim());
			sendFileRange(target, conn, start, end);
		}
	}

	private void handleHead(String[] method, Connection conn) throws Exception {
		if (method[1].contains("..")) {
			send403(conn);
			return;
		}
		if (directory == null) {
			send404(conn);
			return;
		}
		String path = this.directory + "/" + method[1];
		File target = new File(path);
		if (!target.exists()) {
			send404(conn);
			return;
		}
		long size = target.length();
		String type = getContentType(method[1]);
		conn.respond("HTTP/1.1 200 OK\r\n");
		conn.respond("Content-Type: " + type + "\r\n");
		conn.respond("Server: XXX\r\n");
		conn.respond("Content-Length: " + String.valueOf(size) + "\r\n\r\n");
	}

	private void handleConnection(Connection conn) throws Exception {
		while (true) {
			byte[] header_bytes = conn.readUntilDoubleCRLF();
			if (header_bytes == null) {
				break;
			}
			String header = new String(header_bytes);
			handleHeader(header, conn);
		}
		conn.close();

	}
	
	public static void get(String s, Route r) throws Exception {
		if (server == null) {
			server = new Server(80);
		}
		if (!flag) {
			flag = true;
			Thread t = new Thread(() -> {
				try {
					server.run();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			t.start();
		}
		if (!routingTable.containsKey("GET")) {
			routingTable.put("GET", new HashMap<String, Route>());
		}
		routingTable.get("GET").put(s, r);
		
	}
	
	public static void post(String s, Route r) {
		if (server == null) {
			server = new Server(80);
		}
		if (!flag) {
			flag = true;
			Thread t = new Thread(() -> {
				try {
					server.run();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			t.start();
		}
		if (!routingTable.containsKey("POST")) {
			routingTable.put("POST", new HashMap<String, Route>());
		}
		routingTable.get("POST").put(s, r);
		
	}
	
	public static void put(String s, Route r) {
		if (server == null) {
			server = new Server(80);
		}
		if (!flag) {
			flag = true;
			Thread t = new Thread(() -> {
				try {
					server.run();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			t.start();
		}
		if (!routingTable.containsKey("PUT")) {
			routingTable.put("PUT", new HashMap<String, Route>());
		}
		routingTable.get("PUT").put(s, r);
	}
	
	public static void port(int port) {
		if (server == null) {
			server = new Server(port);
		}
	}
	
	public static void securePort(int securePort) {
		if (server == null) {
			server = new Server(80);
		}
		server.securePort = securePort;
	}
	
	private void serverLoop(ServerSocket listenSocket) throws Exception {
		while (keep_running) {
			try {
				Socket connSocket = listenSocket.accept();
				Connection conn = new Connection(connSocket);
				Task t = new ServerTask(conn);
				threadPool.dispatch(t);
			} catch (Exception e) {

			}
		}
	}
	
	private void expireSessions() throws InterruptedException {
		Thread.sleep(3);
		for (String key : sessions.keySet()) {
			SessionImpl sess = (SessionImpl) sessions.get(key);
			if (System.currentTimeMillis() - sess.lastAccessedTime() > sess.maxActiveInterval * 1000) {
				sessions.remove(key);
			}
		}
		
	}

	public void run() throws Exception {
		// Prepare for control-c shutdown.
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				threadPool.killWorkers();
				keep_running = false;
			}
		});
		
		threadPool.spawnWorkers(100);
		ServerSocket httpSock = new ServerSocket(port);
		Thread listenHttp = new Thread() {
			public void run() {
				try {
					serverLoop(httpSock);
					httpSock.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
//		String pwd = "secret";
//		KeyStore keyStore = KeyStore.getInstance("JKS");
//		keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
//		KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
//		keyManagerFactory.init(keyStore, pwd.toCharArray());
//		SSLContext sslContext = SSLContext.getInstance("TLS");
//		sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
//		ServerSocketFactory factory = sslContext.getServerSocketFactory();
//		ServerSocket serverSocketTLS = factory.createServerSocket(securePort);
//		Thread listenHttps = new Thread() {
//			@Override
//			public void run() {
//				try {
//					serverLoop(serverSocketTLS);
//					serverSocketTLS.close();
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			}
//		};
		Thread sessionDaemon = new Thread() {
			public void run() {
				try {
					expireSessions();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};
		listenHttp.start();
		// listenHttps.start();
		sessionDaemon.start();
	}
}

