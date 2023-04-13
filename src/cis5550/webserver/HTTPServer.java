package cis5550.webserver;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

class HTTPServer implements Runnable{
    public Socket sock = null;
    Server server;
    boolean connClose = false;
    boolean secure = false;
    public HTTPServer(Socket sock, Server server, boolean secureArg) {
        this.sock = sock;
        this.server = server;
        this.connClose = false;
        this.secure = secureArg;
    }
    public static HashMap<Integer, String> error_descriptions;
    public static HashMap<String, String> extension_types;
    public static String [] allowed_methods = {"GET", "HEAD"};
    public static String [] implemented_methods = {"GET", "HEAD", "POST", "PUT"};
    public static void sendError(PrintWriter out, int error_code)
    {
        sendError(out, error_code, error_descriptions.get(error_code));
    }
    public static void sendError(PrintWriter out, int error_code, String reasonPhrase)
    {
        out.print("HTTP/1.1 " + Integer.toString(error_code) + " " + reasonPhrase + " \r\n");
        out.print("Server: localhost " + "\r\n");
        out.flush();
        if(error_code != 304)
        {
            out.print("Content-Length: " + reasonPhrase.length() + "\r\n");
            out.flush();
            out.print("Content-Type: text/plain\r\n");
            out.flush();
        }
        out.print("\r\n");
        out.flush();
        if(error_code != 304) {
            out.print(reasonPhrase);
            out.flush();
        }
    }
    public static boolean isModified(long last_modified_serv, String last_modified_req_str)
    {
        DateTimeFormatter formatter = DateTimeFormatter.RFC_1123_DATE_TIME;
        OffsetDateTime odt = OffsetDateTime.parse(last_modified_req_str, formatter);
        long last_modified_req = odt.toInstant().toEpochMilli();
        if(last_modified_req < last_modified_serv)
            return true;
        return false;
    }
    public static void sendFileResponse(PrintWriter out, OutputStream stream, Path path, HTTPRequest r) {
        try
        {
            String path_str = path.toString();
            File file = new File(path_str);
            long last_modified_serv = file.lastModified(); // GMT: Wed, January 25, 2023 3:09:45.200 AM
            if((r.headers.get("if-modified-since") == null) || isModified(last_modified_serv, r.headers.get("if-modified-since")))
            {
                if(!file.canRead())
                {
                    sendError(out, 403);
                }
                else {
                    byte[] fileContents;
                    Instant instant = Instant.ofEpochMilli(last_modified_serv);
                    OffsetDateTime odt = instant.atOffset(ZoneOffset.UTC);
                    String last_modified_serv_str = odt.format (DateTimeFormatter.RFC_1123_DATE_TIME);
                    long file_size = Files.size(path);
                    if(r.headers.get("range") == null)
                    {
//                    	for (File f: file.listFiles()) {
//                    		System.out.println(f.getName());
//                    	}
//                    	System.out.println(file.listFiles());
//                    	System.out.println(Files.isReadable(path));
                    	
                    	if (r.requestURI.equals("/")) {
                    		Path filePath = Paths.get(path_str + "/index.html");
                    		fileContents = Files.readAllBytes(filePath);
                    	} else {
                            fileContents =  Files.readAllBytes(path);
                    	}
                        out.print("HTTP/1.1 200 OK" + "\r\n");
                    }
                    else
                    {
                        long start, end;
                        String range = r.headers.get("range").substring(6);
                        if(range.charAt(0) == '-')
                        {
                            start = file_size - Integer.parseInt(range.substring(1));
                            end = file_size - 1;
                        } else if (range.charAt(range.length() - 1) == '-') {
                            start = Integer.parseInt(range.substring(0, range.length()-1));
                            end = file_size - 1;
                        }
                        else
                        {
                            String[] splitted_range = range.split("-");
                            start = Integer.parseInt(splitted_range[0]);
                            end = Integer.parseInt(splitted_range[1]);
                        }
                        fileContents = new byte[(int) (end - start + 1)];
                        RandomAccessFile raf = new RandomAccessFile(path_str, "r");
                        raf.seek(start);
                        raf.read(fileContents, 0, (int) (end - start + 1));
                        out.print("HTTP/1.1 206 " + error_descriptions.get(206) + "\r\n");
                        out.print("Content-Range: bytes " + Long.toString(start) + "-" + Long.toString(end) + "/" + Long.toString(end + 1));
                    }
                    String extension = path_str.substring(path_str.lastIndexOf(".") + 1);
                    if (r.requestURI.equals("/"))
                    	extension = "html";
                    String type = extension_types.get(extension) == null ? "application/octet-stream" : extension_types.get(extension);
                    
                    out.print("Content-Type: " + type + "\r\n");
                    out.print("Server: localhost " + "\r\n");
                    out.flush();
                    out.print("Content-Length: " + Integer.toString(fileContents.length) + "\r\n");
                    out.flush();
                    out.print("Last-Modified: " + last_modified_serv_str + "\r\n");
                    out.flush();
                    out.print("\r\n");
                    out.flush();
                    if(!r.method.equals("HEAD")) {
                        stream.write(fileContents);
                        stream.flush();
                    }
                }
            }
            else
            {
                sendError(out, 304);
            }
        } catch (IOException ioe) {
        	ioe.printStackTrace();
            sendError(out, 500);
        }
    }
    public void sendRouteResponse(ResponseImpl res, PrintWriter out, OutputStream stream) throws IOException {
        boolean contentTypePresent = false;
        out.print("HTTP/1.1 " + res.statusCode + " " + res.reasonPhrase + "\r\n");
        // System.out.println("HTTP/1.1 " + res.statusCode + " " + res.reasonPhrase);
        out.flush();
        for(Map.Entry<String, List<String>> entry : res.headers.entrySet())
        {
            for(int i = 0; i < entry.getValue().size(); i++)
            {
                if(entry.getKey().toLowerCase().equals("content-type"))
                    contentTypePresent = true;
                out.print(entry.getKey() + ": " + entry.getValue().get(i) + "\r\n");
                // System.out.println(entry.getKey() + ": " + entry.getValue().get(i));
                out.flush();
            }
        }
        if(res.body != null)
        {
            out.print("Content-Length: " + Integer.toString(res.body.length) + "\r\n");
            // System.out.println("Content-Length: " + Integer.toString(res.body.length));
            out.flush();
        }
        if(!contentTypePresent)
        {
            out.print("Content-Type: text/html" + "\r\n");
            // System.out.println("Content-Type: text/html");
            out.flush();
        }
        out.print("\r\n");
        out.flush();
        if(res.body != null)
        {
            stream.write(res.body);
            // System.out.println(res.body);
            stream.flush();
        }
    }
    public class MalformedRequestException extends Exception {
		public MalformedRequestException(String errorMessage) {
            super(errorMessage);
        }
    }
    public boolean routeExists(HTTPRequest r, PrintWriter out, OutputStream stream) throws IOException, MalformedRequestException {
        if(server.routes == null) {
            return false;
        }
        Map<String, String> query_params =  new HashMap<>();
        if(r.requestURI.contains("?"))
        {
            String[] ampersand_split = r.requestURI.split("\\?", 2);
            r.requestURI = ampersand_split[0];
            String[] query_split = ampersand_split[1].split("&");
            for(int i = 0; i < query_split.length; i++)
            {
                if(query_split[i].contains("="))
                {
                    String[] equal_split = query_split[i].split("=", 2);
                    query_params.put(java.net.URLDecoder.decode(equal_split[0], StandardCharsets.UTF_8),
                            java.net.URLDecoder.decode(equal_split[1], StandardCharsets.UTF_8
                            ));
                }
                else
                {
                    throw new MalformedRequestException("No = present in query params!");
                }
            }
        }
        if(r.headers.get("content-type") != null && r.headers.get("content-type").equals("application/x-www-form-urlencoded"))
        {
            String[] query_split = r.body().split("&");
            for(int i = 0; i < query_split.length; i++)
            {
                if(query_split[i].contains("="))
                {
                    String[] equal_split = query_split[i].split("=", 2);
                    query_params.put(java.net.URLDecoder.decode(equal_split[0], StandardCharsets.UTF_8),
                            java.net.URLDecoder.decode(equal_split[1], StandardCharsets.UTF_8
                            ));
                }
                else
                {
                    throw new MalformedRequestException("No = present in query params!");
                }
            }
        }
        String host_name = r.headers.get("host");
        for(int i = 0; i < server.routes.size(); i++)
        {
            if(server.routes.get(i).host.equals(host_name))
            {
                boolean isPathMatching = true;
                String[] url_split = r.requestURI.split("/");
                String[] path_split = server.routes.get(i).path_pattern.split("/");
                Map<String, String> params =  new HashMap<>();
                if(url_split.length == path_split.length)
                {
                    for(int j = 0; j < url_split.length; j++)
                    {
                        if(!url_split[j].equals(path_split[j]) && path_split[j].charAt(0) != ':')
                        {
                            isPathMatching = false;
                            break;
                        }
                        else if(path_split[j].length() > 1 && path_split[j].charAt(0) == ':')
                        {
                            params.put(path_split[j].substring(1), java.net.URLDecoder.decode(url_split[j], StandardCharsets.UTF_8));
                        }
                    }
                }
                else
                {
                    isPathMatching = false;
                }
                if(server.routes.get(i).method.equals(r.method) &&
                        isPathMatching)
                {
                    InetSocketAddress addr = (InetSocketAddress) sock.getRemoteSocketAddress();
                    RequestImpl req = new RequestImpl(r.method, r.requestURI, r.protocol, r.headers, query_params, params, addr, r.body, server, r.cookies);
                    ResponseImpl res = new ResponseImpl(req, secure);
                    res.setClientSocket(sock);
                    try {
                        boolean halt = false;
                        if(server.before_filters.get(host_name) != null)
                        {
                            for (int k = 0; k < server.before_filters.get(host_name).size(); k++) {
                                server.before_filters.get(host_name).get(k).handle(req, res);
                                if(res.headers.get("halt-called") != null && res.headers.get("halt-called").equals("true"))
                                {
                                    halt = true;
                                    break;
                                }
                            }
                        }
                        res.before_stage = false;
                        if(!halt)
                        {
                            Object response = server.routes.get(i).route.handle(req, res);
                            if(req.newSession) {
                                String cookie_val = "SessionID=" + req.session().id() + "; SameSite=Strict; HttpOnly";
                                if(secure)
                                    cookie_val += "; Secure";
                                res.header("Set-Cookie", cookie_val);
                            }
                            if(server.after_filters.get(host_name) != null)
                            {
                                for (int k = 0; k < server.after_filters.get(host_name).size(); k++) {
                                    server.after_filters.get(host_name).get(k).handle(req, res);
                                }
                            }
                            if(res.writeCalled)
                            {
                                connClose = true;
                                return true;
                            }
                            else if(response != null)
                            {
                                res.body = response.toString().getBytes();
                            }
                            sendRouteResponse(res, out, stream);
                        }
                        else
                        {
                            sendError(out, res.statusCode, res.reasonPhrase);
                        }
                    } catch (Exception e) {

                        if(!res.writeCalled)
                        {
                        	e.printStackTrace();
                            sendError(out, 500);
                        }
                        else
                        {
                            connClose = true;
                            return true;
                        }
                    }
                    return true;
                }
            }
        }
        return false;
    }
    @Override
    public void run() {
        try {
            error_descriptions = new HashMap<Integer, String>();
            error_descriptions.put(206, "Partial Content");
            error_descriptions.put(304, "Not Modified");
            error_descriptions.put(400, "Bad Request");
            error_descriptions.put(403, "Forbidden");
            error_descriptions.put(404, "Not Found");
            error_descriptions.put(405, "Method Not Allowed");
            error_descriptions.put(500, "Internal Server Error");
            error_descriptions.put(501, "Not Implemented");
            error_descriptions.put(505, "HTTP Version not supported");
            extension_types = new HashMap<String, String>();
            extension_types.put("css", "text/css");
            extension_types.put("js", "application/javascript");
            extension_types.put("svg", "image/svg+xml");
            extension_types.put("jpg", "image/jpeg");
            extension_types.put("jpeg", "image/jpeg");
            extension_types.put("png", "image/png");
            extension_types.put("txt", "text/plain");
            extension_types.put("html", "text/html");
            InputStream in = this.sock.getInputStream();
            OutputStream stream_out = this.sock.getOutputStream();
            PrintWriter out = new PrintWriter(stream_out);
            while (true) {
                HTTPRequest r = new HTTPRequest();
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                ByteArrayOutputStream entire = new ByteArrayOutputStream();
                int matchPtr = 0, numBytesRead = 0;
                boolean endOfReq = false, processNextReq = false;
                while (matchPtr < 4) {
                    int b = 0;
                    b = in.read();
                    if (b >= 0)
                        numBytesRead++;
                    if (b < 0) {
                        if (numBytesRead != 0) {
                            sendError(out, 400);
                            processNextReq = true;
                        }
                        else {
                            endOfReq = true;
                        }
                        break;
                    }
                    buffer.write(b);
                    entire.write(b);
                    if ((((matchPtr == 0) || (matchPtr == 2)) && (b == '\r')) || (((matchPtr == 1) || (matchPtr == 3)) && (b == '\n')))
                        matchPtr++;
                    else
                        matchPtr = 0;
                }
                if (endOfReq) {
                    break;
                }
                else if(processNextReq) {
                    continue;
                }
                BufferedReader hdr = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer.toByteArray())));
                String requestLine = hdr.readLine();
                // System.out.println(requestLine);
                String[] p = requestLine.split(" ");
                if (p.length < 3) {
                    sendError(out, 400);
                    continue;
                }
                if (!p[2].equals("HTTP/1.1")) {
                    sendError(out, 505);
                    continue;
                }
                if (!Arrays.asList(implemented_methods).contains(p[0])) {
                    sendError(out, 501);
                    continue;
                }
                if(p[1].contains("..")) {
                    sendError(out, 403);
                    continue;
                }

                r.method = p[0];
                r.requestURI = p[1];
                r.protocol = p[2];
                boolean malformed_header = false;
                while (true) {
                    String l = hdr.readLine();
                    if (l.equals(""))
                        break;
                    // System.out.println(l);
                    String[] p2 = l.split(":", 2);
                    if (p2.length == 2) {
                        r.headers.put(p2[0].toLowerCase().trim(), p2[1].trim());
                    } else {
                        malformed_header = true;
                        break;
                    }
                }
                if (r.headers.get("host") == null || malformed_header) {
                    sendError(out, 400);
                    continue;
                }
                if(r.headers.get("host").contains(":"))
                {
                    r.headers.put("host", r.headers.get("host").split(":")[0]);
                }
                if(!server.named_hosts.contains(r.headers.get("host"))) {
                    r.headers.put("host", "*");
                }
                ByteArrayOutputStream body = new ByteArrayOutputStream();
                String cl = r.headers.get("content-length");
                int bodyBytesReceived = 0;
                if (cl != null) {
                    boolean messageIncomplete = false;
                    for (int i = 0; i < Integer.valueOf(cl).intValue(); i++) {
                        int b = 0;
                        b = in.read();
                        if (b < 0) {
                            messageIncomplete = true;
                            break;
                        }
                        body.write(b);
                        entire.write(b);
                    }
                    if (messageIncomplete) {
                        sendError(out, 400);
                        continue;
                    }
                }
                r.body = body.toByteArray();
                r.request = entire.toByteArray();
                if(r.requestURI.charAt(0) != '/')
                    r.requestURI = "/" + r.requestURI;
                if(r.headers.get("cookie") != null)
                {
                    String[] cookie_split = r.headers.get("cookie").split("; ");
                    for(int i = 0; i < cookie_split.length; i++)
                    {
                        if(cookie_split[i].contains("="))
                        {
                            String[] equal_split = cookie_split[i].split("=", 2);
                            r.cookies.put(equal_split[0], equal_split[1]);
                        }
                    }
                }
                if(r.cookies.get("SessionID") != null)
                {
                    String sessionID = r.cookies.get("SessionID");
                    long current_time = System.currentTimeMillis();
                    if(server.sessionMap.containsKey(sessionID))
                    {
                        if(current_time - server.sessionMap.get(sessionID).lastAccessedTime() < 5000
                                && current_time - server.sessionMap.get(sessionID).creationTime() < Server.sessionMap.get(sessionID).getMaxActiveInterval() * 1000)
                            server.sessionMap.get(sessionID).setLastAccessedTime(current_time);
                        else
                            server.sessionMap.remove(sessionID);
                    }

                }
                try {
                    if(!routeExists(r, out, stream_out))
                    {
                        if (Arrays.asList(implemented_methods).contains(p[0]) &&
                                !Arrays.asList(allowed_methods).contains(p[0])) {
                            sendError(out, 405);
                        }
                        else if(server.locations.get(r.headers.get("host")) == null)
                        {
                            sendError(out, 404);
                        }
                        else
                        {
                            Path path = Paths.get(server.locations.get(r.headers.get("host")) + r.requestURI);
                            if (Files.exists(path)) {
                            	System.out.println("uri " + r.requestURI);
                                sendFileResponse(out, stream_out, path, r);
                            }
                            else
                                sendError(out, 404);
                        }
                    }
                } catch (MalformedRequestException e) {
                    sendError(out, 400);
                }
                catch (Exception e) {
                	e.printStackTrace();
                    sendError(out, 500);
                }
                if(connClose){
                    break;
                }
            }
            in.close();
            out.close();
            stream_out.close();
            this.sock.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}