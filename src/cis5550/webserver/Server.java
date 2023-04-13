package cis5550.webserver;
import cis5550.tools.SNIInspector;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import javax.net.ssl.*;
import java.security.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Server {

    static Server servInstance = null;
    static boolean threadLaunched = false;
    static int port = 80;
    static Integer secure_port = null;
    static ConcurrentHashMap<String, String> locations;
    static String current_host = "*";
    public static ArrayList<String> named_hosts;
    public static ArrayList<RouteInfo> routes;
    public static ConcurrentHashMap<String, ArrayList<Filter>> before_filters;
    public static ConcurrentHashMap<String, ArrayList<Filter>> after_filters;
    public static ConcurrentHashMap<String, Session> sessionMap;
    public static ConcurrentHashMap<String, ArrayList<String>> hosts_certificate;
    public static class RouteInfo
    {
        String method;
        String path_pattern;
        Route route;
        String host;
        RouteInfo(String method, String path_pattern, Route route, String host_name)
        {
            this.method = method;
            this.path_pattern = path_pattern;
            this.route = route;
            this.host = host_name;
        }
    }
    public static void initialize()
    {
        servInstance = new Server();
        routes = new ArrayList<RouteInfo>();
        named_hosts = new ArrayList<String>();
        locations = new ConcurrentHashMap<String, String>();
        before_filters = new ConcurrentHashMap<String, ArrayList<Filter>>();
        after_filters = new ConcurrentHashMap<String, ArrayList<Filter>>();
        sessionMap = new ConcurrentHashMap<String, Session>();
        hosts_certificate = new ConcurrentHashMap<String, ArrayList<String>>();
    }
    public static class staticFiles{
        /*
        The server should also contain a public static class staticFiles with static method location(P).
        When this method is called with a string P, the server should start serving static files from path P
         */
        public static void location(String s) throws Exception {
            if(servInstance == null)
            {
                initialize();
            }
            locations.put(current_host, s);
            if(!threadLaunched)
            {
                threadLaunched = true;
                Thread thread = new Thread(){
                    public void run(){
                        try {
                        	servInstance.run();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
                thread.start();
            }
            // System.out.println("Location set to " + s);
        }

    }
    public static void host(String host_name, String keystore_path, String pass)
    {
        if(host_name.contains(":"))
        {
            host_name = host_name.split(":")[0];
        }
        current_host = host_name;
        named_hosts.add(host_name);
        hosts_certificate.put(host_name, new ArrayList<String>());
        hosts_certificate.get(host_name).add(keystore_path);
        hosts_certificate.get(host_name).add(pass);
    }
    public static void get(String p, Route L) throws Exception {
        if(servInstance == null)
        {
            initialize();
        }
        routes.add(new RouteInfo("GET", p, L, current_host));
        if(!threadLaunched)
        {
            threadLaunched = true;
            Thread thread = new Thread(){
                public void run(){
                    try {
                    	servInstance.run();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            thread.start();
        }
    }
    public static void put(String p, Route L) throws Exception {
        if(servInstance == null)
        {
            initialize();
        }
        routes.add(new RouteInfo("PUT", p, L, current_host));
        if(!threadLaunched)
        {
            threadLaunched = true;
            Thread thread = new Thread(){
                public void run(){
                    try {
                    	servInstance.run();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            thread.start();
        }
    }
    public static void post(String p, Route L) throws Exception {
        if(servInstance == null)
        {
            initialize();
        }
        routes.add(new RouteInfo("POST", p, L, current_host));
        if(!threadLaunched)
        {
            threadLaunched = true;
            Thread thread = new Thread(){
                public void run(){
                    try {
                    	servInstance.run();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            thread.start();
        }
    }
    public static void port(int port_number)
    {
        if(servInstance == null)
        {
            initialize();
        }
        //System.out.println("Port set to " + port_number);
        port = port_number;
    }
    public static void securePort(int port_number)
    {
        if(servInstance == null)
        {
            initialize();
        }
        //System.out.println("Port set to " + port_number);
        secure_port = port_number;
    }
    public static void before(Filter runBefore)
    {
        if(!before_filters.containsKey(current_host))
        {
            ArrayList<Filter> list = new ArrayList<>();
            before_filters.put(current_host, list);
        }
        before_filters.get(current_host).add(runBefore);
    }
    public static void after(Filter runAfter)
    {
        if(!after_filters.containsKey(current_host))
        {
            ArrayList<Filter> list = new ArrayList<>();
            after_filters.put(current_host, list);
        }
        after_filters.get(current_host).add(runAfter);
    }
    public static void run() throws Exception{
        final int NUM_WORKERS = 50;
        Pool thread_pool = new Pool(NUM_WORKERS);
        Thread session_thread = new Thread(){
            public void run(){
                while(true)
                {
                    long current_time = System.currentTimeMillis();
                    for (Map.Entry<String, Session> entry : sessionMap.entrySet())
                    {
                        if((current_time - entry.getValue().lastAccessedTime() > 1000) || (current_time - entry.getValue().creationTime() < entry.getValue().getMaxActiveInterval() * 1000))
                            sessionMap.remove(entry.getKey());
                    }
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
        session_thread.start();
        ServerSocket ssock = new ServerSocket(port);
        Thread http_thread = new Thread(){
            public void run(){
                while(true)
                {
                    Socket sock = null;
                    try {
                        sock = ssock.accept();
                        thread_pool.submit(new HTTPServer(sock, servInstance, false));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
        http_thread.start();
        if(secure_port != null)
        {
            ServerSocket https_sock = new ServerSocket(secure_port);
            Thread https_thread = new Thread(){
                public void run(){
                    while(true)
                    {
                        Socket sock = null;
                        try {
                            sock = https_sock.accept();
                            SNIInspector sniInspector = new SNIInspector();
                            sniInspector.parseConnection(sock);
                            ByteArrayInputStream bais = sniInspector.getInputStream();
                            String pwd = "secret";
                            String keystore_file = "keystore.jks";
                            SNIHostName host_name = sniInspector.getHostName();
                            if(host_name != null)
                            {
                                String hosts_str = host_name.getAsciiName();
                                if(hosts_certificate.containsKey(hosts_str))
                                {
                                    keystore_file = hosts_certificate.get(hosts_str).get(0);
                                    pwd = hosts_certificate.get(hosts_str).get(1);
                                }
                            }
                            KeyStore keyStore = KeyStore.getInstance("JKS");
                            keyStore.load(new FileInputStream(keystore_file), pwd.toCharArray());
                            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
                            keyManagerFactory.init(keyStore, pwd.toCharArray());
                            SSLContext sslContext = SSLContext.getInstance("TLS");
                            sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
                            SSLSocketFactory factory = sslContext.getSocketFactory();
                            Socket socketTLS = factory.createSocket(sock, bais, true);
                            thread_pool.submit(new HTTPServer(socketTLS, servInstance, true));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            };
            https_thread.start();
        }

    }

}