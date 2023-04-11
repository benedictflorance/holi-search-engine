package cis5550.webserver;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;

import static cis5550.webserver.HTTPServer.error_descriptions;
import static cis5550.webserver.HTTPServer.sendError;


// Provided as part of the framework code

class ResponseImpl implements Response {
    int statusCode;
    String reasonPhrase;
    Map<String, List<String>> headers;
    boolean writeCalled;
    boolean redirectCalled;
    boolean before_stage;
    byte body[];
    Socket client_socket;
    OutputStream stream_out;
    PrintWriter out;
    HashMap<Integer, String> error_descriptions;
    RequestImpl request;
    boolean secure = false;
    ResponseImpl(RequestImpl req, boolean secureArg) {
        statusCode = 200;
        body = null;
        headers = new HashMap<>();
        error_descriptions = new HashMap<Integer, String>();
        error_descriptions.put(301, "Moved Permanently");
        error_descriptions.put(302, "Found");
        error_descriptions.put(303, "See Other");
        error_descriptions.put(307, "Temporary Redirect");
        error_descriptions.put(308, "Permanent Redirect");
        reasonPhrase = "OK";
        writeCalled = false;
        client_socket = null;
        redirectCalled = false;
        before_stage = true;
        request = req;
        secure = secureArg;
    }
    public void setClientSocket(Socket socket) throws IOException {
        client_socket = socket;
        stream_out = socket.getOutputStream();
        out = new PrintWriter(stream_out);
    }
    public void body(String body)
    {
        this.body = body.getBytes();
    }
    public void bodyAsBytes(byte bodyArg[])
    {
        this.body = bodyArg;
    }

    public void header(String name, String value) {
        if(!writeCalled)
        {
            if(!headers.containsKey(name.toLowerCase()))
            {
                List<String> list = new ArrayList<>();
                headers.put(name.toLowerCase(), list);
            }
            headers.get(name.toLowerCase()).add(value);
        }
    }
    public void type(String contentType) {
        header("Content-Type", contentType);
    }

    public void status(int statusCode, String reasonPhrase) {
        if(!redirectCalled)
        {
            this.statusCode = statusCode;
            this.reasonPhrase = reasonPhrase;
        }
    }
    public void write(byte[] b)
    {
        try{
            if(!writeCalled)
            {
                boolean contentTypePresent = false;
                header("Connection", "close");
                out.print("HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\n");
                out.flush();
                if(request.newSession) {
                    String cookie_val = "SessionID=" + request.session().id() + "; SameSite=Strict; HttpOnly";
                    if(secure)
                        cookie_val += "; Secure";
                    header("Set-Cookie", cookie_val);
                }
                for(Map.Entry<String, List<String>> entry : headers.entrySet())
                {
                    for(int i = 0; i < entry.getValue().size(); i++)
                    {
                        if(entry.getKey().toLowerCase().equals("content-type"))
                            contentTypePresent = true;
                        out.print(entry.getKey() + ": " + entry.getValue().get(i) + "\r\n");
                        out.flush();
                    }
                }
                if(!contentTypePresent)
                {
                    out.print("Content-Type: text/html" + "\r\n");
                    out.flush();
                }
                out.print("\r\n");
                out.flush();
                stream_out.write(b);
                stream_out.flush();
                writeCalled = true;
            }
            else
            {
                stream_out.write(b);
                stream_out.flush();
            }
        }
        catch (Exception e)
        {
            return;
        }
    }
    public void redirect(String url, int responseCode) {
        if(!writeCalled)
        {
            header("Location", url);
            status(responseCode, error_descriptions.get(responseCode));
            redirectCalled = true;
        }
    }
    public void halt(int statusCode, String reasonPhrase) {
        if(before_stage)
        {
            header("halt-called", "true");
            status(statusCode, reasonPhrase);
        }
    }
}