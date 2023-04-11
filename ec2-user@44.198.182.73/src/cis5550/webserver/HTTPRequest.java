package cis5550.webserver;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class HTTPRequest {
    String method;
    String requestURI;
    String protocol;
    HashMap<String,String> headers;
    HashMap<String,String> cookies;
    byte body[], request[];

    HTTPRequest() {
        method = null;
        requestURI = null;
        protocol = null;
        headers = new HashMap<String,String>();
        cookies = new HashMap<String,String>();
        body = null;
        request = null;
    }

    String body() {
        return new String(body, StandardCharsets.UTF_8);
    }

    byte[] bodyAsBytes() {
        return body;
    }
};