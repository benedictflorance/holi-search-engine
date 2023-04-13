package cis5550.webserver;

import java.util.*;
import java.net.*;
import java.nio.charset.*;

// Provided as part of the framework code

class RequestImpl implements Request {
  String method;
  String url;
  String protocol;
  InetSocketAddress remoteAddr;
  Map<String, String> headers;
  Map<String, String> cookies;
  Map<String, String> queryParams;
  Map<String, String> params;
  byte bodyRaw[];
  Server server;
  Session session;
  boolean newSession;

  RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String, String> headersArg, Map<String, String> queryParamsArg, Map<String, String> paramsArg, InetSocketAddress remoteAddrArg, byte bodyRawArg[], Server serverArg, Map<String, String> cookiesArg) {
    method = methodArg;
    url = urlArg;
    remoteAddr = remoteAddrArg;
    protocol = protocolArg;
    headers = headersArg;
    queryParams = queryParamsArg;
    params = paramsArg;
    bodyRaw = bodyRawArg;
    session = null;
    server = serverArg;
    newSession = false;
    cookies = cookiesArg;
  }

  public String requestMethod() {
    return method;
  }

  public void setParams(Map<String, String> paramsArg) {
    params = paramsArg;
  }

  public int port() {
    return remoteAddr.getPort();
  }

  public String url() {
    return url;
  }

  public String protocol() {
    return protocol;
  }

  public String contentType() {
    return headers.get("content-type");
  }

  public String ip() {
    return remoteAddr.getAddress().getHostAddress();
  }

  public String body() {
    return new String(bodyRaw, StandardCharsets.UTF_8);
  }

  public byte[] bodyAsBytes() {
    return bodyRaw;
  }

  public int contentLength() {
    return bodyRaw.length;
  }

  public String headers(String name) {
    return headers.get(name.toLowerCase());
  }

  public Set<String> headers() {
    return headers.keySet();
  }

  public String queryParams(String param) {
    return queryParams.get(param);
  }

  public Set<String> queryParams() {
    return queryParams.keySet();
  }

  public String params(String param) {
    return params.get(param);
  }

  public Map<String, String> params() {
    return params;
  }

  public Session session() {
    if (session == null) {
      if (cookies.get("SessionID") != null) {
        String sessionID = cookies.get("SessionID");
        long current_time = System.currentTimeMillis();
        if(server.sessionMap.containsKey(sessionID) &&
                current_time -server.sessionMap.get(sessionID).lastAccessedTime() < 5000
                && current_time - server.sessionMap.get(sessionID).creationTime() < server.sessionMap.get(sessionID).getMaxActiveInterval() * 1000)
        {
          session = server.sessionMap.get(sessionID);
        }
        else {
          session = new SessionImpl(server);
          server.sessionMap.put(session.id(), session);
          newSession = true;
        }
      }
      else
      {
        session = new SessionImpl(server);
        server.sessionMap.put(session.id(), session);
        newSession = true;
      }
    }
    return session;
  }
}
