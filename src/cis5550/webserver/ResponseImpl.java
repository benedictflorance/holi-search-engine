package cis5550.webserver;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

public class ResponseImpl implements Response {
	int statusCode;
	String reasonPhrase;
	Map<String,String> headers;
	byte[] body;
	boolean written;
	boolean halt;
	Connection conn;
		
	public ResponseImpl(Connection conn) {
		statusCode = 200;
		reasonPhrase = "OK";
		headers = new HashMap<String, String>();
		body = null;
		written = false;
		halt = false;
		this.conn = conn;
	}
  // The methods below are used to set the body, either as a string or as an array of bytes 
  // (if the application wants to return something binary - say, an image file). Your server
  // should send back the following in the body of the response:
  //   * If write() has been called, ignore both the return value of Route.handle() and 
  //     any calls to body() and bodyAsBytes().
  //   * If write() has not been called and Route.handle() returns something other than null, 
  //     call the toString() method on that object and send the result.
  //   * If write() has not been called and Route.handle returns null(), use the value from
  //     the most recent body() or bodyAsBytes() call.
  //   * If none of write(), body(), and bodyRaw() have been called and Route.handle returns null,
  //     do not send a body in the response.
  public void body(String body) {
	  this.body = body.getBytes();
  }
  
  public void bodyAsBytes(byte bodyArg[]) {
	  this.body = bodyArg;
  }

  // This method adds a header. For instance, header("Cookie", "abc=def") should cause your
  // server to eventually send a header line "Cookie: abc=def". This method can be called 
  // multiple times with the same header name; the result should be multiple header lines. 
  // type(X) should be the same as header("Content-Type", X). If write() has been called, 
  // these methods should have no effect.
  public void header(String name, String value) {
	  headers.put(name, value);
  }
  
  public void type(String contentType) {
	  headers.put("Content-Type", contentType);
  }

  // This method sets the status code and the reason phrase. If it is called more than once,
  // use the latest values. If it is never called, use 200 and "OK". If write() has been
  // called, status() should have no effect.
  public void status(int statusCode, String reasonPhrase) {
	  this.statusCode = statusCode;
	  this.reasonPhrase = reasonPhrase;
  }

  // This method can be used to send data directly to the connection, without buffering it
  // in an object in memory. The first time write() is called, it should 'commit' the 
  // response by sending out the status code/reason phrase and any headers that have been
  // set so far. Your server should 1) add a 'Connection: close' header, and it should 
  // 2) NOT add a Content-Length header in this case. Then, and in any subsequent calls, 
  // it should simply write the provided bytes directly to the connection.
  public void write(byte[] b) throws Exception {
	  if (!written) {
		written = true;
		headers.put("Connection", "close");
		conn.respond("HTTP/1.1 " + String.valueOf(statusCode) + " " + reasonPhrase + "\r\n");
		for (String headerName : headers.keySet()) {
			conn.respond(headerName + ": " + headers.get(headerName) + "\r\n");
		}
		conn.respond("\r\n");
	  }
	  conn.send(b, b.length);
  }

  // EXTRA CREDIT ONLY - please see the handout for details. If you are not doing the extra
  // credit, please implement this with a dummy method that does nothing.
  public void redirect(String url, int responseCode) {
	  this.statusCode = responseCode;
	  switch (responseCode) {
	  	case 301:
	  		reasonPhrase = "Moved Permanently";
	  		break;
	  	case 302:
	  		reasonPhrase = "Found";
	  		break;
	  	case 303:
	  		reasonPhrase = "See Other";
	  		break;
	  	case 307:
	  		reasonPhrase = "Temporary Redirect";
	  		break;
	  	case 308:
	  		reasonPhrase = "Permanent Redirect";
	  		break;
	  }
	  headers.put("Location", url);
  }

  // EXTRA CREDIT ONLY - please see the handout for details. If you are not doing the extra
  // credit, please implement this with a dummy method that does nothing.
  public void halt(int statusCode, String reasonPhrase) {
	  halt = true;
	  this.statusCode = statusCode;
	  this.reasonPhrase = reasonPhrase;
  }
}
