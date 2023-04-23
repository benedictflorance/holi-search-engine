package cis5550.webserver;
import java.io.*;
import java.io.IOException;
import java.net.*;
import java.util.Arrays;

public class Connection {
	byte[] buf;
	static final byte[] DOUBLE_CRLF = {'\r', '\n', '\r', '\n'};
	Socket sock;

	public Connection(Socket sock) throws Exception {
		this.sock = sock;
		buf = new byte[0];

	}
	
	public SocketAddress getRmoteSocketAddress() {
		return sock.getRemoteSocketAddress();
	}

	public void respond(String response) throws Exception {
		sock.getOutputStream().write(response.getBytes());
		sock.getOutputStream().flush();
	}

	public void send(byte[] content, int length) throws Exception {
		sock.getOutputStream().write(content, 0, length);
		sock.getOutputStream().flush();
	}

	public void close() throws IOException {
		sock.close();
	}
	
	public byte[] readUntilDoubleCRLF() {
		byte[] ret;
		int index = find(buf, DOUBLE_CRLF);
		if (index != -1) {
			ret = Arrays.copyOfRange(buf, 0, index);
			buf = Arrays.copyOfRange(buf, index + DOUBLE_CRLF.length, buf.length);
			return ret;
		}
		try {
			while (true) {
				byte[] new_bytes = new byte[1048576];
				int n = sock.getInputStream().read(new_bytes);
				// if connection is closed, return null.
				if (n < 0) {
					return null;
				}
				addToBuffer(new_bytes, n);
				index = find(buf, DOUBLE_CRLF);
				if (index != -1) {
					ret = Arrays.copyOfRange(buf, 0, index);
					buf = Arrays.copyOfRange(buf, index + DOUBLE_CRLF.length, buf.length);
					break;
				}
			}

		} catch (IOException e) {
			System.out.println(sock.getInetAddress() + ":" + sock.getPort());
			e.printStackTrace();
			return null;
		}
		return ret;

	}

	public byte[] readLength(int length) throws Exception {
		byte[] ret;
		if (buf.length >= length) {
			ret = Arrays.copyOfRange(buf, 0, length);
			buf = Arrays.copyOfRange(buf, length, buf.length);
			return ret;
		}
		ret = Arrays.copyOf(buf, length);
		int newBytes = sock.getInputStream().readNBytes(ret, buf.length, length - buf.length);
		buf = new byte[0];
		return ret;
	}

	private int find(byte[] buff, byte[] target) {
	    for (int i = 0; i <= buff.length - target.length; i++) {
	        int j = 0;
	        while (j < target.length && buff[i + j] == target[j]) {
	            j++;
	        }
	        if (j == target.length) {
	           return i;
	        }
	    }
	    return -1;
	}

	private void addToBuffer(byte[] new_bytes, int length) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		bos.write(buf);
		bos.write(new_bytes, 0, length);
		buf = bos.toByteArray();
		bos.close();
	}
}