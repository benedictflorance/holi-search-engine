package cis5550.kvs;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import cis5550.webserver.Server;

public interface Table {
	void putRow(String rKey, Row row) throws Exception;
	Row getRow(String rKey) throws Exception;
	Row getRowForDisplay(String rKey) throws Exception;
	boolean persistent();
	int numRows();
	Set<String> getRowKeys();
	String getKey();
	boolean rename(String tKey) throws IOException;
	void delete() throws Exception;
	void collectGarbage() throws Exception; 
	public void putBatch(List<Row> batch, Server server) throws IOException;
}