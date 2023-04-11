package cis5550.kvs;
import java.io.IOException;
import java.util.Set;

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
}