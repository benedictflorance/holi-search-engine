package cis5550.kvs;
import java.util.Set;

public interface Table {
	void putRow(String rKey, Row row) throws Exception;
	Row getRow(String rKey) throws Exception;
	boolean persistent();
	int numRows();
	Set<String> getRowKeys();
	String getKey();
	void setKey(String tKey);
	void delete() throws Exception;
	void collectGarbage() throws Exception; 
}