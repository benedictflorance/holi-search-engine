package cis5550.jobs;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Set;
import cis5550.kvs.Row;
import static cis5550.kvs.Row.readFrom;

public class PageRankCombiner {
    public static void main(String args[]) throws Exception {
        RandomAccessFile combined_file = new RandomAccessFile("/Users/namitashukla/Desktop/pageranks.table", "rw");
        Set<String> rows = new HashSet<>();
        File f = new File("/Users/namitashukla/Desktop/pageranktables/");
        File[] files = f.listFiles();
        for (int i = 0; i < files.length; i++) {
//            if(files[i].getName().length() < 6 || !files[i].getName().substring(files[i].getName().length()-6).equals(".table"))
//                continue;
        	System.out.println(files[i].getName());
        	if(!files[i].getName().substring(files[i].getName().length()-6).equals(".table"))
        		continue;
            RandomAccessFile file = new RandomAccessFile("/Users/namitashukla/Desktop/pageranktables/" + files[i].getName(), "rw");
            do{
                long file_pos = file.getFilePointer();
                Row row = readFrom(file);
                if(row == null)
                    break;
                String table = files[i].getName().split("\\.")[0];
                if(rows.contains(row.key()))
                {
                    continue;
                }
                else
                {
                    rows.add(row.key());
                    combined_file.write(row.toByteArray());
                    combined_file.writeBytes("\n");
                }
            }while (true);
            // Close the file and the reader
        }
    }
}
