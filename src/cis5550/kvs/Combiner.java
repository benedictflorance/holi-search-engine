package cis5550.kvs;
import java.io.File;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.util.*;

import static cis5550.kvs.Row.readFrom;
public class Combiner {
    public static int workerIndexForKey(List<String> workers, String key) {
        int chosenWorker = workers.size()-1;
        if (key != null) {
            for (int i=0; i<workers.size()-1; i++) {
                if ((key.compareTo(workers.get(i)) >= 0) && (key.compareTo(workers.get(i+1)) < 0))
                    chosenWorker = i;
            }
        }
        return chosenWorker;
    }
    public static void main(String args[]) throws Exception {
        List<String> workers = Arrays.asList("andre", "donke", "googl", "joker", "monke", "parro", "socke", "umbre", "wonde", "yoloo");
        List<RandomAccessFile> rafs = new ArrayList<>();
        for(int i = 0; i < workers.size(); i++)
        {
            String crawl_filepath = args[1] + "worker" + Integer.toString(i + 1) + "/" + "crawl.table";
            File crawl_file = new File(crawl_filepath);
            crawl_file.getParentFile().mkdirs();
            String id_filepath = args[1] + "worker" + Integer.toString(i + 1) + "/" + "id";
            File id_file = new File(id_filepath);
            id_file.getParentFile().mkdirs();

            rafs.add(new RandomAccessFile(crawl_filepath, "rw"));
            FileWriter writer = new FileWriter(id_filepath);
            writer.write(workers.get(i));
            writer.close();
        }
        Set<String> rows = new HashSet<>();
        File f = new File(args[0]);
        File[] files = f.listFiles();
        for (int i = 0; i < files.length; i++) {
            if(files[i].getName().length() < 6 || !files[i].getName().substring(files[i].getName().length()-6).equals(".table"))
                continue;
            RandomAccessFile file = new RandomAccessFile(args[0] + "/" + files[i].getName(), "rw");
            do{
                long file_pos = file.getFilePointer();
                Row row = readFrom(file);
                if(row == null)
                    break;
                String table = files[i].getName().split("\\.")[0];
                System.out.println(row.key());
                if(rows.contains(row.key()))
                {
                    continue;
                }
                else
                {
                    rows.add(row.key());
                    rafs.get(workerIndexForKey(workers, row.key())).write(row.toByteArray());
                    rafs.get(workerIndexForKey(workers, row.key())).writeBytes("\n");
                }
            }while (true);
            // Close the file and the reader
        }
    }
}
