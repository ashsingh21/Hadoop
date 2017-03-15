package FileWriters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Created by ashu on 3/14/2017.
 */
// writes a file in hdfs
public class MultipleFileWriter {


    public  void writeToFile(StringBuilder sb , long i) throws IOException {

        Configuration conf = Conf.getConf();

        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            System.out.println("Can't create File System: " + e);
        }

        Path workingDir = fs.getHomeDirectory();
        Path out = new Path("/WebCrawler/links/" + i + ".link");

        Path completePath = Path.mergePaths(workingDir, out);

        BufferedWriter bw = null;

        try {
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(completePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (sb != null && bw != null) {
            bw.write(sb.toString());
            bw.newLine();
            bw.close();
        }
    }

}
