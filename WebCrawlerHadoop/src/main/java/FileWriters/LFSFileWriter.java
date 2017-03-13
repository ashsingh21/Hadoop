package FileWriters;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by ashu on 3/13/2017.
 */
public class LFSFileWriter {
    private static final LFSFileWriter inst = new LFSFileWriter();

    private LFSFileWriter(){};

    // a synchornised local file system file writer
    public synchronized void writeToFile(StringBuilder sb, String path) throws IOException {
        if (sb != null) {
            BufferedWriter bw = new BufferedWriter(new FileWriter(path, true));
            bw.write(sb.toString());
            bw.newLine();
            bw.close();
        }
    }

    public static LFSFileWriter getInstance() {
        return inst;
    }
}
