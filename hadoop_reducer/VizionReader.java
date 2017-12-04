package hadoop_reducer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class VizionReader extends RecordReader<Text, Video> {

    private static final Log LOG = LogFactory.getLog(VizionReader.class);
    private int startIndex = 0;
    private String fileName;
    private int endIndex;

    private FSDataInputStream fsDataInputStream;
    private Text key = null;
    private Video value = null;
    private Video video = null;

    /**
     * Initialize the RecordReader implementation
     * @param inputSplit
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // Get the section of an input file
        FileSplit split = (FileSplit) inputSplit;
        org.apache.hadoop.conf.Configuration job = taskAttemptContext.getConfiguration();

        startIndex = 0;
        endIndex = 1;

        final Path path = split.getPath();
        FileSystem fs = path.getFileSystem(job);
        fsDataInputStream = fs.open(path);
        fileName = path.getName();

        byte[] b = new byte[fsDataInputStream.available()];
        fsDataInputStream.readFully(b);
        video = new Video(b);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (startIndex < endIndex) {
            key = new Text();
            key.set(fileName);
            value = video;
            startIndex++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Video getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (startIndex == endIndex) {
            return 1.0f;
        } else {
            return (float)(endIndex - startIndex) / (endIndex);
        }
    }

    @Override
    public void close() throws IOException {

    }
}
