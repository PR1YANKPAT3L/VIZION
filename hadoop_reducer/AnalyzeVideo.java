package hadoop_reducer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalyzeVideo {

    private static final Log LOG = LogFactory.getLog(AnalyzeVideo.class);

    private static Configuration configuration;
    static long MILLISECS = 500000;
    private static Job job = null;
    private final static String JOB_NAME = "AnalyzeVideo";
    private static String WORKING_DIR = "";
    private static Path outputPath = null;
    private static FileSystem fileSystem = null;
    private static String input_dir, output_dir;

    /**
     * Main entry point for the hadoop driver to configure and initiate the jobs
     * @param args Takes the arguments from the user as the input
     * @throws Exception Throws exception during runtime
     */
    public static void main(String args[]) throws Exception {
        configuration = new Configuration();
        configuration.setLong("mapred.task.timeout", MILLISECS);

        input_dir = args[0];
        output_dir = args[1];

        if (job != null) {

            WORKING_DIR = job.getWorkingDirectory().toString();

            // Get job instance with the configuration of timeout of 500s
            job = Job.getInstance(configuration, JOB_NAME);

            // Set the Jar to be used during the hadoop,
            // which is the driver class with main entry
            job.setJarByClass(AnalyzeVideo.class);

            // Set the custom InputFormat, which MapReduce framework relies on
            // 1. Validate the input-spec. of the job
            // 2. Split-up the input file(s) into logical InputSplits, each of which is then assigned to an individual Mapper
            // 3. Provide the RecordReader implementation to be used to glean input records from the logical InputSplit for processing by the Mapper
            job.setInputFormatClass(VizionFormatter.class);

            // Set the custom mapper class
            job.setMapperClass(VizionMapper.class);

            // Set the custom reducer class
            job.setReducerClass(VizionReducer.class);

            // Set the Output Key class and the Output value class as they need to match the job's output,
            // which is a text output
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }

        // FileInputFormat is a base-class for all file-based classes.
        FileInputFormat.addInputPath(job, new Path(input_dir));

        // Set the output path that is specified by the user
        outputPath = new Path( WORKING_DIR+ "/" + output_dir);

        // Get the file system from the hadoop cluster
        fileSystem = outputPath.getFileSystem(configuration);

        // Check for existence of output directory
        if (fileSystem.exists(outputPath)) {
            // Delete the directory to be replaced with the new output
            fileSystem.delete(outputPath, true);
        }

        // Set the output path of the FileOutput System
        FileOutputFormat.setOutputPath(job, new Path(output_dir));

        // Exit the system on sync job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
