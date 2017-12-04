package hadoop_reducer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;

public class VizionMapper extends Mapper<Text, Video, Text, Text> {

    // Declare LOG for logging class runtime
    private static final Log LOG = LogFactory.getLog(VizionMapper.class);

    @Override
    protected void map(Text key, Video value, Context context) throws IOException, InterruptedException {

        String folder_ref = key.toString().split("\\.")[0].replaceAll("[\\D]", "");

        File directory = new File("/home/shankz/CROPPER/pic_" + folder_ref);
        directory.mkdir();

        File directory2 = new File("/home/shankz/CROPPER/cropped_" + folder_ref);
        directory2.mkdir();

        String output = null;

        int total_seconds = 0;

        int id = value.getUid();

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(value.getVideoBytesArray());
        LOG.info("LOG: VizionMapper_byteArray: " + byteArrayInputStream.available());

        int flag = 0;
        int video_num = 0;

        FileSystem hdfs = FileSystem.get(new Configuration());
        Path hdfs_file_path = new Path("/user/shankz/input/reference.jpg");
        Path local_file_path = new Path("/tmp/");
        hdfs.copyToLocalFile(hdfs_file_path, local_file_path);

        hdfs_file_path = new Path("/user/shankz/input/timer.txt");
        local_file_path = new Path("/tmp/");
        hdfs.copyToLocalFile(hdfs_file_path, local_file_path);

        String file_name = key.toString();

        // Process inputData = Runtime.getRuntime().exec("/usr/local/hadoop/bin/hdfs dfs -get /home/shankz/hadoop/test/input /tmp");
        // inputData.waitFor();

        LocalFileSystem fileSystem = FileSystem.getLocal(context.getConfiguration());
        Path file_path = new Path("/tmp", file_name);
        Path res_file = new Path("/tmp", "res_" + file_name);

        System.out.println("File to Process: " + file_path.toString());

        FSDataOutputStream dataOutputStream = fileSystem.create(file_path, true);
        output = file_path.toString().split("/")[2];
        dataOutputStream.write(value.getVideoBytesArray());
        dataOutputStream.close();

        StringBuilder stringBuilder = new StringBuilder("");

        try {
            ProcessBuilder ffmpeg = new ProcessBuilder("avconv -i " + file_path.toString() + " -r 1 -f image2 /user/shankz/CROPPER/pic_" + folder_ref + "/image-%3d.jpg");
            // ProcessBuilder ffmpeg = new ProcessBuilder("/usr/bin/avconv -i /user/shankz/hadoop/test/input/video_1.mkv -r 1 -f image2 /user/shankz/CROPPER/pic_" + folder_ref + "/image-%3d.jpg");
             ffmpeg.redirectErrorStream(true);
             Process ffmpegProcess = ffmpeg.start();


            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(ffmpegProcess.getInputStream()));
            String linez = null;
            while((linez = bufferedReader.readLine()) != null) {
                stringBuilder.append(linez);
            }
            ffmpegProcess.waitFor();
            stringBuilder.append(ffmpegProcess.exitValue());

             Process cropper = Runtime.getRuntime().exec("/usr/bin/python main.py " + folder_ref + "", null, new File("/home/shankz/IdeaProjects/VIZION/CROPPER/"));
             cropper.waitFor();

            BufferedReader bufferedReader2 = new BufferedReader(new InputStreamReader(cropper.getInputStream()));
            String linez2 = null;
            while((linez2 = bufferedReader2.readLine()) != null) {
                stringBuilder.append(linez2);
            }


        } catch (Exception ex) {
            ex.printStackTrace();
            stringBuilder.append("FAILED: " + ex.getMessage());
        }


        stringBuilder.append(folder_ref);

        String reducer_output = stringBuilder.toString();
        context.write(key, new Text(reducer_output));
        stringBuilder.setLength(0);
    }
}
