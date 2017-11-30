package hadoop_reducer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VizionMapper extends Mapper<Text, Video, Text, Text> {

    // Declare LOG for logging class runtime
    private static final Log LOG = LogFactory.getLog(VizionMapper.class);

    @Override
    protected void map(Text key, Video value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
