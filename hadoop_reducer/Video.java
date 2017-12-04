package hadoop_reducer;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

public class Video implements Writable {

    private byte[] videoBytesArray = null;
    private InputStream inputStream = null;
    private int uid;

    /**
     * This is a serializable object that implements the base class Writable Object,
     * the Writable object implements a simple, efficient, and serialization protocol, based on DataInput and DataOutput
     * Any key or value type in the Hadoop Map-Reduce framework implements this interface.
     * For more information, visit: https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/io/Writable.html
     */
    public Video() {}

    /**
     * This is a constructor with 1 argument that contains the bytes of the video
     * @param _video
     */
    public Video(byte[] _video) {
        videoBytesArray = _video;
    }

    /**
     * Serialize the fields of this object to dataOutput
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        // Serializes an integer to a binary stream with zero-compressed encoding.
        WritableUtils.writeVInt(dataOutput, videoBytesArray.length);

        // Writes to the output stream
        dataOutput.write(videoBytesArray);
    }

    /**
     * Deserialize the fields of this object from dataInput
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        // Get the length of the data
        int length = WritableUtils.readVInt(dataInput);

        // Create a new byte array containing the bytes of the video
        videoBytesArray = new byte[length];

        // Reads len bytes from an input stream
        dataInput.readFully(videoBytesArray, 0, length);
    }

    /**
     * Read the DataInput bytes from binary stream
     * @param in
     * @return video Object with the bytes data
     * @throws IOException
     */
    public static Video read(DataInput in) throws IOException {
        // Construct the Video Object
        Video video = new Video();
        // Deserialize the fields of this object
        video.readFields(in);
        // Return the video object
        return video;
    }

    /**
     * Get the unique identifier for this object
     * @return
     */
    public int getUid() {
        return uid;
    }

    /**
     * Set the unique identifier for this object
     * @param uid
     */
    public void setUid(int uid) {
        // Set the uid provided by the argument
        this.uid = uid;
    }

    /**
     * Return the input stream of the video object
     * @return inputStream
     */
    public InputStream getInputStream() {
        return inputStream;
    }

    /**
     * Set the input stream from the argument
     * @param inputStream
     */
    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    /**
     * Get the video bytes array
     * @return videoBytesArray
     */
    public byte[] getVideoBytesArray() {
        return videoBytesArray;
    }

    /**
     * Set the video bytes specified by the argument
     * @param videoBytesArray
     */
    public void setVideoBytesArray(byte[] videoBytesArray) {
        this.videoBytesArray = videoBytesArray;
    }
}
