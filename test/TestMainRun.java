package test;

import java.io.IOException;

public class TestMainRun {
    public static void main(String args[]) throws InterruptedException {
        try {
            Process ffmpeg = Runtime.getRuntime().exec("avconv -i ~/IdeaProjects/VIZION/data/videos/video_1.mkv -r 1 -f image2 /home/shankz/IdeaProjects/VIZION/data/test_pics/image-%3d.jpg");
            ffmpeg.waitFor();
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
