package bdpuh.hw2;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

public class ParallelLocalToHdfsCopy1 {

    public static void main(String args[]) throws InterruptedException {

        Path localPath = new Path(args[0]);
        Path hdfsPath = new Path(args[1]);
        int numOfThreads = Integer.parseInt(args[2]);

//        Path localPath = new Path("/home/hdadmin/programming/test");
//        Path hdfsPath = new Path("/test");
//        int numOfThreads = 2;

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numOfThreads);

        Configuration configuration = new Configuration();

        // Get a copy of FileSystem Object
        FileSystem hdfsFileSystem = null;
        FileSystem localFileSystem = null;
        try {
            hdfsFileSystem = FileSystem.newInstance(configuration);
            localFileSystem = FileSystem.newInstanceLocal(configuration);
        } catch (IOException ex) {
            Logger.getLogger(ParallelLocalToHdfsCopy1.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Local directory check
        // If the local directory doesn't exist, print an error msg and exit program
        File localDir = new File(localPath.toString());
        if(!(localDir.exists())) {
            System.out.println("Source directory does not exist");
            System.exit(0);
        }

        // HDFS directory check
        // If the hdfs directory exists, print error msg and exit program
        try {
            if (hdfsFileSystem.exists(hdfsPath)) {
                System.out.println("Destination directory already exists. " +
                        "Please delete before running the program");
                System.exit(0);
            }
            else {
                hdfsFileSystem.mkdirs(hdfsPath);
            }
        } catch (IOException ex) {
            Logger.getLogger(ParallelLocalToHdfsCopy1.class.getName()).log(Level.SEVERE, null, ex);
        }


        File localDirectory = new File(localPath.toString());
        File[] localFiles = localDirectory.listFiles();
        final Path[] fileToRead = {null};
        final Path[] compressedFileToWrite = {null};

        // Get Compressed FileOutputStream
        CompressionCodec compressionCodec = new GzipCodec();
        final CompressionOutputStream[] compressedOutputStream = {null};

        final FSDataInputStream[] fSDataInputStream = {null};
        final FSDataOutputStream[] fsDataOutputStream = {null};

        if (localFiles != null) {
            for(File file: localFiles) {

                FileSystem finalLocalFileSystem = localFileSystem;
                FileSystem finalHdfsFileSystem = hdfsFileSystem;
                executor.submit(()-> {

                    fileToRead[0] = new Path(file.getPath());
                    compressedFileToWrite[0] = new Path(hdfsPath, fileToRead[0].getName()+ ".gz");

                    try {
                        // Open a File for Reading
                        fSDataInputStream[0] = finalLocalFileSystem.open(fileToRead[0]);

                        // Open a File for Writing .gz file
                        fsDataOutputStream[0] = finalHdfsFileSystem.create(compressedFileToWrite[0]);
                        compressedOutputStream[0] = compressionCodec.createOutputStream(fsDataOutputStream[0]);

                        // Copy files
                        IOUtils.copyBytes(fSDataInputStream[0], compressedOutputStream[0], configuration);

                    } catch (IOException ex) {
                        Logger.getLogger(ParallelLocalToHdfsCopy1.class.getName()).log(Level.SEVERE, null, ex);
                    }
                });
            }

            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            // Close streams
            try {
                fSDataInputStream[0].close();
                fsDataOutputStream[0].close();
                compressedOutputStream[0].close();
                hdfsFileSystem.close();
            } catch (IOException ex) {
                Logger.getLogger(ParallelLocalToHdfsCopy1.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}