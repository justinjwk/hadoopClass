package bdpuh.hw2;

import java.io.File;
import java.io.IOException;
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

public class ParallelLocalToHdfsCopy {

    public static void main(String args[]) {

//        Path localPath = new Path(args[0]);
//        Path hdfsPath = new Path(args[1]);
//        int numOfThreads = Integer.parseInt(args[2]);

        Path localPath = new Path("/home/hdadmin/programming/test");
        Path hdfsPath = new Path("/test");
        int numOfThreads = 2;

        Configuration configuration = new Configuration();

        // Get a copy of FileSystem Object
        FileSystem hdfsFileSystem = null;
        FileSystem localFileSystem = null;
        try {
            hdfsFileSystem = FileSystem.get(configuration);
            localFileSystem = FileSystem.getLocal(configuration);
        } catch (IOException ex) {
            Logger.getLogger(ParallelLocalToHdfsCopy.class.getName()).log(Level.SEVERE, null, ex);
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
            Logger.getLogger(ParallelLocalToHdfsCopy.class.getName()).log(Level.SEVERE, null, ex);
        }


        File localDirectory = new File(localPath.toString());
        File[] localFiles = localDirectory.listFiles();
        Path fileToRead = null;
        Path compressedFileToWrite = null;

        // Get Compressed FileOutputStream
        CompressionCodec compressionCodec = new GzipCodec();
        CompressionOutputStream compressedOutputStream = null;

        FSDataInputStream fSDataInputStream = null;
        FSDataOutputStream fsDataOutputStream = null;

        if (localFiles != null) {
            for(File file: localFiles) {

                fileToRead = new Path(file.getPath());
                compressedFileToWrite = new Path(hdfsPath, fileToRead.getName()+ ".gz");

                try {
                    // Open a File for Reading
                    fSDataInputStream = localFileSystem.open(fileToRead);

                    // Open a File for Writing .gz file
                    fsDataOutputStream = hdfsFileSystem.create(compressedFileToWrite);
                    compressedOutputStream = compressionCodec.createOutputStream(fsDataOutputStream);

                    // Copy files
                    IOUtils.copyBytes(fSDataInputStream, compressedOutputStream, configuration);

                } catch (IOException ex) {
                    Logger.getLogger(ParallelLocalToHdfsCopy.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            // Close streams
            try {
                fSDataInputStream.close();
                fsDataOutputStream.close();
                compressedOutputStream.close();
                hdfsFileSystem.close();
            } catch (IOException ex) {
                Logger.getLogger(ParallelLocalToHdfsCopy.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}