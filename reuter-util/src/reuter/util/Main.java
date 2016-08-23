package reuter.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author emmanuj
 */
public class Main {

    public static void main(String[] args) {
        try {
            //validate number arguments
            if (args.length != 2) {
                System.out.println("You need to pass two arguments:\n 1. Input "
                        + "directory containing input files followed by:\n 2. Directory for output files");
            }

            Job job = createJob("DocLengthCounter", args[0]);
            job.setMapperClass(DocFolders.Map.class);
            job.setReducerClass(DocFolders.Reduce.class);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException | ClassNotFoundException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    private static Job createJob(String jobname, String inputDir) throws IOException {
        Configuration conf = new Configuration();
        //conf.set("mapred.reduce.tasks", "4");
        
        Job job = new Job(conf, jobname);
        job.setJarByClass(Main.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //job.setInputFormatClass(TextInputFormat.class);
        job.setInputFormatClass(ZipFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        ZipFileInputFormat.setLenient(true);
        ZipFileInputFormat.setInputPaths(job, new Path(inputDir));
        return job;
    }
}
