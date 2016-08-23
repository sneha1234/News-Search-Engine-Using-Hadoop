package reuter.searcher;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    public static class ReuterSearcherMapper extends
            Mapper<Object, Text, Text, FloatWritable> {

        private String query;
        private final Text docid = new Text();
        private final FloatWritable val = new FloatWritable();
        private int N =0;
        
        Map<String, Integer> queryTerms = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            query = conf.get("query");
            File numcountFile = new File(conf.get("count_path")+"/numdocs.txt");
            
            //read number of docs
            Scanner in = new Scanner(numcountFile);
            N = Integer.parseInt(in.nextLine().trim());
            
            String []normq = StringUtils.normalizeText(query).split("::");
            for (String w : normq) {
                Integer n = queryTerms.get(w);
                n = (n == null) ? 1 : ++n;
                queryTerms.put(w, n);
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();//term:docFreq;docID:pos1,pos2;docID:pos1,pos2;
            String term = line.split(";")[0].split(":")[0].trim();

            if (queryTerms.containsKey(term)) {
                String[] part = line.split(";");
                int df = Integer.parseInt(line.split(";")[0].split(":")[1].trim());

                for (int i = 1; i < part.length - 1; i++) { //skip the first and last semi colon
                    //save doc and tf
                    String[] dp = part[i].split(":"); //dp[0] = docId,dp[1]= postings(positions)
                    int tf = dp[1].trim().split(",").length;
                    String docId = dp[0].trim();
                    double Wft = (Math.log10(N / df)) * tf;
                    double Wtq = (Math.log10(N / df)) * queryTerms.get(term);
                    
                    docid.set(docId);
                    val.set((float)(Wft*Wtq));
                    context.write(docid, val);//docId, term:idf&lamda
                }
            }
        }
    }
    public static class ReuterSearcherCombiner extends
            Reducer<Text, FloatWritable, Text, FloatWritable> {

        private static final HashMap<String, Double> docScores = new HashMap<>();
        private static final FloatWritable score = new FloatWritable();
        @Override
        public void reduce(Text key, Iterable<FloatWritable> values,
                Context context) throws IOException, InterruptedException {
            float sum =0;
            for (FloatWritable val : values)
            {
                sum+= val.get();
            }
            
            score.set(sum);
            context.write(key, score);
        }
    }
    public static class ReuterSearcherReducer extends
            Reducer<Text, FloatWritable, Text, FloatWritable> {

        private static final HashMap<String, Integer> docLengths = new HashMap();
        private static final FloatWritable score = new FloatWritable();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            File doclengthsFile = new File(conf.get("count_path")+"/doc_lengths.txt");
            //read doclengths from file
            Scanner in = new Scanner(doclengthsFile);
            while(in.hasNextLine()){
                String line[] = in.nextLine().trim().split("\t");
                docLengths.put(line[0].trim(), Integer.parseInt(line[1]));
            }
        }
        @Override
        public void reduce(Text key, Iterable<FloatWritable> values,
                Context context) throws IOException, InterruptedException {
            
            float sum =0;
            for (FloatWritable val : values) //dv=docID::term-docFreq
            {
                sum+= val.get();
            }
            
            score.set((float) sum/docLengths.get(key.toString()));
            context.write(key, score);
        }
    }

    public static void main(String[] args) throws IOException {
        // deleteDirectory("search_results");
        try {
            Configuration conf = new Configuration();
            conf.set("query", args[2].trim());
            conf.set("count_path", args[1].trim());

            Job job = new Job(conf, "reuter-searcher-tfidf");
            job.setJarByClass(Main.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);
            //job.setInputFormatClass(TextInputFormat.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job, new Path(args[0]));

            job.setNumReduceTasks(1);
            
            job.setMapperClass(ReuterSearcherMapper.class);
            //job.setCombinerClass(ReuterSearcherCombiner.class);
            job.setReducerClass(ReuterSearcherReducer.class);
            FileOutputFormat.setOutputPath(job, new Path("search_results"));
            job.waitForCompletion(true);

        } catch (InterruptedException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
