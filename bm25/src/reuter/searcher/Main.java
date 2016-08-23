package reuter.searcher;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
            Mapper<Object, Text, Text, Text> {

        private String query;
        private final Text docid = new Text();
        private final Text val = new Text();
        private int N = 0;
        private double Lave = 0;
        private static final HashMap<String, Integer> docLengths = new HashMap();
        private static final double k = 2.0;
        private static final double b = 0.75;
        private static List<String> queryTerms;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            query = conf.get("query");
            File numcountFile = new File(conf.get("count_path") + "/numdocs.txt");
            File doclengthsFile = new File(conf.get("count_path") + "/doc_lengths.txt");

            //read number of docs
            Scanner in = new Scanner(numcountFile);
            N = Integer.parseInt(in.nextLine().trim());

            int sum = 0;
            //read doclengths from file
            in = new Scanner(doclengthsFile);
            while (in.hasNextLine()) {
                String line[] = in.nextLine().trim().split("\t");
                docLengths.put(line[0].trim(), Integer.parseInt(line[1]));
                sum += Integer.parseInt(line[1]);
            }

            Lave = sum / docLengths.size();

            queryTerms = Arrays.asList(StringUtils.normalizeText(query).split("::"));
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();//term:docFreq;docID:pos1,pos2;docID:pos1,pos2;
            String term = line.split(";")[0].split(":")[0].trim();

            if (queryTerms.contains(term)) {
                //System.out.println(term);
                String[] part = line.split(";");
                int df = Integer.parseInt(line.split(";")[0].split(":")[1].trim());

                for (int i = 1; i < part.length - 1; i++) { //skip the first and last semi colon
                    //save doc and tf
                    String[] dp = part[i].split(":"); //dp[0] = docId,dp[1]= postings(positions)
                    int tf = dp[1].trim().split(",").length;
                    String docId = dp[0].trim();
                    double num = (k + 1) * tf;
                    double den = k * ((1 - b) + b * (docLengths.get(docId) / Lave)) + tf;
                    double lambda = num / den;
                    docid.set(docId);
                    val.set(term + ":" + (Math.log10(N / df)) + "&" + lambda);
                    context.write(docid, val);//docId, term:idf&lamda
                }
            }
        }
    }

    public static class ReuterSearcherReducer extends
            Reducer<Text, Text, Text, Text> {

        private static final ArrayList<SearchResult> results = new ArrayList<>();
        private static final HashMap<String, Double> docScores = new HashMap<>();
        private static final HashMap<String, Double> termIdf = new HashMap<>();
        private static final Text docId = new Text();
        private static final Text score_text = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {

            HashMap<String, Integer> postings = new HashMap();
            for (Text dv : values) //dv=docID::term-docFreq
            {
                String l[] = dv.toString().split("&");
                if (docScores.get(key.toString()) != null) {
                    docScores.put(key.toString(), docScores.get(key.toString()) + Double.parseDouble(l[1]));
                } else {
                    docScores.put(key.toString(), Double.parseDouble(l[1]));
                }
                if (termIdf.get(l[0].split(":")[0]) == null) {
                    termIdf.put(l[0].split(":")[0], Double.parseDouble(l[0].split(":")[1]));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            double sumIdf = 0;
            for (String t : termIdf.keySet()) {
                sumIdf += termIdf.get(t);
            }

            for (String docid : docScores.keySet()) {
                double score = docScores.get(docid) * sumIdf;
                //docScores.put(docid, score); //not necessary
                docId.set(docid);
                score_text.set(score + "");
                context.write(docId, score_text);
            }

        }

        class SearchResult {

            public SearchResult(String docID, Double score) {
                this.docID = docID;
                this.score = score;
            }

            String docID;
            Double score = 0.0;

            @Override
            public boolean equals(Object obj) {
                if (!(obj instanceof SearchResult)) {
                    return false;
                }

                SearchResult s = (SearchResult) obj;
                return Objects.equals(s.score, this.score) && s.docID.equals(this.docID);
            }

            @Override
            public int hashCode() {
                int hash = 3;
                hash = 43 * hash + Objects.hashCode(this.docID);
                hash = 43 * hash + Objects.hashCode(this.score);
                return hash;
            }

        }
    }

    public static void main(String[] args) throws IOException {
        // deleteDirectory("search_results");
        try {
            Configuration conf = new Configuration();
            conf.set("query", args[2].trim());
            conf.set("count_path", args[1].trim());

            Job job = new Job(conf, "reuter-searcher-bm25");
            job.setJarByClass(Main.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //job.setInputFormatClass(TextInputFormat.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job, new Path(args[0]));

            job.setNumReduceTasks(1);
            job.setMapperClass(ReuterSearcherMapper.class);
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
