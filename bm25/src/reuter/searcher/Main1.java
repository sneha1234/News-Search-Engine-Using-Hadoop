package reuter.searcher;

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

public class Main1 {

    public static class ReuterSearcherMapper extends
            Mapper<Object, Text, Text, Text> {

        private String query;
        private final Text word = new Text();
        private final Text val = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            query = conf.get("query");
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();//term:docFreq;docID:pos1,pos2;docID:pos1,pos2;

            String term = line.split(";")[0].split(":")[0].trim();
               // if (term.equalsIgnoreCase(t)) {
            //save df
            //parse line
            String[] k = line.split(";");
            for (int i = 1; i < k.length - 1; i++) { //skip the first and last semi colon
                //save doc and tf
                String[] dp = k[i].split(":");
                StringBuilder builder = new StringBuilder();
                builder.append(dp[0].trim()).append("::").append(dp[1].trim().split(",").length);
                word.set(term);
                val.set(builder.toString());
                context.write(word, val);//(term,docID::term-docFreq)
            }
        }
    }

    public static class ReuterSearcherReducer extends
            Reducer<Text, Text, Text, Text> {

        private String query;
        private static final ArrayList<SearchResult> results = new ArrayList<>();
        private static final HashMap<String, Integer> doctf = new HashMap<>();
        private static final HashMap<String, HashMap<String, Integer>> tf = new HashMap();
        private static final Set<String> docs = new HashSet<>();
        private static final HashMap<String, Integer> doc_length = new HashMap<>();
        private static String[] words;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            query = conf.get("query");
            words = StringUtils.normalizeText(query).split("::");
        }

        @Override
        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {

            int df = 0;
            HashMap<String, Integer> postings = new HashMap();
            for (Text dv : values) //dv=docID::term-docFreq
            {
                String[] l = dv.toString().split("::");
                df += Integer.parseInt(l[1]);
                docs.add(l[0].trim());//maintains docIDs for all term

                //building doc lengths
                if (doc_length.containsKey(l[0].trim())) {
                    doc_length.put(l[0].trim(), (doc_length.get(l[0].trim()) + Integer.parseInt(l[1])));
                } else {
                    doc_length.put(l[0].trim(), Integer.parseInt(l[1]));
                }

                postings.put(l[0], Integer.parseInt(l[1]));// maintains docId,term-docFreq

                for(int m = 0; m < words.length; m++) {
                    if (key.toString().equals(words[m])) {
                        tf.put(key.toString(), postings);//term,posting-list
                        doctf.put(key.toString(), df);//term,termfreq
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!tf.isEmpty()) {

                HashMap<String, Double> idf = new HashMap();
                HashMap<String, Double> score = new HashMap();
                //fill freq of terms in docs
                //--------------------executed for each query term------------------------------
                int i, docFreq;
                for (String term : words) {
                    docFreq = tf.get(term).keySet().size();
                    idf.put(term, Double.parseDouble("" + docFreq));

                }

                //----------------------------------end-------------------------------------------
                //idf calculation
                double numerator, denominator, k1 = 2.0, b = 0.75;
                for (String w : idf.keySet()) {

                    numerator = docs.size() - idf.get(w) + 0.5;
                    denominator = idf.get(w) + 0.5;
                    idf.put(w, Math.log10(numerator / denominator));
                }

                //average doc length calculation
                double avg_l = 0.0;
                for (String doc_id : doc_length.keySet()) {
                    avg_l = avg_l + doc_length.get(doc_id);
                }
                avg_l = avg_l / doc_length.keySet().size();

                //BM-25 calculation
                for (String term : words) {
                    HashMap<String, Integer> tfreq = tf.get(term);
                    if(tfreq!=null){
                        for (String k : tfreq.keySet()) {
                            int doc_term_freq = tfreq.get(k);
                            numerator = doc_term_freq * (k1 + 1);
                            denominator = doc_term_freq + (k1 * (1 - b + (b * (doc_length.get(k) / avg_l))));
                            numerator = numerator / denominator;
                            numerator = numerator * idf.get(term);
                            if (score.containsKey(k)) {
                                score.put(k, score.get(k) + numerator);
                            } else {
                                score.put(k, numerator);
                            }
                        }
                    }
                }

                for (String docID : score.keySet()) {
                    results.add(new SearchResult(docID, score.get(docID)));
                }

                Collections.sort(results, new Comparator<SearchResult>() {

                    @Override
                    public int compare(SearchResult o1, SearchResult o2) {
                        if (o1.score > o2.score) {
                            return -1;
                        } else if (o1.score < o2.score) {
                            return 1;
                        }
                        return 0;
                    }
                });
            }

            if (results.size() > 0) {
                int k = 0;
                for (SearchResult r : results) {
                    //System.out.println(r.docID + ":" + r.score);
                    context.write(new Text(r.docID), new Text(r.score + ""));
                    if (k >= 9) {
                        break;
                    }
                    k++;
                }
            } else {
                //System.out.println("No results found");
                context.write(new Text(""), new Text("No results found"));
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
            conf.set("query", args[1].trim());

            Job job = new Job(conf, "reuter-search");
            job.setJarByClass(Main1.class);

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
            Logger.getLogger(Main1.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Main1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
