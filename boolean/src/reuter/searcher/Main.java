package reuter.searcher;

import java.io.FileNotFoundException;
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
        private static String[] words;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            query = conf.get("query");
            words = StringUtils.normalizeText(query).split("::");
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();//term 

            String term = line.split("\t")[0].trim();
            if(!term.isEmpty()){
                for(String t:words){
                    if(t.trim().equalsIgnoreCase(term)){
                        context.write(new Text(t), new Text(line.split("\t")[1]));
                    }
                }
            }
            
        }
    }

    public static class ReuterSearcherReducer extends
            Reducer<Text, Text, Text, Text> {
        private ArrayList<String[]> postings = new ArrayList();
        @Override
        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            for(Text tv: values){
                postings.add(tv.toString().split(" "));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            
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

            Job job = new Job(conf, "reuter-searcher-boolean");
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
    
    private static ArrayList<String> intersectOrPostings(ArrayList<String[]> p) {
        ArrayList<String> answer = new ArrayList<>();

        if (p.isEmpty()) {
            return answer;
        }

        for(String []s:p){
            Collections.addAll(answer, s);
        }
        
        return answer;
    }
    private static StringTokenizer parseAndQuery(String query) {
        StringTokenizer tokenizer = new StringTokenizer(query, "AND");
        return tokenizer;
    }

    private static StringTokenizer parseOrQuery(String query) {
        StringTokenizer tokenizer = new StringTokenizer(query, "OR");
        return tokenizer;
    }

    private static void printResult(ArrayList<String> data) {
        System.out.println("Number of results found: " + data.size());
        for (String docid : data) {
            System.out.println(docid);
        }
    }

    private static void printResults(ArrayList<String[]> data) {
        System.out.println("Number of results found: " + data.size());
        for (String[] docids : data) {
            for (String docid : docids) {
                System.out.print(docid+" ");
            }
            System.out.println();
        }
    }
    private static ArrayList<String> intersectAndPostings(ArrayList<String[]> p) {
        ArrayList<String> answer = new ArrayList<>();

        if (p.isEmpty()) {
            return answer;
        }

        if (p.size() == 1) {
            String[] p1 = p.get(0);
            answer.addAll(Arrays.asList(p1));
            return answer;
        }

        String[] p1 = p.get(0);
        String[] p2 = p.get(1);

        int i = 0;
        int j = 0;
        while (i != p1.length && j != p2.length) {
            long d1 = Long.parseLong(p1[i].trim());
            long d2 = Long.parseLong(p2[j].trim());
            if (d1 == d2) {
                answer.add(p1[i]);
                i++;
                j++;
            } else if (d1 < d2) {
                i++;
            } else {
                j++;
            }
            System.out.println(d1+" : "+d2);
        }
        return answer;
    }
    private static ArrayList<String> searchWithInverted(String query) throws FileNotFoundException {
        ArrayList<String[]> postings = new ArrayList<>();
        StringTokenizer t = parseAndQuery(query);
        
        if(query.contains(" OR ")){
            t = parseOrQuery(query);
            while (t.hasMoreTokens()) {
                String line = getLineInFile(t.nextToken().trim(), "inverted_index");
                if (!line.trim().equals("")) {
                    postings.add(line.split("\t")[1].split(" "));
                }
                
                return intersectOrPostings(postings);
            }
        }else{
            while (t.hasMoreTokens()) {
                String line = getLineInFile(t.nextToken().trim(), "inverted_index");
                if (!line.trim().equals("")) {
                    postings.add(line.split("\t")[1].split(" "));
                }
            }
        }

        
        return intersectPostings(postings);
    }
}
