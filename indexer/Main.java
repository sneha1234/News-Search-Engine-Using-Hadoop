
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    private static int indexType;

    public static void main(String[] args) {
        try {
            //validate number arguments
            if (args.length != 2) {
                System.out.println("You need to pass two arguments:\n 1. Input "
                        + "directory containing input files followed by:\n 2. option for index to use (--biword, --positional or --inverted)\n"
                        + "e.g usage: java Main.java inputDir outputDir --inverted");
            }
            
            //Delete the output folder if it already exist
            deleteDirectory("inverted_index");
            deleteDirectory("biword_index");
            deleteDirectory("postional_index");
            
            Job job = createJob("WordIndexer1", args[0]);
            switch (args[1]) {
                case "--biword":
                    job.setMapperClass(BiWordIndexer.Map.class);
                    job.setReducerClass(BiWordIndexer.Reduce.class);
                    FileOutputFormat.setOutputPath(job, new Path("biword_index"));
                    indexType = 1;
                    job.waitForCompletion(true);
                    break;
                case "--positional":
                    job.setMapperClass(PositionalIndexer.Map.class);
                    job.setReducerClass(PositionalIndexer.Reduce.class);
                    FileOutputFormat.setOutputPath(job, new Path("positional_index"));
                    job.waitForCompletion(true);
                    indexType = 2;
                    break;
                case "--inverted":
                    job.setMapperClass(InvertedIndexer.Map.class);
                    job.setReducerClass(InvertedIndexer.Reduce.class);
                    FileOutputFormat.setOutputPath(job, new Path("inverted_index"));
                    job.waitForCompletion(true);
                    indexType = 0;
                    break;

                default:
                    System.out.println("Invalid indexer option selected. ");
                    System.exit(0);
                    break;
            }

            System.out.println("Creating index file. Please wait...");
            System.out.println("Please enter query to search for: ");
            Scanner in = new Scanner(System.in);
            String query = in.nextLine();
            switch (indexType) {
                case 0:
                    printResult(searchWithInverted(query));
                    break;
                case 1:
                    printResult(searchWithBiWord(query));
                    break;
                case 2:
                    System.out.println("Positional indices created. However, positional query not supported yet. ");
                    System.exit(0);
            }

            //QParser qp = new QParser(new FastCharStream(new StringReader(query)));
            //System.out.println(qp.getToken(0));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException | ClassNotFoundException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
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

    private static ArrayList<String> intersectPostings(ArrayList<String[]> p) {
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

    private static ArrayList<String> searchWithBiWord(String query) throws FileNotFoundException {
        ArrayList<String[]> postings = new ArrayList<>();
        StringTokenizer t = parseAndQuery(query);
        
        if(query.contains(" OR ")){
            t = parseOrQuery(query);
            while (t.hasMoreTokens()) {
                String line = getLineInFile(t.nextToken().trim(), "biword_index");
                if (!line.trim().equals("")) {
                    postings.add(line.split("\t")[1].split(" "));
                }
                return intersectOrPostings(postings);
            }
        }else{
            while (t.hasMoreTokens()) {
                String line = getLineInFile(t.nextToken().trim(), "biword_index");
                if (!line.trim().equals("")) {
                    postings.add(line.split("\t")[1].split(" "));
                }
            }
        }

        
        return intersectPostings(postings);
    }

    private static ArrayList<String> searchWithPositional(String query) {
        return null;
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

    private static String getLineInFile(String word, String dir) throws FileNotFoundException {
        File infile = new File(dir);

        File indices[] = infile.listFiles();

        Scanner in;

        for (File f : indices) {
            in = new Scanner(new FileInputStream(f));
            while (in.hasNextLine()) {
                String line = in.nextLine();
                if (line.contains(word.toLowerCase())) {
                    return line;
                }
            }
        }
        return "";
    }

    private static void deleteDirectory(String dir) {
        File output_dir = new File(dir);
        if (output_dir.exists() && output_dir.isDirectory()) {
            for (File f : output_dir.listFiles()) {
                f.delete();
            }
            output_dir.delete();
        }
    }

    private static Job createJob(String jobname, String inputDir) throws IOException {
        Configuration conf = new Configuration();
        conf.set("mapred.reduce.tasks", "4");
        
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
}
