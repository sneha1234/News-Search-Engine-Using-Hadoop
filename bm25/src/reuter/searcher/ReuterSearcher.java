package reuter.searcher;



import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Emmanuel John
 */
public class ReuterSearcher {

    public static class Map extends Mapper<Text, BytesWritable, Text, Text> {

        public Map() {
        }
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

            ReutersDoc doc = StringUtils.getXMLContent(value);
            //normalize and tokenize string
            String words[] = StringUtils.normalizeText(doc.getContent()).split("::");
            for (int i = 0; i < words.length; i++) {
                StringBuilder buf = new StringBuilder();
                if (!words[i].trim().equals("")) {
                    word.set(words[i]);
                    buf.append(doc.getDocID()).append("::").append(i + 1);
                    context.write(word, new Text(buf.toString()));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public Reduce() {
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder buf = new StringBuilder();
            long freq = 0;

            HashMap<String, ArrayList<String>> docPositions = new HashMap<>();
            for (Text text : values) {
                freq++;
                String d[] = text.toString().split("::");
                if (docPositions.containsKey(d[0])) {
                    docPositions.get(d[0]).add(d[1]);
                } else {
                    ArrayList<String> pos = new ArrayList<>();
                    pos.add(d[1]);
                    docPositions.put(d[0], pos);
                }
            }
            buf.append(key).append(":").append(freq).append(";");
            for (String k : docPositions.keySet()) {
                buf.append(k).append(":");
                for (String p : docPositions.get(k)) {
                    buf.append(p).append(",");
                }
                buf.replace(buf.length() -1, buf.length(), ";");
            }
            context.write(new Text(buf.toString()), new Text());
        }
    }
}
