
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Emmanuel John
 */
public class PositionalIndexer {

    public static class Map extends Mapper<Text, BytesWritable, Text, Text> {

        public Map() {
        }
        private final Text word = new Text();
        
        @Override
        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            ReutersDoc doc = StringUtils.getXMLContent(value);
            //normalize and tokenize string
            String words[] = StringUtils.normalizeText(doc.getContent()).split("::");
            for (int i = 0; i < words.length; i++) {
                StringBuilder buf = new StringBuilder();
                if (!words[i].trim().equals("")) {
                    word.set(words[i]);
                    buf.append(doc.getDocID()).append("::").append(i + 1);
                    context.write(word, new Text(buf.toString()));//<term, docId::pos>
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

            HashMap<String, ArrayList<String>> docPositions = new HashMap<>();
            for (Text text : values) {
                String d[] = text.toString().split("::");
                if (docPositions.containsKey(d[0].trim())) {
                    docPositions.get(d[0].trim()).add(d[1].trim());
                } else {
                    ArrayList<String> pos = new ArrayList<>();
                    pos.add(d[1]);
                    docPositions.put(d[0].trim(), pos);
                }
            }
            
            buf.append(key).append(":").append(docPositions.keySet().size()).append(";");//term:df;
            for (String k : docPositions.keySet()) {
                //docId:pos1,..., posN;
                buf.append(k).append(":");
                for (String p : docPositions.get(k)) {
                    buf.append(p).append(",");
                }
                buf.replace(buf.length() -1, buf.length(), ";");//replace last , with ;
            }
            context.write(new Text(buf.toString()), new Text());//<term:df;docId:pos1,...posN;...;docIdN:pos1,...,posN;>
        }
    }
}
