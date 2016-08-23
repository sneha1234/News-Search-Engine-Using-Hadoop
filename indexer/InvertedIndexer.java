

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class InvertedIndexer {

    public static class Map extends Mapper<Text, BytesWritable, Text, Text> {

        public Map() {
        }
        private final static IntWritable one = new IntWritable(1);
        private final Text docId = new Text();
        private final Text word = new Text();

        @Override
        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            ReutersDoc doc = StringUtils.getXMLContent(value);
            docId.set(doc.getDocID());
            //normalize and tokenize string
            StringTokenizer tokenizer = new StringTokenizer(StringUtils.normalizeText(doc.getContent()), "::");
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken().trim());
                if (!word.toString().equals("")) {
                    context.write(word, docId);
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
            ArrayList<Long> ids = new ArrayList<>();
            //TODO: save word in memory
            for (Text text : values) {
                if(!ids.contains(Long.parseLong(text.toString().trim()))){
                    ids.add(Long.parseLong(text.toString().trim()));
                }
            }
            Collections.sort(ids);
            key.set(key.toString().trim());
            StringBuilder buf = new StringBuilder();
            for(long id: ids){
                buf.append(" ").append(id);
            }
            context.write(key, new Text(buf.toString().trim()));
        }

    }
}
