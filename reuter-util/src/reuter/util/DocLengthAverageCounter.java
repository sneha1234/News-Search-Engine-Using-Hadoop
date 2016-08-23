package reuter.util;


import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Emmanuel John
 */
public class DocLengthAverageCounter {

    public static class Map extends Mapper<Text, BytesWritable, Text, Text> {

        public Map() {
        }
        private final Text word = new Text();
        private static int count =0;
        private static int sumCount =0;
        @Override
        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            ReutersDoc doc = StringUtils.getXMLContent(value);
            //normalize and tokenize string
            String words[] = StringUtils.normalizeText(doc.getContent()).split("::");
            sumCount+= words.length;
            count++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(""), new Text(count+":"+sumCount));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public Reduce() {
        }
        private static int sumCount = 0;
        private static int count =0;
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for(Text val: values){
                String v[] = val.toString().split(":");
                count += Integer.parseInt(v[0]);
                sumCount += Integer.parseInt(v[1]);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            double avl = sumCount /count;
            context.write(new Text(""), new Text(avl+""));
        }
        
    }
}
