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
public class DocFolders {

    public static class Map extends Mapper<Text, BytesWritable, Text, Text> {

        public Map() {
        }
        private static final Text wordCount = new Text();
        private static final Text docid =  new Text();
        @Override
        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            ReutersDoc doc = StringUtils.getXMLContent(value);
            docid.set(doc.getDocID());
            wordCount.set(doc.getDate());
            context.write(docid,wordCount);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public Reduce() {
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for(Text val: values){
                context.write(key, val);
            }
        }
    }
}
