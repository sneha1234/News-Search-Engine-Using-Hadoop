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
public class DocCounter {

    public static class Map extends Mapper<Text, BytesWritable, Text, Text> {

        public Map() {
        }
        private final Text word = new Text();
        private int count =0;
        @Override
        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            
            count= count +1;
            //System.out.println(count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println(count);
            context.write(new Text(""), new Text(count+""));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public Reduce() {
        }
        private static int count =0;
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for(Text c: values){
                count += Integer.parseInt(c+"");
                System.out.println(count);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println(count);
            context.write(new Text(""), new Text(count+""));
        }
    }
}
