package reuter.searcher;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author emmanuj
 */
public class DocWritable implements Writable {
    private Text termIdf;
    private Text lambda;
    private Text docId;

    public DocWritable() {
    }

    public DocWritable(Text termIdf, Text lambda, Text docId) {
        this.termIdf = termIdf;
        this.lambda = lambda;
        this.docId = docId;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        termIdf.write(d);
        lambda.write(d);
        docId.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        termIdf.readFields(di);
        lambda.readFields(di);
        docId.readFields(di);
    }

    public Text getTermIdf() {
        return termIdf;
    }

    public void setTermIdf(Text termIdf) {
        this.termIdf = termIdf;
    }

    public Text getLambda() {
        return lambda;
    }

    public void setLambda(Text lambda) {
        this.lambda = lambda;
    }

    public Text getDocId() {
        return docId;
    }

    public void setDocId(Text docId) {
        this.docId = docId;
    }
    
}
