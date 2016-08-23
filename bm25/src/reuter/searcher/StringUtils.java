package reuter.searcher;



import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.hadoop.io.BytesWritable;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

/**
 *
 * @author hadoop
 */
public class StringUtils {

    private static final String[] punctuation = {"`", "&", "'", "*", "\\", "{", "}", "[",
        "]", ":", ",", "!", ">", "<", "#", "(", ")", "%", ".",
        "+", "?", "\"", ";", "/", "^", "", "|", "~", "+","$"};
    private static final String[] stop_words = {"a", "an", "and", "are", "as", "at", "be", "by",
        "for", "from", "has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
        "to", "was", "were", "will", "with"};

    public static String normalizeText(String str) {
        //replace punctuation
        for (String punc : punctuation) {
            str = str.replace(punc, "");
        }
        
        //replace numbers
        str = str.replaceAll("\\d+.*", "");
        
        str = str.toLowerCase();
        //replace stop words
        for (String stop_word : stop_words) {
            str = str.replace(" " + stop_word + " ", " ");
        }

        str = str.replaceAll("-+", " ").replaceAll(" +", "#")
                .replaceAll("\n+", "#").replaceAll("\t+", "#")
                .replaceAll("\r+", "#").replaceAll("#+", "::");
        
        //System.out.println(str);
        return str.trim();
    }

    public static ReutersDoc getXMLContent(BytesWritable xmlStr) {
        //get the xml factory and parse the xml file
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document dom;
        StringBuilder docContent = new StringBuilder();
        ReutersDoc doc = new ReutersDoc();
        try {
            //Using factory get an instance of document builder
            DocumentBuilder db = dbf.newDocumentBuilder();

            //parse using builder to get DOM representation of the XML file
            dom = db.parse(new ByteArrayInputStream(xmlStr.getBytes()));

            //now parse document
            //get document id
            Element root = dom.getDocumentElement();
            doc.setDocID(root.getAttribute("itemid"));

            //get title and headline and body of xml data
            docContent.append(root.getElementsByTagName("title").item(0).getTextContent().trim()).append(" ");
            docContent.append(root.getElementsByTagName("headline").item(0).getTextContent().trim()).append(" ");
            docContent.append(root.getElementsByTagName("text").item(0).getTextContent().trim());

            doc.setContent(docContent.toString());
        } catch (ParserConfigurationException | SAXException | IOException pce) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, pce);
        }

        return doc;
    }
}
