package mongoflink;

import mongoflink.serde.DocumentSerializer;
import org.bson.Document;

/**
 * @author chenzhuoyu
 * @date 2021/10/21 13:15
 */
public class StringDocumentSerializer implements DocumentSerializer<String> {

    @Override
    public Document serialize(String string) {
        Document document = new Document();
        String[] elements = string.split(",");
        document.append("_id", elements[0]);
        document.append("count", Integer.parseInt(elements[1]));
        return document;
    }
}
