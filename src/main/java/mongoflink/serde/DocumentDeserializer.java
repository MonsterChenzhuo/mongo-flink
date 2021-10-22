package mongoflink.serde;

import org.bson.Document;

import java.io.Serializable;

/**
 * @author chenzhuoyu
 * @date 2021/9/17 22:13
 * DocumentDeserializer deserialize {@link Document} into POJOs or other Java objects .
 **/
public interface DocumentDeserializer<T> extends Serializable {

    /**
     * Serialize input Java objects into {@link Document}.
     * @param document The input {@link Document}.
     * @return The serialized object.
     */
     T deserialize(Document document);

}
