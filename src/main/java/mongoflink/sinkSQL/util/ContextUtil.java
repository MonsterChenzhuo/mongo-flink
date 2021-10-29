package mongoflink.sinkSQL.util;

import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.Map;

/**
 * @author chenzhuoyu
 * @date 2021/9/17 22:13
 */
public class ContextUtil {
    public static void transformContext(DynamicTableFactory factory, DynamicTableFactory.Context context) {
        Map<String, String> catalogOptions = context.getCatalogTable().getOptions();

        Map<String, String> convertedOptions = FactoryOptionUtil.normalizeOptionCaseAsFactory(factory, catalogOptions);

        catalogOptions.clear();
        for (Map.Entry<String, String> entry : convertedOptions.entrySet()) {
            catalogOptions.put(entry.getKey(), entry.getValue());
        }
    }

}
