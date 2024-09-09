import org.apache.spark.sql.api.java.UDF2;
import java.util.HashMap;
import java.util.Map;

public class VWAPStatefulUDF implements UDF2<String, Double, Double> {
    private static Map<String, Double> state = new HashMap<>();

    @Override
    public Double call(String symbol, Double currentVWAP) {
        // 直接使用 get()，當沒有上一筆資料時返回 null
        Double previousVWAP = state.get(symbol);
        state.put(symbol, currentVWAP); // 更新當前的 VWAP 值
        return previousVWAP; // 返回前一個 VWAP 值，若無則為 null
    }
}
