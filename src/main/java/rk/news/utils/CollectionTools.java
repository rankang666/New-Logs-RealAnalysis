package rk.news.utils;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @Author rk
 * @Date 2018/12/3 19:18
 * @Description:
 **/
public class CollectionTools {
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map){
        Map<K, V> result = new LinkedHashMap<>();
        Stream<Map.Entry<K, V>> st = map.entrySet().stream();
        st.sorted(Comparator.comparing(e -> e.getValue()))
                .forEach(e -> result.put(e.getKey(), e.getValue()));
        return result;
    }
}
