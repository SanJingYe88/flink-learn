package it.com.util;

import java.util.HashMap;
import java.util.Map;

/**
 * map展开
 */
public class NestedMapFlattenerUtils {

    private static final String SEPARATOR = ".";

    public static Map<String, Object> flattenJson(String json) {
        HashMap map = JsonUtils.toBean(json, HashMap.class);
        return flattenMap(map);
    }

    public static Map<String, Object> flattenMap(Map<String, Object> map) {
        Map<String, Object> flattenedMap = new HashMap<>();
        flattenMap(map, "", flattenedMap);
        return flattenedMap;
    }

    private static void flattenMap(Map<String, Object> map, String prefix, Map<String, Object> flattenedMap) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = prefix + entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map) {
                flattenMap((Map<String, Object>) value, key + SEPARATOR, flattenedMap);
            } else {
                flattenedMap.put(key, value);
            }
        }
    }

    public static void main(String[] args) {
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("level1", "value1");
        Map<String, Object> level2 = new HashMap<>();
        level2.put("level2a", "value2a");
        level2.put("level2b", "value2b");
        nestedMap.put("level2", level2);
        Map<String, Object> level3 = new HashMap<>();
        level3.put("level3a", "value3a");
        level2.put("level3", level3);

        Map<String, Object> flattenedMap = flattenMap(nestedMap);
        flattenedMap.forEach((k, v) -> System.out.println(k + " : " + v));
    }
}
