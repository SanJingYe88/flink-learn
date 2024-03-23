package it.com.util;


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

@Slf4j
public final class JsonUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        // 控制着当JSON输入包含Java对象没有的属性时，反序列化过程是否应该失败。
        // 默认情况下，这个特性是启用的，这意味着如果JSON字符串包含了一个Java对象中没有映射的字段，反序列化过程将会抛出一个UnrecognizedPropertyException异常。
        // 如果你希望忽略JSON中多余的属性，而不是让反序列化失败，你可以禁用这个特性。
        OBJECT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        // 用于控制JSON输出的格式化。
        // 当这个特性被启用时，生成的JSON字符串将会以一种易读的方式格式化，包含缩进和换行符，使得输出的JSON结构更加清晰和易于阅读。
        // 默认情况下，这个特性是禁用的，因此生成的JSON通常是紧凑的，没有额外的空白和换行。
        OBJECT_MAPPER.disable(SerializationFeature.INDENT_OUTPUT);
        // 控制着反序列化过程中反斜杠（\）字符的使用。在JSON中，反斜杠通常用于转义特殊字符，如引号（"）、反斜杠本身（\）、换行符（\n）等。
        // 然而，在某些情况下，可能会遇到使用反斜杠转义其他字符的情况，这在标准的JSON中是不允许的。
        // 启用该特性后，Jackson将允许在JSON字符串中使用反斜杠来转义任何字符，而不仅仅是标准的转义序列。这提供了更宽松的处理方式，可以处理那些不符合严格JSON规范的数据。
        // 已经在Jackson库的较新版本中被弃用了。这是因为允许反斜杠转义任何字符可能会引入安全问题，特别是当处理不受信任的输入时。弃用此特性是为了鼓励开发者使用更安全的JSON处理方式。
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        // 控制着JSON解析器如何处理非标准的数字格式。在标准的JSON中，数字应该是标准的十进制、十六进制或浮点表示。
        // 然而，有些JSON生成器可能会生成非标准的数字格式，例如使用逗号作为小数点分隔符的浮点数（这在某些地区是常见的表示方法）。
        // 启用特性后，Jackson的JSON解析器将会更加宽容地处理这些非标准的数字格式，尝试将它们解析为有效的数值。这可以使得Jackson能够处理来自不同区域或使用了不同数字表示法的数据源。
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    private JsonUtils() {
    }

    public static <T> T toBean(String json, Class<T> t) {
        if (StringUtils.isEmpty(json)) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(json, t);
        } catch (Exception e) {
            log.error("toBean", e);
        }
        return null;
    }

    public static String toJson(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof String) {
            return o.toString();
        }
        try {
            return OBJECT_MAPPER.writeValueAsString(o);
        } catch (Exception e) {
            log.error("toJson", e);
        }
        return null;
    }
}

