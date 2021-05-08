package org.seepure.flink.datastream.asyncio.redis.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.seepure.flink.datastream.asyncio.redis.util.AssertUtil;
import org.seepure.flink.datastream.asyncio.redis.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTextSourceSchema extends SourceSchema {

    private final static Logger LOG = LoggerFactory.getLogger(MQTextSourceSchema.class);
    private static final String[] EMPTY_STRING_ARRAY = new String[0];
    private String charset;
    private String separator;
    private List<Field> fields;
    private String[] indexToFieldKey;

    @Override
    public void parseConfig(String configContent) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        configContent = StringEscapeUtils.unescapeJson(configContent);
        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(configContent);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
        }
        if (jsonNode == null) {
            configContent = new String(Base64.getDecoder().decode(configContent.getBytes("UTF8")), "UTF8");
            configContent = StringEscapeUtils.unescapeJson(configContent);
            jsonNode = objectMapper.readTree(configContent);
        }
        separator = JsonUtil.getStringOrDefault(jsonNode, "separator", "|");
        charset = JsonUtil.getStringOrDefault(jsonNode, "encoding", "UTF8");
        JsonNode schemaNode = JsonUtil.getJsonNode(jsonNode, "schema");
        JavaType javaType = objectMapper.getTypeFactory().constructCollectionType(List.class, Field.class);
        fields = objectMapper.readValue(schemaNode.toString(), javaType);
        AssertUtil.assertTrue(CollectionUtils.isNotEmpty(fields), "empty schema!");
        int maxIndex = 0;
        for (Field field : fields) {
            if (field.getFieldIndex() > maxIndex) {
                maxIndex = field.getFieldIndex();
            }
        }
        AssertUtil.assertTrue(maxIndex <= 1000, "maximum of index is too large.");
        String[] indexToFieldKey = new String[maxIndex + 1];
        for (Field field : fields) {
            indexToFieldKey[field.getFieldIndex()] = field.getFieldKey();
        }
        this.indexToFieldKey = indexToFieldKey;
    }

    @Override
    public Map<String, String> parseInput(byte[] bytes) {
        try {
            String content = new String(bytes, charset);
            return parseInput(content);
        } catch (UnsupportedEncodingException e) {
            return Collections.emptyMap();
        }
    }

    @Override
    public Map<String, String> parseInput(String input) {
        if (StringUtils.isBlank(input)) {
            return Collections.emptyMap();
        }
        Map<String, String> map = new LinkedHashMap<>();
        String[] msgs = null;
        msgs = split(input, separator);
        if (msgs == null || msgs.length == 1) {
            msgs = input.split(separator);
        }
        for (int i = 0; i < msgs.length; i++) {
            if (i >= indexToFieldKey.length) {
                break;
            }
            if (indexToFieldKey[i] == null) {
                continue;
            }
            map.put(indexToFieldKey[i], msgs[i]);
        }
        return map;
    }

    public static class Field {

        private String fieldKey;
        private int fieldIndex;

        public String getFieldKey() {
            return fieldKey;
        }

        public void setFieldKey(String fieldKey) {
            this.fieldKey = fieldKey;
        }

        public int getFieldIndex() {
            return fieldIndex;
        }

        public void setFieldIndex(int fieldIndex) {
            this.fieldIndex = fieldIndex;
        }
    }

    public static String[] split(String str, String separatorChars) {
        return splitWorker(str, separatorChars, -1, true);
    }

    private static String[] splitWorker(String str, String separatorChars, int max, boolean preserveAllTokens) {

        if (str == null) {
            return null;
        }
        int len = str.length();
        if (len == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new ArrayList<String>();
        int sizePlus1 = 1;
        boolean match = false;
        boolean lastMatch = false;
        int i = 0;
        int start = 0;
        if (separatorChars == null) {
            // Null separator means use whitespace
            while (i < len) {
                if (Character.isWhitespace(str.charAt(i))) {
                    if (match || preserveAllTokens) {
                        lastMatch = true;
                        if (sizePlus1++ == max) {
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                lastMatch = false;
                match = true;
                i++;
            }
        } else if (separatorChars.length() == 1) {
            // Optimise 1 character case
            char sep = separatorChars.charAt(0);
            while (i < len) {
                if (str.charAt(i) == sep) {
                    if (match || preserveAllTokens) {
                        lastMatch = true;
                        if (sizePlus1++ == max) {
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                lastMatch = false;
                match = true;
                i++;
            }
        } else {
            // standard case
            while (i < len) {
                if (separatorChars.indexOf(str.charAt(i)) >= 0) {
                    if (match || preserveAllTokens) {
                        lastMatch = true;
                        if (sizePlus1++ == max) {
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                lastMatch = false;
                match = true;
                i++;
            }
        }
        if (match || preserveAllTokens && lastMatch) {
            list.add(str.substring(start, i));
        }
        return list.toArray(new String[list.size()]);
    }

}
