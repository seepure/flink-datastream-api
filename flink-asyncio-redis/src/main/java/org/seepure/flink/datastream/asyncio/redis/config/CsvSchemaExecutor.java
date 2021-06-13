package org.seepure.flink.datastream.asyncio.redis.config;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.seepure.flink.datastream.asyncio.redis.config.RedisJoinConfig.TableSchema;
import org.seepure.flink.datastream.asyncio.redis.config.RedisJoinConfig.TableSchema.DataField;
import org.seepure.flink.datastream.asyncio.redis.util.AssertUtil;

public class CsvSchemaExecutor extends TableSchemaExecutor<String> {

    private static final String[] EMPTY_STRING_ARRAY = new String[0];
    private static final ThreadLocal<SimpleDateFormat> SDF_THL = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyyMMddHH");
        }
    };
    private String charset;
    private String separator;
    private List<DataField> fields;
    private String[] indexToFieldKey;

    @Override
    public void parseConfig(TableSchema tableSchema) {
        AssertUtil.assertTrue(tableSchema != null, "get empty tableSchema!");
        Map<String, Object> formatSpec = tableSchema.getFormatSpec();
        charset = (String) formatSpec.getOrDefault("encoding", "UTF-8");
        String separator = (String) formatSpec.get("separator");
        AssertUtil.assertTrue(separator != null, "formatSpec.separator is null !");
        this.separator = separator;
        fields = tableSchema.getFields();
        AssertUtil.assertTrue(CollectionUtils.isNotEmpty(fields), "tableSchema.getFields() is empty!");
        int maxIndex = 0;
        for (DataField field : fields) {
            if (field.getFieldIndex() > maxIndex) {
                maxIndex = field.getFieldIndex();
            }
        }
        AssertUtil.assertTrue(maxIndex <= 1000, "maximum of index is too large.");
        String[] indexToFieldKey = new String[maxIndex + 1];
        for (DataField field : fields) {
            indexToFieldKey[field.getFieldIndex()] = field.getFieldKey();
        }
        this.indexToFieldKey = indexToFieldKey;
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
        if (!map.containsKey("ds")) {
            map.put("ds", SDF_THL.get().format(new Date()));
        }
        if (!map.containsKey("__rand_key")) {
            Random random = new Random();
            map.put("__rand_key", String.valueOf(random.nextInt(200_000_000)));
        }
        return map;
    }

    @Override
    public Map<String, String> parseBytes(byte[] bytes) {
        try {
            String content = new String(bytes, charset);
            return parseInput(content);
        } catch (UnsupportedEncodingException e) {
            return Collections.emptyMap();
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
