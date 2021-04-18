package org.seepure.flink.datastream.asyncio.redis.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;

public class JsonUtil {

    public static JsonNode getJsonNode(JsonNode jsonNode, String pathExpression) {
        if (StringUtils.isEmpty(pathExpression)) {
            return null;
        }
        return getJsonNode(jsonNode, StringUtils.split(pathExpression, '.'));
    }

    public static JsonNode getJsonNode(JsonNode jsonNode, String[] paths) {
        if (jsonNode == null || paths == null || paths.length == 0) {
            return null;
        }
        for (int i = 0; i < paths.length; i++) {
            jsonNode = jsonNode.get(paths[i]);
            if (jsonNode == null) {
                return null;
            }
        }
        return jsonNode;
    }

    public static String getStringOrDefault(JsonNode jsonNode, String pathExpression, String defaultValue) {
        String s = getString(jsonNode, StringUtils.split(pathExpression, '.'));
        return s != null ? s : defaultValue;
    }

    public static String getString(JsonNode jsonNode, String pathExpression) {
        return getString(jsonNode, StringUtils.split(pathExpression, '.'));
    }

    public static String getString(JsonNode jsonNode, String[] paths) {
        jsonNode = getJsonNode(jsonNode, paths);
        if (jsonNode == null) {
            return null;
        }
        return jsonNode.isValueNode() ? jsonNode.asText() : jsonNode.toString();
    }
}
