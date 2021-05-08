package org.seepure.flink.datastream.asyncio.redis.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class JoinRule implements Serializable {
    private String type;
    private List<String> leftFields;
    private List<String> rightFields;
    private List<String> leftOutputFields;
    private List<String> rightOutputFields;

    public static JoinRule parseJoinRule(Map<String, String> configMap) {
        JoinRule joinRule = new JoinRule();
        joinRule.type = configMap.get("joinRule.type");
        if (StringUtils.isBlank(joinRule.type)) {
            joinRule.type = configMap.getOrDefault("joinRuleType", "full_join");
        }
        String leftString = configMap.get("joinRule.leftFields");
        if (StringUtils.isBlank(leftString)) {
            leftString = configMap.get("joinRuleLeftFields");
        }
        String rightString = configMap.get("joinRule.rightFields");
        if (StringUtils.isBlank(rightString)) {
            rightString = configMap.get("joinRuleRightFields");
        }
        if (StringUtils.isBlank(leftString) || StringUtils.isBlank(rightString)) {
            ObjectMapper objectMapper = new ObjectMapper();
            String configMapString = null;
            try {
                configMapString = objectMapper.writeValueAsString(configMap);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            if (StringUtils.isBlank(configMapString)) {
                configMapString = String.valueOf(configMap);
            }
            throw new IllegalArgumentException(
                    "parseJoinRule error! leftString=[" + leftString + "], rightString=[" + rightString
                            + "], configMap: " + configMapString);
        }
        joinRule.leftFields = Arrays.asList(leftString.trim().split(","));
        joinRule.rightFields = Arrays.asList(rightString.trim().split(","));
        return joinRule;
    }

    public String getType() {
        return type;
    }

    public List<String> getLeftFields() {
        return leftFields;
    }

    public void setLeftFields(List<String> leftFields) {
        this.leftFields = leftFields;
    }

    public List<String> getRightFields() {
        return rightFields;
    }

    public void setRightFields(List<String> rightFields) {
        this.rightFields = rightFields;
    }

    public List<String> getLeftOutputFields() {
        return leftOutputFields;
    }

    public void setLeftOutputFields(List<String> leftOutputFields) {
        this.leftOutputFields = leftOutputFields;
    }

    public List<String> getRightOutputFields() {
        return rightOutputFields;
    }

    public void setRightOutputFields(List<String> rightOutputFields) {
        this.rightOutputFields = rightOutputFields;
    }
}
