package org.seepure.flink.datastream.asyncio.redis.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class JoinRule implements Serializable {

    public static final String LEFT_JOIN_TYPE = "left_join";
    public static final String INNER_JOIN_TYPE = "inner_join";
    private String type;
    private List<String> leftFields;
    private List<String> rightFields;

    public static JoinRule parseJoinRule(Map<String, String> configMap) {
        JoinRule joinRule = new JoinRule();
        joinRule.type = configMap.get("joinRule.type");
        if (StringUtils.isBlank(joinRule.type)) {
            joinRule.type = configMap.getOrDefault("joinRuleType", LEFT_JOIN_TYPE);
        }
        if (!LEFT_JOIN_TYPE.equalsIgnoreCase(joinRule.type) && !INNER_JOIN_TYPE.equalsIgnoreCase(joinRule.type)) {
            throw new IllegalArgumentException("joinRuleType/joinRule.type is illegal, value=" + joinRule.type);
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

    public static JoinRule parseJoinRule(RedisJoinConfig redisJoinConfig) {
        if (!LEFT_JOIN_TYPE.equalsIgnoreCase(redisJoinConfig.getJoinRuleType()) && !INNER_JOIN_TYPE
                .equalsIgnoreCase(redisJoinConfig.getJoinRuleType())) {
            throw new IllegalArgumentException(
                    "joinRuleType/joinRule.type is illegal, value=" + redisJoinConfig.getJoinRuleType());
        }
        JoinRule joinRule = new JoinRule();
        joinRule.type = redisJoinConfig.getJoinRuleType();
        if (StringUtils.isBlank(redisJoinConfig.getJoinRuleLeftFields()) || StringUtils
                .isBlank(redisJoinConfig.getJoinRuleRightFields())) {
            throw new IllegalArgumentException(
                    "parseJoinRule error! leftString=[" + redisJoinConfig.getJoinRuleLeftFields() + "], rightString=["
                            + redisJoinConfig.getJoinRuleRightFields() + "]");
        }
        joinRule.leftFields = Arrays.asList(redisJoinConfig.getJoinRuleLeftFields().trim().split(","));
        joinRule.rightFields = Arrays.asList(redisJoinConfig.getJoinRuleRightFields().trim().split(","));
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

}
