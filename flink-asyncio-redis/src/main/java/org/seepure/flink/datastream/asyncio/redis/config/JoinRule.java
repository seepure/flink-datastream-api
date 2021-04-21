package org.seepure.flink.datastream.asyncio.redis.config;

import akka.actor.FSM.$minus$greater$;
import java.io.Serializable;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JoinRule implements Serializable {
    private String type;
    private List<String> leftFields;
    private List<String> rightFields;
    private List<String> leftOutputFields;
    private List<String> rightOutputFields;

    public static JoinRule parseJoinRule(Map<String, String> configMap) {
        JoinRule joinRule = new JoinRule();
        joinRule.type = configMap.getOrDefault("joinRule.type", "full_join");
        String leftString = configMap.getOrDefault("joinRule.leftFields", "mykey");
        String rightString = configMap.get("joinRule.rightFields");
        String leftOutputString = configMap.get("joinRule.leftOutputFields");
        String rightOutputString = configMap.get("joinRule.rightOutputFields");
        if (StringUtils.isBlank(leftString) || StringUtils.isBlank(rightString)) {
            throw new IllegalArgumentException("parseJoinRule error!");
        }
        joinRule.leftFields = Arrays.asList(leftString.trim().split(","));
        joinRule.rightFields = Arrays.asList(rightString.trim().split(","));
        joinRule.leftOutputFields = leftOutputString != null ? Arrays.asList(leftOutputString.trim().split(",")) : null;
        joinRule.rightOutputFields = rightOutputString != null ? Arrays.asList(rightOutputString.trim().split(",")) : null;
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
