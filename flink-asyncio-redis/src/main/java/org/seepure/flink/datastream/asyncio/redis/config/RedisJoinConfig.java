package org.seepure.flink.datastream.asyncio.redis.config;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class RedisJoinConfig implements Serializable {

    private String mode;
    private String nodes;
    private String auth;

    private String joinRuleType;
    private String joinRuleLeftFields;
    private String joinRuleRightFields;

    private String cacheType;
    private Integer cacheExpireAfterWrite;
    private Integer cacheSize;
    private Boolean cacheNullable;

    private Integer batchSize;
    private Integer minBatchTime;

    private List<TableSchema> srcTableSchemas;

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getNodes() {
        return nodes;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    public String getAuth() {
        return auth;
    }

    public void setAuth(String auth) {
        this.auth = auth;
    }

    public String getJoinRuleType() {
        return joinRuleType;
    }

    public void setJoinRuleType(String joinRuleType) {
        this.joinRuleType = joinRuleType;
    }

    public String getJoinRuleLeftFields() {
        return joinRuleLeftFields;
    }

    public void setJoinRuleLeftFields(String joinRuleLeftFields) {
        this.joinRuleLeftFields = joinRuleLeftFields;
    }

    public String getJoinRuleRightFields() {
        return joinRuleRightFields;
    }

    public void setJoinRuleRightFields(String joinRuleRightFields) {
        this.joinRuleRightFields = joinRuleRightFields;
    }

    public String getCacheType() {
        return cacheType;
    }

    public void setCacheType(String cacheType) {
        this.cacheType = cacheType;
    }

    public Integer getCacheExpireAfterWrite() {
        return cacheExpireAfterWrite;
    }

    public void setCacheExpireAfterWrite(Integer cacheExpireAfterWrite) {
        this.cacheExpireAfterWrite = cacheExpireAfterWrite;
    }

    public Integer getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(Integer cacheSize) {
        this.cacheSize = cacheSize;
    }

    public Boolean getCacheNullable() {
        return cacheNullable;
    }

    public void setCacheNullable(Boolean cacheNullable) {
        this.cacheNullable = cacheNullable;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Integer getMinBatchTime() {
        return minBatchTime;
    }

    public void setMinBatchTime(Integer minBatchTime) {
        this.minBatchTime = minBatchTime;
    }

    public List<TableSchema> getSrcTableSchemas() {
        return srcTableSchemas;
    }

    public void setSrcTableSchemas(
            List<TableSchema> srcTableSchemas) {
        this.srcTableSchemas = srcTableSchemas;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RedisJoinConfig{");
        sb.append("mode='").append(mode).append('\'');
        sb.append(", nodes='").append(nodes).append('\'');
        sb.append(", auth='").append(auth).append('\'');
        sb.append(", joinRuleType='").append(joinRuleType).append('\'');
        sb.append(", joinRuleLeftFields='").append(joinRuleLeftFields).append('\'');
        sb.append(", joinRuleRightFields='").append(joinRuleRightFields).append('\'');
        sb.append(", cacheType='").append(cacheType).append('\'');
        sb.append(", cacheExpireAfterWrite=").append(cacheExpireAfterWrite);
        sb.append(", cacheSize=").append(cacheSize);
        sb.append(", cacheNullable=").append(cacheNullable);
        sb.append(", srcTableSchema=").append(srcTableSchemas);
        sb.append('}');
        return sb.toString();
    }

    public static class TableSchema implements Serializable {

        private String tableType;
        private String tableName;
        private String contentType;

        private Map<String, Object> formatSpec;

        private List<DataField> fields;

        public String getTableType() {
            return tableType;
        }

        public void setTableType(String tableType) {
            this.tableType = tableType;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getContentType() {
            return contentType;
        }

        public void setContentType(String contentType) {
            this.contentType = contentType;
        }

        public Map<String, Object> getFormatSpec() {
            return formatSpec;
        }

        public void setFormatSpec(Map<String, Object> formatSpec) {
            this.formatSpec = formatSpec;
        }

        public List<DataField> getFields() {
            return fields;
        }

        public void setFields(
                List<DataField> fields) {
            this.fields = fields;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("MetaSchema{");
            sb.append("tableType='").append(tableType).append('\'');
            sb.append(", tableName='").append(tableName).append('\'');
            sb.append(", contentType='").append(contentType).append('\'');
            sb.append(", formatSpec=").append(formatSpec);
            sb.append(", fields=").append(fields);
            sb.append('}');
            return sb.toString();
        }

        public static class DataField implements Serializable {

            private Integer id;
            private String fieldKey;
            private String fieldName;
            private String fieldType;
            private short fieldIndex;

            public Integer getId() {
                return id;
            }

            public void setId(Integer id) {
                this.id = id;
            }

            public String getFieldKey() {
                return fieldKey;
            }

            public void setFieldKey(String fieldKey) {
                this.fieldKey = fieldKey;
            }

            public String getFieldName() {
                return fieldName;
            }

            public void setFieldName(String fieldName) {
                this.fieldName = fieldName;
            }

            public String getFieldType() {
                return fieldType;
            }

            public void setFieldType(String fieldType) {
                this.fieldType = fieldType;
            }

            public short getFieldIndex() {
                return fieldIndex;
            }

            public void setFieldIndex(short fieldIndex) {
                this.fieldIndex = fieldIndex;
            }

            @Override
            public String toString() {
                final StringBuilder sb = new StringBuilder("DataField{");
                sb.append("id=").append(id);
                sb.append(", fieldKey='").append(fieldKey).append('\'');
                sb.append(", fieldName='").append(fieldName).append('\'');
                sb.append(", fieldType='").append(fieldType).append('\'');
                sb.append(", fieldIndex=").append(fieldIndex);
                sb.append('}');
                return sb.toString();
            }
        }

    }
}
