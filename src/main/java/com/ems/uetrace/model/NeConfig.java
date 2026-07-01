package com.ems.uetrace.model;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Data
@Component
@ConfigurationProperties(prefix = "system")
public class NeConfig {
    private SystemConfig fourGa = new SystemConfig();
    private SystemConfig fiveGa = new SystemConfig();

    @Data
    public static class SystemConfig {
        private boolean enabled = true;
        private String mariadbDbName;
        private String clickhouseDbName;
        private List<ExtraFieldDef> extraField = new ArrayList<>();
    }

    @Data
    public static class CounterDef {
        private int id;
        private String column;
        public String resolvedColumn(){
            return column != null && !column.isBlank() ? column : "c_" + id;
        }
    }

    @Data
    public static class KpiDef {
        private int id;
        private String formula;
        private String column;
        public String resolvedColumn(){
            return column != null && !column.isBlank() ? column : "k_" + id;
        }
    }

    @Data
    public static class ExtraFieldDef {
        private String columnName;
        private String columnCode;
        private boolean nullable = true;
        private String columnType = "string";
        private String defaultValue;
        public String resolvedColumnCode(){
            return columnCode != null && !columnCode.isBlank() ? columnCode : columnName;
        }
    }

}
