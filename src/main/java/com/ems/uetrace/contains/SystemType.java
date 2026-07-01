package com.ems.uetrace.contains;

public enum SystemType {
    FOUR_GA("4ga"),

    FIVE_GA("5ga");

    public final String alias;

    SystemType(String alias) {
        this.alias = alias;
    }

    public static SystemType fromRatType(String ratType) {
        if (ratType == null) return null;
        return switch (ratType.toUpperCase()) {
            case "NR_FDD", "NR_TDD" -> FIVE_GA;
            case "LTE_FDD", "LTE_TDD" -> FOUR_GA;
            default -> null;
        };
    }
}
