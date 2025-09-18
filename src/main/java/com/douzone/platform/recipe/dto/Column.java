package com.douzone.platform.recipe.dto;

import com.douzone.platform.util.FormatUtil;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;
import lombok.Setter;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 9. 16.        osh8242       최초 생성
 */
@Getter
@Setter
public class Column implements Expression{
    private String name;
    private String alias;
    public static Column parse(JsonNode node) {
        return null;
    }

    @Override
    public String toString() {
        return FormatUtil.getJson(this);
    }
}
