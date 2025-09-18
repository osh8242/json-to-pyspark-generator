package com.douzone.platform.recipe.dto;

import com.douzone.platform.util.FormatUtil;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 9. 15.        osh8242       최초 생성
 */
@Getter
@Setter
public class PysparkRecipe {
    private String table;
    private List<Step> steps = new ArrayList<>();
    private Option options;

    @Override
    public String toString() {
        return FormatUtil.getJson(this, true);
    }

    @Getter
    @Setter
    public static class Option {
        private Boolean cache;

        @Override
        public String toString() {
            return FormatUtil.getJson(this);
        }
    }

    @Getter
    @Setter
    public static class Step {
        private String op;
        private JsonNode args;

        @Override
        public String toString() {
            return FormatUtil.getJson(this);
        }
    }
}
