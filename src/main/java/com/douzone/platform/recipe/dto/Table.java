package com.douzone.platform.recipe.dto;

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
public class Table implements Expression{
    private String name;
    private String alias;
}
