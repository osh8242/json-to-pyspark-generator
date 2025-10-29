package com.douzone.platform.recipe.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 10. 28.        osh8242       최초 생성
 */
class StringUtilTest {
    @Test
    void pyString_escapesControlCharacters() {
        assertThat(StringUtil.pyString("line1\nline2"))
                .isEqualTo("\"line1\\nline2\"");

        assertThat(StringUtil.pyString("\tab\t"))
                .isEqualTo("\"\\tab\\t\"");

        assertThat(StringUtil.pyString("\u0001"))
                .isEqualTo("\"\\u0001\"");
    }

    @Test
    void pyString_handlesQuotesAndBackslash() {
        assertThat(StringUtil.pyString("He said \"Hi\""))
                .isEqualTo("\"He said \\\"Hi\\\"\"");
        assertThat(StringUtil.pyString("C:\\temp\\file.txt"))
                .isEqualTo("\"C:\\\\temp\\\\file.txt\"");
        assertThat(StringUtil.pyString("mix\"'\\"))
                .isEqualTo("\"mix\\\"'\\\\\"");
    }

    @Test
    void pyString_handlesNullAndEmpty() {
        assertThat(StringUtil.pyString(null))
                .isEqualTo("None");
        assertThat(StringUtil.pyString(""))
                .isEqualTo("\"\"");
    }

    @Test
    void pyString_masksOtherControls() {
        assertThat(StringUtil.pyString("\r\n\t"))
                .isEqualTo("\"\\r\\n\\t\"");
        assertThat(StringUtil.pyString("\u0000\u001f"))
                .isEqualTo("\"\\u0000\\u001f\"");
    }


}