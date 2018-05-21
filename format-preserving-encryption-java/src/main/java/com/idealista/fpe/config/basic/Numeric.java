package com.idealista.fpe.config.basic;

import com.idealista.fpe.config.Alphabet;

import java.io.Serializable;

public class Numeric implements Alphabet, Serializable {

    private static final char[] LOWER_CASE_CHARS = new char[] {'1','2','3','4','5','6','7','8','9','0'};

    @Override
    public char[] availableCharacters() {
        return LOWER_CASE_CHARS;
    }

    @Override
    public Integer radix() {
        return LOWER_CASE_CHARS.length;
    }
}
