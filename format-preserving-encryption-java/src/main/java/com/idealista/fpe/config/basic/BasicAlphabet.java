package com.idealista.fpe.config.basic;

import com.idealista.fpe.config.Alphabet;

import java.io.Serializable;


public class BasicAlphabet implements Alphabet, Serializable {

    private static final char[] LOWER_CASE_CHARS = new char[] {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h','i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C','D','E','F','G','H','I','J',' ','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'};

    @Override
    public char[] availableCharacters() {
        return LOWER_CASE_CHARS;
    }

    @Override
    public Integer radix() {
        return LOWER_CASE_CHARS.length;
    }
}
