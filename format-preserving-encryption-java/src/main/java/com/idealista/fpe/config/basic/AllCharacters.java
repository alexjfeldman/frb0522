package com.idealista.fpe.config.basic;

import com.idealista.fpe.config.Alphabet;

import java.io.Serializable;

public class AllCharacters implements Alphabet, Serializable {

    private static final char[] All_Character = new char[] {'-', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', ' ', '\'', '`', ',', '.', '~',
 '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '=', '+', '{', '}', '[', ']', ':', ';', '"', '<', '>', '?',
 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};

    @Override
    public char[] availableCharacters() {
        return All_Character;
    }

    @Override
    public Integer radix() {
        return All_Character.length;
    }
}
