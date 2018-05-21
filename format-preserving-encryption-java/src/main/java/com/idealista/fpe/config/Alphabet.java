package com.idealista.fpe.config;

import java.io.Serializable;

public interface Alphabet extends Serializable {

    char[] availableCharacters();
    Integer radix();

}
