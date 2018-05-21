package com.idealista.fpe.config;

import java.io.Serializable;

public interface Domain extends Serializable {
    Alphabet alphabet();
    int[] transform(String data);
    String transform(int[] data);
}
