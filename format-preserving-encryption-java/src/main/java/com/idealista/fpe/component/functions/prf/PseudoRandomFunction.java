package com.idealista.fpe.component.functions.prf;


import java.io.Serializable;

public interface PseudoRandomFunction extends Serializable {
    byte[] apply(byte[] text);
}
