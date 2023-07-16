package com.kuzudb;

public class KuzuObjectRefDestroyedException extends Exception {
    public KuzuObjectRefDestroyedException(String errorMessage) {
        super(errorMessage);
    }
}
