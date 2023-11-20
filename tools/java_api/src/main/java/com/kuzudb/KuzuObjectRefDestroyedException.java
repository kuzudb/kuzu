package com.kuzudb;

/**
 * KuzuObjectRefDestroyedException is thrown when a destroyed object is accessed
 * or destroyed again.
 * @see KuzuConnection#destroy()
 */
public class KuzuObjectRefDestroyedException extends Exception {
    public KuzuObjectRefDestroyedException(String errorMessage) {
        super(errorMessage);
    }
}
