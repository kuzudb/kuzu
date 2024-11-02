package com.kuzudb;

/**
 * ObjectRefDestroyedException is thrown when a destroyed object is accessed
 * or destroyed again.
 *
 * @see Connection#destroy()
 */
public class ObjectRefDestroyedException extends Exception {
    public ObjectRefDestroyedException(String errorMessage) {
        super(errorMessage);
    }
}
