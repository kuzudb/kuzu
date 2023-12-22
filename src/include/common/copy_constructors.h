#pragma once

// This file defines many macros for controlling copy constructors and move constructors on classes.

#define DELETE_COPY(Object) Object(const Object& other) = delete

#define DELETE_COPY_ASSN(Object) Object& operator=(const Object& other) = delete

#define DELETE_MOVE(Object)                                                                        \
    Object(Object&& other) = delete;                                                               \
    Object& operator=(Object&& other) = delete

#define ENABLE_MOVE(Object)                                                                        \
    Object(Object&& other) = default;                                                              \
    Object& operator=(Object&& other) = default

#define EXPLICIT_COPY_METHOD(Object)                                                               \
    Object copy() const { return *this; }

// EXPLICIT_COPY should be the default choice. It expects a PRIVATE copy constructor to be defined,
// which will be used by an explicit `copy()` method. For instance:
//
//   private:
//     MyClass(const MyClass& other) : field(other.field.copy()) {}
//
//   public:
//     EXPLICIT_COPY(MyClass);
//
// Now:
//
// MyClass o1;
// MyClass o2 = o1; // Compile error, copy assignment deleted.
// MyClass o2 = o1.copy(); // OK.
// MyClass o2(o1); // Compile error, copy constructor is private.
#define EXPLICIT_COPY(Object)                                                                      \
    DELETE_COPY_ASSN(Object);                                                                      \
    ENABLE_MOVE(Object);                                                                           \
    EXPLICIT_COPY_METHOD(Object)

// NO_COPY should be used for objects that for whatever reason, should never be copied.
#define NO_COPY(Object)                                                                            \
    DELETE_COPY(Object);                                                                           \
    DELETE_COPY_ASSN(Object);                                                                      \
    ENABLE_MOVE(Object)

// NO_MOVE_OR_COPY exists solely for explicitness, when an object cannot be moved nor copied. Any
// object containing a lock cannot be moved or copied.
#define NO_MOVE_OR_COPY(Object)                                                                    \
    DELETE_COPY(Object);                                                                           \
    DELETE_COPY_ASSN(Object);                                                                      \
    DELETE_MOVE(Object)
