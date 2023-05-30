#pragma once

#include <stdexcept>
#include <string>

namespace kuzu {
namespace common {

class Exception : public std::exception {
public:
    explicit Exception(std::string msg) : exception(), exception_message_(std::move(msg)){};

public:
    const char* what() const noexcept override { return exception_message_.c_str(); }

    // TODO(Guodong): this is being used in both loader and node table. A better way to do this
    // could be throw this error msg during insert.
    static std::string getExistedPKExceptionMsg(const std::string& pkString) {
        auto result = "A node is created with an existed primary key " + pkString +
                      ", which violates the uniqueness constraint of the primary key property.";
        return result;
    };

private:
    std::string exception_message_;
};

class ParserException : public Exception {
public:
    explicit ParserException(const std::string& msg) : Exception("Parser exception: " + msg){};
};

class BinderException : public Exception {
public:
    explicit BinderException(const std::string& msg) : Exception("Binder exception: " + msg){};
};

class ConversionException : public Exception {
public:
    explicit ConversionException(const std::string& msg) : Exception(msg){};
};

class CopyException : public Exception {
public:
    explicit CopyException(const std::string& msg) : Exception("Copy exception: " + msg){};
};

class CatalogException : public Exception {
public:
    explicit CatalogException(const std::string& msg) : Exception("Catalog exception: " + msg){};
};

class HashIndexException : public Exception {
public:
    explicit HashIndexException(const std::string& msg)
        : Exception("HashIndex exception: " + msg){};
};

class StorageException : public Exception {
public:
    explicit StorageException(const std::string& msg) : Exception("Storage exception: " + msg){};
};

class BufferManagerException : public Exception {
public:
    explicit BufferManagerException(const std::string& msg)
        : Exception("Buffer manager exception: " + msg){};
};

class InternalException : public Exception {
public:
    explicit InternalException(const std::string& msg) : Exception(msg){};
};

class NotImplementedException : public Exception {
public:
    explicit NotImplementedException(const std::string& msg) : Exception(msg){};
};

class RuntimeException : public Exception {
public:
    explicit RuntimeException(const std::string& msg) : Exception("Runtime exception: " + msg){};
};

class ConnectionException : public Exception {
public:
    explicit ConnectionException(const std::string& msg) : Exception(msg){};
};

class TransactionManagerException : public Exception {
public:
    explicit TransactionManagerException(const std::string& msg) : Exception(msg){};
};

class InterruptException : public Exception {
public:
    explicit InterruptException() : Exception("Interrupted."){};
};

class TestException : public Exception {
public:
    explicit TestException(const std::string& msg) : Exception("Test exception: " + msg){};
};

} // namespace common
} // namespace kuzu
