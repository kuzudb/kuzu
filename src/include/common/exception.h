#pragma once

#include <stdexcept>
#include <string>

using namespace std;

namespace kuzu {
namespace common {

class Exception : public exception {
public:
    explicit Exception(string msg) : exception(), exception_message_(move(msg)){};

public:
    const char* what() const noexcept override { return exception_message_.c_str(); }

    // TODO(Guodong): this is being used in both loader and node table. A better way to do this
    // could be throw this error msg during insert.
    static string getExistedPKExceptionMsg(const string& pkString) {
        auto result = "A node is created with an existed primary key " + pkString +
                      ", which violates the uniqueness constraint of the primary key property.";
        return result;
    };

private:
    std::string exception_message_;
};

class ParserException : public Exception {
public:
    explicit ParserException(const string& msg) : Exception("Parser exception: " + msg){};
};

class BinderException : public Exception {
public:
    explicit BinderException(const string& msg) : Exception("Binder exception: " + msg){};
};

class ConversionException : public Exception {
public:
    explicit ConversionException(const string& msg) : Exception(msg){};
};

class CSVReaderException : public Exception {
public:
    explicit CSVReaderException(const string& msg) : Exception("CSVReader exception: " + msg){};
};

class CopyCSVException : public Exception {
public:
    explicit CopyCSVException(const string& msg) : Exception("CopyCSV exception: " + msg){};
};

class CatalogException : public Exception {
public:
    explicit CatalogException(const string& msg) : Exception("Catalog exception: " + msg){};
};

class StorageException : public Exception {
public:
    explicit StorageException(const string& msg) : Exception("Storage exception: " + msg){};
};

class BufferManagerException : public Exception {
public:
    explicit BufferManagerException(const string& msg)
        : Exception("Buffer manager exception: " + msg){};
};

class InternalException : public Exception {
public:
    explicit InternalException(const string& msg) : Exception(msg){};
};

class NotImplementedException : public Exception {
public:
    explicit NotImplementedException(const string& msg) : Exception(msg){};
};

class RuntimeException : public Exception {
public:
    explicit RuntimeException(const string& msg) : Exception("Runtime exception: " + msg){};
};

class ConnectionException : public Exception {
public:
    explicit ConnectionException(const string& msg) : Exception(msg){};
};

class TransactionManagerException : public Exception {
public:
    explicit TransactionManagerException(const string& msg) : Exception(msg){};
};

} // namespace common
} // namespace kuzu
