#pragma once

#include <stdexcept>
#include <string>

using namespace std;

namespace graphflow {
namespace common {

class Exception : public exception {
public:
    explicit Exception(string msg) : exception(), exception_message_(move(msg)){};

public:
    const char* what() const noexcept override { return exception_message_.c_str(); }

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

class LoaderException : public Exception {
public:
    explicit LoaderException(const string& msg) : Exception("Loader exception: " + msg){};
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
} // namespace graphflow
