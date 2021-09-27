#pragma once

#include <stdexcept>
#include <string>

using namespace std;

namespace graphflow {
namespace common {

class Exception : public exception {
public:
    explicit Exception(const string& msg) : exception(), exception_message_(msg){};

public:
    const char* what() const noexcept override { return exception_message_.c_str(); }

private:
    std::string exception_message_;
};

class ConversionException : public Exception {
public:
    explicit ConversionException(const string& msg) : Exception(msg){};
};

class LoaderException : public Exception {
public:
    explicit LoaderException(const string& msg) : Exception(msg){};
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
} // namespace common
} // namespace graphflow
