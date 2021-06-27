#pragma once

#include <stdexcept>
#include <string>

using namespace std;
namespace graphflow {
namespace common {

class Exception : public exception {
public:
    explicit Exception(const string& msg);

public:
    const char* what() const noexcept override;

private:
    std::string exception_message_;
};

class ConversionException : public Exception {
public:
    explicit ConversionException(const string& msg);
};

class InternalException : public Exception {
public:
    explicit InternalException(const string& msg);
};
} // namespace common
} // namespace graphflow
