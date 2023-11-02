#pragma once

#include <exception>
#include <string>

namespace kuzu {
namespace common {

class Exception : public std::exception {
public:
    explicit Exception(std::string msg) : exception(), exception_message_(std::move(msg)){};

public:
    const char* what() const noexcept override { return exception_message_.c_str(); }

private:
    std::string exception_message_;
};

} // namespace common
} // namespace kuzu
