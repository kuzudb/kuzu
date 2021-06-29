#include "src/common/include/exception.h"

namespace graphflow {
namespace common {

Exception::Exception(const std::string& msg) : std::exception() {
    exception_message_ = msg;
}

const char* Exception::what() const noexcept {
    return exception_message_.c_str();
}

ConversionException::ConversionException(const string& msg) : Exception(msg) {}

InternalException::InternalException(const string& msg) : Exception(msg) {}

} // namespace common
} // namespace graphflow
