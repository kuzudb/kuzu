#pragma once

#include <cerrno>
#include <string>
#include <system_error>

namespace kuzu {
namespace common {
inline std::string systemErrMessage(int code) {
    // System errors are unexpected. For anything expected, we should catch it explicitly and
    // provide a better error message to the user.
    // LCOV_EXCL_START
    return std::system_category().message(code);
    // LCOV_EXCL_END
}
inline std::string posixErrMessage() {
    // LCOV_EXCL_START
    return systemErrMessage(errno);
    // LCOV_EXCL_END
}
} // namespace common
} // namespace kuzu
