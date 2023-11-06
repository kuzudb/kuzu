#pragma once

#include "common/exception/internal.h"
#include "common/string_format.h"

namespace kuzu {
namespace common {
[[noreturn]] inline void kuAssertFailureInternal(
    const char* condition_name, const char* file, int linenr) {
    // LCOV_EXCL_START
    throw InternalException(stringFormat(
        "Assertion failed in file \"{}\" on line {}: {}", file, linenr, condition_name));
    // LCOV_EXCL_END
}

#ifdef KUZU_RUNTIME_CHECKS
#define KU_ASSERT(condition)                                                                       \
    if (!(condition)) {                                                                            \
        [[unlikely]] kuzu::common::kuAssertFailureInternal(#condition, __FILE__, __LINE__);        \
    }
#else
#define KU_ASSERT(condition)
#endif

} // namespace common
} // namespace kuzu
