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

// Roughly copy the definition in <assert.h>. Specifically, we make `assert` an expression that
// evaluates to void(0).
#if defined(KUZU_RUNTIME_CHECKS) || !defined(NDEBUG)
#define KU_ASSERT(condition)                                                                       \
    static_cast<bool>(condition) ?                                                                 \
        void(0) :                                                                                  \
        kuzu::common::kuAssertFailureInternal(#condition, __FILE__, __LINE__)
#else
#define KU_ASSERT(condition) void(0)
#endif

} // namespace common
} // namespace kuzu
