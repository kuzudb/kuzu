#pragma once

namespace kuzu {
namespace common {
[[noreturn]] void kuAssertFailureInternal(const char* condition_name, const char* file, int linenr);

#define KU_ASSERT(condition)                                                                       \
    if (!(condition)) {                                                                            \
        [[unlikely]] kuzu::common::kuAssertFailureInternal(#condition, __FILE__, __LINE__);        \
    }

} // namespace common
} // namespace kuzu
