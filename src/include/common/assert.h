#pragma once

namespace kuzu {
namespace common {
void kuAssertInternal(bool condition, const char* condition_name, const char* file, int linenr);

#define KU_ASSERT(condition)                                                                       \
    kuzu::common::kuAssertInternal(bool(condition), #condition, __FILE__, __LINE__)
} // namespace common
} // namespace kuzu
