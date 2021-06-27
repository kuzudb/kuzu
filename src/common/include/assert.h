#pragma once

namespace graphflow {
namespace common {
void gfAssertInternal(bool condition, const char* condition_name, const char* file, int linenr);

#define GF_ASSERT(condition)                                                                       \
    graphflow::common::gfAssertInternal(bool(condition), #condition, __FILE__, __LINE__)
} // namespace common
} // namespace graphflow
