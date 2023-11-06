#include "common/assert.h"

#include "common/exception/internal.h"
#include "common/string_format.h"

namespace kuzu {
namespace common {

[[noreturn]] void kuAssertFailureInternal(
    const char* condition_name, const char* file, int linenr) {
    // LCOV_EXCL_START
    throw InternalException(stringFormat(
        "Assertion failed in file \"{}\" on line {}: {}", file, linenr, condition_name));
    // LCOV_EXCL_END
}

} // namespace common
} // namespace kuzu
