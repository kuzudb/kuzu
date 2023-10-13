#include "common/assert.h"

#include "common/exception/internal.h"
#include "common/string_format.h"
#include "common/utils.h"

namespace kuzu {
namespace common {

void kuAssertInternal(bool condition, const char* condition_name, const char* file, int linenr) {
    if (!condition) {
        // LCOV_EXCL_START
        throw InternalException(stringFormat(
            "Assertion triggered in file \"{}\" on line {}: {}", file, linenr, condition_name));
        // LCOV_EXCL_END
    }
}

} // namespace common
} // namespace kuzu
