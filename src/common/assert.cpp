#include "common/assert.h"

#include "common/exception.h"
#include "common/string_utils.h"
#include "common/utils.h"

namespace kuzu {
namespace common {

void kuAssertInternal(bool condition, const char* condition_name, const char* file, int linenr) {
    if (condition) {
        return;
    }
    throw InternalException(StringUtils::string_format(
        "Assertion triggered in file \"{}\" on line {}: {}", file, linenr, condition_name));
}

} // namespace common
} // namespace kuzu
