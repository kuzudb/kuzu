#include "src/common/include/assert.h"

#include "src/common/include/exception.h"
#include "src/common/include/utils.h"

namespace graphflow {
namespace common {

void gfAssertInternal(bool condition, const char* condition_name, const char* file, int linenr) {
    if (condition) {
        return;
    }
    throw InternalException(StringUtils::string_format(
        "Assertion triggered in file \"%s\" on line %d: %s", file, linenr, condition_name));
}

} // namespace common
} // namespace graphflow
