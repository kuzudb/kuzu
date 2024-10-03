#include "common/enums/extend_direction.h"

#include "common/exception/runtime.h"
#include "common/string_utils.h"

namespace kuzu {
namespace common {

ExtendDirection ExtendDirectionUtil::fromString(const std::string& str) {
    auto normalizedString = StringUtils::getUpper(str);
    if (normalizedString == "FWD") {
        return ExtendDirection::FWD;
    } else if (normalizedString == "BWD") {
        return ExtendDirection::BWD;
    } else if (normalizedString == "BOTH") {
        return ExtendDirection::BOTH;
    } else {
        throw RuntimeException(stringFormat("Cannot parse {} as ExtendDirection.", str));
    }
}

} // namespace common
} // namespace kuzu
