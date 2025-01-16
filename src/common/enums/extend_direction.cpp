#include "common/enums/extend_direction.h"

#include "common/exception/runtime.h"
#include "common/string_utils.h"

namespace kuzu {
namespace common {

ExtendDirection ExtendDirectionUtil::getDefaultExtendDirection() {
#if defined(KUZU_DEFAULT_FWD_REL_STORAGE) && (KUZU_DEFAULT_FWD_REL_STORAGE == 1)
    static constexpr ExtendDirection defaultExtendDirection = ExtendDirection::FWD;
#else
    static constexpr ExtendDirection defaultExtendDirection = ExtendDirection::BOTH;
#endif
    return defaultExtendDirection;
}

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

std::string ExtendDirectionUtil::toString(ExtendDirection direction) {
    switch (direction) {
    case ExtendDirection::FWD:
        return "fwd";
    case ExtendDirection::BWD:
        return "bwd";
    case ExtendDirection::BOTH:
        return "both";
    default:
        KU_UNREACHABLE;
    }
}

} // namespace common
} // namespace kuzu
