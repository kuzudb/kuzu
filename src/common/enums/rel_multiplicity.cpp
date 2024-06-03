#include "common/enums/rel_multiplicity.h"

#include "common/exception/binder.h"
#include "common/string_format.h"
#include "common/string_utils.h"

namespace kuzu {
namespace common {

RelMultiplicity RelMultiplicityUtils::getFwd(const std::string& multiplicityStr) {
    auto normalizedMultiStr = common::StringUtils::getUpper(multiplicityStr);
    if ("ONE_ONE" == normalizedMultiStr || "ONE_MANY" == normalizedMultiStr) {
        return RelMultiplicity::ONE;
    } else if ("MANY_ONE" == normalizedMultiStr || "MANY_MANY" == normalizedMultiStr) {
        return RelMultiplicity::MANY;
    }
    throw BinderException(
        stringFormat("Cannot bind {} as relationship multiplicity.", multiplicityStr));
}

RelMultiplicity RelMultiplicityUtils::getBwd(const std::string& multiplicityStr) {
    auto normalizedMultiStr = common::StringUtils::getUpper(multiplicityStr);
    if ("ONE_ONE" == normalizedMultiStr || "MANY_ONE" == normalizedMultiStr) {
        return RelMultiplicity::ONE;
    } else if ("ONE_MANY" == normalizedMultiStr || "MANY_MANY" == normalizedMultiStr) {
        return RelMultiplicity::MANY;
    }
    throw BinderException(
        stringFormat("Cannot bind {} as relationship multiplicity.", multiplicityStr));
}

} // namespace common
} // namespace kuzu
