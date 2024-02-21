#include "common/enums/rel_multiplicity.h"

#include "common/exception/binder.h"
#include "common/string_format.h"

namespace kuzu {
namespace common {

RelMultiplicity RelMultiplicityUtils::getFwd(const std::string& multiplicityStr) {
    if ("ONE_ONE" == multiplicityStr || "ONE_MANY" == multiplicityStr) {
        return RelMultiplicity::ONE;
    } else if ("MANY_ONE" == multiplicityStr || "MANY_MANY" == multiplicityStr) {
        return RelMultiplicity::MANY;
    }
    throw BinderException(
        stringFormat("Cannot bind {} as relationship multiplicity.", multiplicityStr));
}

RelMultiplicity RelMultiplicityUtils::getBwd(const std::string& multiplicityStr) {
    if ("ONE_ONE" == multiplicityStr || "MANY_ONE" == multiplicityStr) {
        return RelMultiplicity::ONE;
    } else if ("ONE_MANY" == multiplicityStr || "MANY_MANY" == multiplicityStr) {
        return RelMultiplicity::MANY;
    }
    throw BinderException(
        stringFormat("Cannot bind {} as relationship multiplicity.", multiplicityStr));
}

} // namespace common
} // namespace kuzu
