#include "common/types/value/rdf_variant.h"

using namespace kuzu::common;

namespace kuzu {
namespace common {

LogicalTypeID RdfVariant::getLogicalTypeID(const Value* rdfVariant) {
    auto typeVal = NestedVal::getChildVal(rdfVariant, 0);
    return static_cast<LogicalTypeID>(typeVal->getValue<uint8_t>());
}
} // namespace common
} // namespace kuzu
