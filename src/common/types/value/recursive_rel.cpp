#include "common/types/value/recursive_rel.h"

#include "common/exception/exception.h"
#include "common/types/types.h"
#include "common/types/value/value.h"
#include "spdlog/fmt/fmt.h"

namespace kuzu {
namespace common {

Value* RecursiveRelVal::getNodes(const Value* val) {
    throwIfNotRecursiveRel(val);
    return val->children[0].get();
}

Value* RecursiveRelVal::getRels(const Value* val) {
    throwIfNotRecursiveRel(val);
    return val->children[1].get();
}

void RecursiveRelVal::throwIfNotRecursiveRel(const Value* val) {
    if (val->dataType->getLogicalTypeID() != LogicalTypeID::RECURSIVE_REL) {
        auto actualType = LogicalTypeUtils::dataTypeToString(val->dataType->getLogicalTypeID());
        throw Exception(fmt::format("Expected RECURSIVE_REL type, but got {} type", actualType));
    }
}

} // namespace common
} // namespace kuzu
