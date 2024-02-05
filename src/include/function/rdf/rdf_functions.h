#pragma once

#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct Type {
    static inline void operation(common::struct_entry_t& input, common::ku_string_t& result,
        common::ValueVector& inputValueVector, common::ValueVector& resultValueVector) {
        auto typeVector =
            common::StructVector::getFieldVector(&inputValueVector, 0 /* fieldIndex */);
        auto type = static_cast<common::LogicalTypeID>(typeVector->getValue<uint8_t>(input.pos));
        auto typeInStr = common::LogicalTypeUtils::toString(type);
        common::StringVector::addString(&resultValueVector, result, typeInStr);
    }
};

} // namespace function
} // namespace kuzu
