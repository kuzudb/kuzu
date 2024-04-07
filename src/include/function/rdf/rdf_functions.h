#pragma once

#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "common/types/ku_string.h"
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

struct ValidatePredicate {
    static void operation(common::ku_string_t& input, common::ku_string_t& result,
        common::ValueVector& resultVector) {
        if (input.len >= 2 && input.prefix[0] == '_' && input.prefix[1] == ':') {
            throw common::RuntimeException(common::stringFormat(
                "{} is a blank node and cannot be a valid predicate.", input.getAsString()));
        }
        common::StringVector::addString(&resultVector, result, input);
    }
};

} // namespace function
} // namespace kuzu
