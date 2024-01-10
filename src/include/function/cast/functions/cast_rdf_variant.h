#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct CastFromRdfVariant {
    template<typename INPUT_TYPE, typename RESULT_TYPE>
    static void operation(INPUT_TYPE& src, common::ValueVector& inputVector, uint64_t inputPos,
        RESULT_TYPE& result, common::ValueVector& resultVector, uint64_t resultPos);
};

struct CastToRdfVariant {
    template<typename INPUT_TYPE, typename RESULT_TYPE>
    static void operation(INPUT_TYPE& src, common::ValueVector&, uint64_t, RESULT_TYPE&,
        common::ValueVector& resultVector, uint64_t resultPos) {
        common::RdfVariantVector::add(&resultVector, resultPos, src);
    }
};

template<>
inline void CastToRdfVariant::operation(common::ku_string_t& src, common::ValueVector&, uint64_t,
    common::struct_entry_t&, common::ValueVector& resultVector, uint64_t resultPos) {
    common::RdfVariantVector::addString(&resultVector, resultPos, src);
}

} // namespace function
} // namespace kuzu
