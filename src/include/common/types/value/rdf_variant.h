#pragma once

#include "common/api.h"
#include "common/types/blob.h"
#include "common/types/value/nested.h"
#include "common/types/value/value.h"

namespace kuzu {
namespace common {
class Value;
class RdfVariant {
public:
    /**
     * @brief Get the logical type id of the rdf variant.
     * @param rdfVariant the rdf variant.
     * @return the logical type id.
     */
    KUZU_API static LogicalTypeID getLogicalTypeID(const Value* rdfVariant);

    /**
     * @brief Get the value of the rdf variant.
     * @tparam T the type of the value.
     * @param rdfVariant the rdf variant.
     * @return the value.
     */
    template<typename T>
    T static getValue(const Value* rdfVariant) {
        auto blobData = NestedVal::getChildVal(rdfVariant, 1)->strVal.data();
        return Blob::getValue<T>(blobData);
    }
};

/**
 * @brief Specialization for string.
 * @param rdfVariant the rdf variant.
 * @return the string value.
 */
template<>
KUZU_API inline std::string RdfVariant::getValue<std::string>(const Value* rdfVariant) {
    return NestedVal::getChildVal(rdfVariant, 1)->strVal;
}
} // namespace common
} // namespace kuzu
