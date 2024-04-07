#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace processor {

struct RdfUtils {
    static common::LogicalTypeID getLogicalTypeID(const std::string& type);
    static void addRdfLiteral(common::ValueVector* vector, uint32_t pos, const std::string& str,
        common::LogicalTypeID targetTypeID);
};

} // namespace processor
} // namespace kuzu
