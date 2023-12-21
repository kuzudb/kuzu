#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace processor {

struct RdfUtils {
    static void addRdfLiteral(
        common::ValueVector* vector, uint32_t pos, const char* buf, uint32_t length);
    static void addRdfLiteral(common::ValueVector* vector, uint32_t pos, const char* buf,
        uint32_t length, const char* typeBuf, uint32_t typeLength);
};

} // namespace processor
} // namespace kuzu
