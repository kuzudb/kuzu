#pragma once

#include "common/sha256.h"
#include "common/vector/value_vector.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct SHA256Operator {
    static void operation(ku_string_t& operand, ku_string_t& result, ValueVector& resultVector) {
        StringVector::reserveString(&resultVector, result, SHA256::SHA256_HASH_LENGTH_TEXT);
        SHA256 hasher;
        hasher.addString(operand.getAsString());
        hasher.finishSHA256(reinterpret_cast<char*>(result.getDataUnsafe()));
    }
};

} // namespace function
} // namespace kuzu
