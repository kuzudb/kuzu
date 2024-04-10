#pragma once

#include "common/md5.h"
#include "common/vector/value_vector.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct MD5Operator {
    static void operation(ku_string_t& operand, ku_string_t& result, ValueVector& resultVector) {
        MD5 hasher;
        hasher.addToMD5(reinterpret_cast<const char*>(operand.getData()), operand.len);
        StringVector::addString(&resultVector, result, std::string(hasher.finishMD5()));
    }
};

} // namespace function
} // namespace kuzu
