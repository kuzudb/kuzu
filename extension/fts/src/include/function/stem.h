#pragma once

#include "function/function.h"

namespace kuzu {
namespace fts_extension {

struct StemFunction {
    static constexpr const char* name = "STEM";

    static function::function_set getFunctionSet();

    static void validateStemmer(const std::string& stemmer);
};

} // namespace fts_extension
} // namespace kuzu
