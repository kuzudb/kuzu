#pragma once

#include "function/function.h"

namespace kuzu {
namespace llm_extension {

struct CreateEmbedding {
    static constexpr const char* name = "CREATE_EMBEDDING";

    static function::function_set getFunctionSet();
};

} // namespace llm_extension
} // namespace kuzu
