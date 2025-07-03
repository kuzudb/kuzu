#pragma once

#include "function/function.h"

namespace kuzu {
namespace embedding_extension {

struct CreateEmbedding {
    static constexpr const char* name = "CREATE_EMBEDDING";

    static function::function_set getFunctionSet();
};

} // namespace embedding_extension
} // namespace kuzu
