#pragma once

#include "binder/expression/node_expression.h"
#include "function/table/bind_data.h"
#include "function/table/simple_table_function.h"
#include "function/table/table_function.h"

namespace kuzu {
namespace llm_extension {

struct CreateEmbedding final {
    static constexpr const char* name = "CREATE_EMBEDDING";

    static function::function_set getFunctionSet();
};

} // namespace llm_extension
} // namespace kuzu
