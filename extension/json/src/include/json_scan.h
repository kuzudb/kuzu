#pragma once

#include "function/table/bind_data.h"
#include "function/table_functions.h"

namespace kuzu {
namespace json_extension {

struct JsonScan {
    static constexpr const char* name = common::SCAN_JSON_FUNC_NAME;

    static std::unique_ptr<function::TableFunction> getFunction();
};

} // namespace json_extension
} // namespace kuzu
