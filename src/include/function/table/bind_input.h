#pragma once

#include <vector>

#include "common/case_insensitive_map.h"
#include "common/copier_config/reader_config.h"
#include "common/types/value/value.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace common {
class Value;
}

namespace function {

using optional_params_t = common::case_insensitive_map_t<common::Value>;

struct TableFunction;

struct ExtraTableFuncBindInput {
    virtual ~ExtraTableFuncBindInput() = default;
};

struct TableFuncBindInput {
    std::vector<common::Value> params;
    optional_params_t optionalParams;
    std::unique_ptr<ExtraTableFuncBindInput> extraInput = nullptr;

    TableFuncBindInput() = default;

    void addParam(common::Value value) { params.push_back(std::move(value)); }
    const common::Value& getParam(common::idx_t idx) const { return params[idx]; }
};

struct ExtraScanTableFuncBindInput : ExtraTableFuncBindInput {
    common::ReaderConfig config;
    std::vector<std::string> expectedColumnNames;
    std::vector<common::LogicalType> expectedColumnTypes;
    function::TableFunction* tableFunction = nullptr;
};

} // namespace function
} // namespace kuzu
