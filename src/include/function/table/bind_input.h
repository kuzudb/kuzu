#pragma once

#include <vector>

#include "binder/expression/expression.h"
#include "common/case_insensitive_map.h"
#include "common/copier_config/reader_config.h"
#include "common/types/value/value.h"

namespace kuzu {
namespace binder {
class LiteralExpression;
class Binder;
} // namespace binder
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
    binder::expression_vector params;
    optional_params_t optionalParams;
    std::unique_ptr<ExtraTableFuncBindInput> extraInput = nullptr;
    binder::Binder* binder = nullptr;

    TableFuncBindInput() = default;

    void addLiteralParam(common::Value value);

    std::shared_ptr<binder::Expression> getParam(common::idx_t idx) const { return params[idx]; }
    common::Value getValue(common::idx_t idx) const;
    template<typename T>
    T getLiteralVal(common::idx_t idx) const;
};

struct ExtraScanTableFuncBindInput : ExtraTableFuncBindInput {
    common::ReaderConfig config;
    std::vector<std::string> expectedColumnNames;
    std::vector<common::LogicalType> expectedColumnTypes;
    function::TableFunction* tableFunction = nullptr;
};

} // namespace function
} // namespace kuzu
