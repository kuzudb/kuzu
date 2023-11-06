#pragma once

#include "common/types/types.h"

namespace kuzu {
namespace function {

struct TableFuncBindData {
    std::vector<common::LogicalType> returnTypes;
    std::vector<std::string> returnColumnNames;

    TableFuncBindData(
        std::vector<common::LogicalType> returnTypes, std::vector<std::string> returnColumnNames)
        : returnTypes{std::move(returnTypes)}, returnColumnNames{std::move(returnColumnNames)} {}

    virtual ~TableFuncBindData() = default;

    virtual std::unique_ptr<TableFuncBindData> copy() = 0;
};

} // namespace function
} // namespace kuzu
