#pragma once

#include "common/types/value/value.h"

namespace kuzu {
namespace function {
struct TableFuncBindData {
    std::vector<common::LogicalType> returnTypes;
    std::vector<std::string> returnColumnNames;
    common::offset_t maxOffset;

    TableFuncBindData(std::vector<common::LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : returnTypes{std::move(returnTypes)},
          returnColumnNames{std::move(returnColumnNames)}, maxOffset{maxOffset} {}

    virtual ~TableFuncBindData() = default;

    virtual std::unique_ptr<TableFuncBindData> copy() {
        return std::make_unique<TableFuncBindData>(returnTypes, returnColumnNames, maxOffset);
    }
};
} // namespace function
} // namespace kuzu
