#pragma once

#include "binder/expression/expression.h"
#include "common/enums/table_type.h"
#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"

namespace kuzu {
namespace binder {

struct BoundFileScanInfo {
    function::TableFunction* copyFunc;
    std::unique_ptr<function::TableFuncBindData> copyFuncBindData;
    binder::expression_vector columns;
    std::shared_ptr<Expression> internalID;

    // TODO: remove the following field
    common::TableType tableType;

    BoundFileScanInfo(function::TableFunction* copyFunc,
        std::unique_ptr<function::TableFuncBindData> copyFuncBindData,
        binder::expression_vector columns, std::shared_ptr<Expression> internalID,
        common::TableType tableType)
        : copyFunc{copyFunc}, copyFuncBindData{std::move(copyFuncBindData)},
          columns{std::move(columns)}, internalID{std::move(internalID)}, tableType{tableType} {}
    BoundFileScanInfo(const BoundFileScanInfo& other)
        : copyFunc{other.copyFunc}, copyFuncBindData{other.copyFuncBindData->copy()},
          columns{other.columns}, internalID{other.internalID}, tableType{other.tableType} {}

    inline std::unique_ptr<BoundFileScanInfo> copy() const {
        return std::make_unique<BoundFileScanInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
