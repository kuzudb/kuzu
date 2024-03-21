#pragma once

#include "function/function.h"
#include "function/table/bind_data.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

struct FTableScanBindData : public function::TableFuncBindData {
    std::shared_ptr<FactorizedTable> table;
    std::vector<ft_col_idx_t> columnIndices;
    uint64_t morselSize;

    FTableScanBindData(std::shared_ptr<FactorizedTable> table,
        std::vector<ft_col_idx_t> columnIndices, uint64_t morselSize)
        : table{std::move(table)}, columnIndices{std::move(columnIndices)}, morselSize{morselSize} {
    }
    FTableScanBindData(const FTableScanBindData& other)
        : function::TableFuncBindData{other}, table{other.table},
          columnIndices{other.columnIndices}, morselSize{other.morselSize} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<FTableScanBindData>(*this);
    }
};

struct FTableScan {
    static function::function_set getFunctionSet();
};

} // namespace processor
} // namespace kuzu
