#pragma once

#include "common/types/types.h"
#include "storage/predicate/column_predicate.h"

namespace kuzu {
namespace common {
class FileSystem;
}

namespace function {

struct KUZU_API TableFuncBindData {
    binder::expression_vector columns;
    common::row_idx_t numRows;

    TableFuncBindData() : numRows{0} {}
    explicit TableFuncBindData(common::row_idx_t numRows) : numRows{numRows} {}
    explicit TableFuncBindData(binder::expression_vector columns)
        : columns{std::move(columns)}, numRows{0} {}
    TableFuncBindData(binder::expression_vector columns, common::row_idx_t numRows)
        : columns{std::move(columns)}, numRows{numRows} {}
    TableFuncBindData(const TableFuncBindData& other)
        : columns{other.columns}, numRows{other.numRows}, columnSkips{other.columnSkips},
          columnPredicates{copyVector(other.columnPredicates)} {}
    TableFuncBindData& operator=(const TableFuncBindData& other) = delete;
    virtual ~TableFuncBindData() = default;

    common::idx_t getNumColumns() const { return columns.size(); }
    void setColumnSkips(std::vector<bool> skips) { columnSkips = std::move(skips); }
    std::vector<bool> getColumnSkips() const;

    void setColumnPredicates(std::vector<storage::ColumnPredicateSet> predicates) {
        columnPredicates = std::move(predicates);
    }
    const std::vector<storage::ColumnPredicateSet>& getColumnPredicates() const {
        return columnPredicates;
    }

    virtual std::shared_ptr<binder::Expression> getNodeOutput() const { return nullptr; }

    virtual bool getIgnoreErrorsOption() const;

    virtual std::unique_ptr<TableFuncBindData> copy() const;

    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const TARGET*>(this);
    }

    template<class TARGET>
    TARGET& cast() {
        return *common::ku_dynamic_cast<TARGET*>(this);
    }

protected:
    std::vector<bool> columnSkips;
    std::vector<storage::ColumnPredicateSet> columnPredicates;
};

} // namespace function
} // namespace kuzu
