#pragma once

#include "common/copier_config/reader_config.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "storage/predicate/column_predicate.h"

namespace kuzu {
namespace common {
class FileSystem;
}

namespace function {

struct TableFuncBindData {
    binder::expression_vector columns;
    // the last {numWarningDataColumns} columns are for temporary internal use
    common::column_id_t numWarningDataColumns;
    common::cardinality_t cardinality;

    TableFuncBindData() : numWarningDataColumns{0}, cardinality{0} {}
    explicit TableFuncBindData(binder::expression_vector columns)
        : columns{std::move(columns)}, numWarningDataColumns{0}, cardinality{0} {}
    TableFuncBindData(binder::expression_vector columns, common::column_id_t numWarningColumns,
        common::cardinality_t cardinality)
        : columns{std::move(columns)}, numWarningDataColumns{numWarningColumns},
          cardinality{cardinality} {}
    TableFuncBindData(const TableFuncBindData& other)
        : columns{other.columns}, numWarningDataColumns(other.numWarningDataColumns),
          cardinality{other.cardinality}, columnSkips{other.columnSkips},
          columnPredicates{copyVector(other.columnPredicates)} {}
    virtual ~TableFuncBindData() = default;

    common::idx_t getNumColumns() const { return columns.size(); }
    void setColumnSkips(std::vector<bool> skips) { columnSkips = std::move(skips); }
    KUZU_API std::vector<bool> getColumnSkips() const;

    void setColumnPredicates(std::vector<storage::ColumnPredicateSet> predicates) {
        columnPredicates = std::move(predicates);
    }
    const std::vector<storage::ColumnPredicateSet>& getColumnPredicates() const {
        return columnPredicates;
    }

    virtual std::unique_ptr<TableFuncBindData> copy() const = 0;

    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const TARGET*>(this);
    }

    template<class TARGET>
    TARGET& cast() {
        return *common::ku_dynamic_cast<TARGET*>(this);
    }

private:
    std::vector<bool> columnSkips;
    std::vector<storage::ColumnPredicateSet> columnPredicates;
};

struct ScanBindData : public TableFuncBindData {
    common::ReaderConfig config;
    main::ClientContext* context;

    ScanBindData(binder::expression_vector columns, common::ReaderConfig config,
        main::ClientContext* context)
        : TableFuncBindData{std::move(columns)}, config{std::move(config)}, context{context} {}
    ScanBindData(binder::expression_vector columns, common::ReaderConfig config,
        main::ClientContext* context, common::column_id_t numWarningDataColumns,
        common::row_idx_t estCardinality)
        : TableFuncBindData{std::move(columns), numWarningDataColumns, estCardinality},
          config{std::move(config)}, context{context} {}
    ScanBindData(const ScanBindData& other)
        : TableFuncBindData{other}, config{other.config.copy()}, context{other.context} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ScanBindData>(*this);
    }
};

} // namespace function
} // namespace kuzu
