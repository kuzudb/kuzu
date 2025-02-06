#pragma once

#include "common/copier_config/file_scan_info.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "storage/predicate/column_predicate.h"

namespace kuzu {
namespace common {
class FileSystem;
}

namespace function {

struct KUZU_API TableFuncBindData {
    binder::expression_vector columns;
    // the last {numWarningDataColumns} columns are for temporary internal use
    common::column_id_t numWarningDataColumns;
    common::cardinality_t cardinality;
    common::offset_t maxOffset;

    TableFuncBindData() : numWarningDataColumns{0}, cardinality{0}, maxOffset{0} {}
    explicit TableFuncBindData(common::offset_t maxOffset)
        : numWarningDataColumns{0}, cardinality{0}, maxOffset{maxOffset} {}
    explicit TableFuncBindData(binder::expression_vector columns)
        : columns{std::move(columns)}, numWarningDataColumns{0}, cardinality{0}, maxOffset{0} {}
    TableFuncBindData(binder::expression_vector columns, common::offset_t maxOffset)
        : columns{std::move(columns)}, numWarningDataColumns{0}, cardinality{0},
          maxOffset{maxOffset} {}
    TableFuncBindData(binder::expression_vector columns, common::column_id_t numWarningColumns,
        common::cardinality_t cardinality)
        : columns{std::move(columns)}, numWarningDataColumns{numWarningColumns},
          cardinality{cardinality}, maxOffset{0} {}
    TableFuncBindData(const TableFuncBindData& other)
        : columns{other.columns}, numWarningDataColumns(other.numWarningDataColumns),
          cardinality{other.cardinality}, maxOffset{other.maxOffset},
          columnSkips{other.columnSkips}, columnPredicates{copyVector(other.columnPredicates)} {}
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

private:
    std::vector<bool> columnSkips;
    std::vector<storage::ColumnPredicateSet> columnPredicates;
};

struct KUZU_API ScanBindData : TableFuncBindData {
    common::FileScanInfo fileScanInfo;
    main::ClientContext* context;

    ScanBindData(binder::expression_vector columns, common::FileScanInfo fileScanInfo,
        main::ClientContext* context)
        : TableFuncBindData{std::move(columns)}, fileScanInfo{std::move(fileScanInfo)},
          context{context} {}
    ScanBindData(binder::expression_vector columns, common::FileScanInfo fileScanInfo,
        main::ClientContext* context, common::column_id_t numWarningDataColumns,
        common::row_idx_t estCardinality)
        : TableFuncBindData{std::move(columns), numWarningDataColumns, estCardinality},
          fileScanInfo{std::move(fileScanInfo)}, context{context} {}
    ScanBindData(const ScanBindData& other)
        : TableFuncBindData{other}, fileScanInfo{other.fileScanInfo.copy()},
          context{other.context} {}

    bool getIgnoreErrorsOption() const override;

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ScanBindData>(*this);
    }
};

} // namespace function
} // namespace kuzu
