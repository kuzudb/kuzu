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
    std::vector<common::LogicalType> columnTypes;
    std::vector<std::string> columnNames;

    // the last {numWarningDataColumns} columns are for temporary internal use
    common::column_id_t numWarningDataColumns;

    TableFuncBindData() : numWarningDataColumns{common::INVALID_COLUMN_ID} {}
    TableFuncBindData(std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> columnNames, common::column_id_t numWarningDataColumns = 0)
        : columnTypes{std::move(columnTypes)}, columnNames{std::move(columnNames)},
          numWarningDataColumns(numWarningDataColumns) {}
    TableFuncBindData(const TableFuncBindData& other)
        : columnTypes{common::LogicalType::copy(other.columnTypes)}, columnNames{other.columnNames},
          numWarningDataColumns(other.numWarningDataColumns), columnSkips{other.columnSkips},
          columnPredicates{copyVector(other.columnPredicates)} {}
    virtual ~TableFuncBindData() = default;

    common::idx_t getNumColumns() const { return columnTypes.size(); }
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

private:
    std::vector<bool> columnSkips;
    std::vector<storage::ColumnPredicateSet> columnPredicates;
};

struct ScanBindData : public TableFuncBindData {
    common::ReaderConfig config;
    main::ClientContext* context;

    ScanBindData(std::vector<common::LogicalType> columnTypes, std::vector<std::string> columnNames,
        common::ReaderConfig config, main::ClientContext* context,
        common::column_id_t numWarningDataColumns = 0)
        : TableFuncBindData{std::move(columnTypes), std::move(columnNames), numWarningDataColumns},
          config{std::move(config)}, context{context} {}
    ScanBindData(const ScanBindData& other)
        : TableFuncBindData{other}, config{other.config.copy()}, context{other.context} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ScanBindData>(*this);
    }
};

} // namespace function
} // namespace kuzu
