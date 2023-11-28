#pragma once

#include "common/copier_config/reader_config.h"
#include "common/types/types.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace function {

struct TableFuncBindData {
    common::logical_types_t columnTypes;
    std::vector<std::string> columnNames;

    TableFuncBindData(common::logical_types_t columnTypes, std::vector<std::string> columnNames)
        : columnTypes{std::move(columnTypes)}, columnNames{std::move(columnNames)} {}
    TableFuncBindData(const TableFuncBindData& other)
        : columnTypes{common::LogicalType::copy(other.columnTypes)}, columnNames{
                                                                         other.columnNames} {}

    virtual ~TableFuncBindData() = default;

    virtual std::unique_ptr<TableFuncBindData> copy() = 0;
};

struct ScanBindData : public TableFuncBindData {
    storage::MemoryManager* mm;
    common::ReaderConfig config;

    ScanBindData(common::logical_types_t columnTypes, std::vector<std::string> columnNames,
        storage::MemoryManager* mm, const common::ReaderConfig& config)
        : TableFuncBindData{std::move(columnTypes), std::move(columnNames)}, mm{mm}, config{
                                                                                         config} {}
    ScanBindData(const ScanBindData& other)
        : TableFuncBindData{other}, mm{other.mm}, config{other.config} {}

    inline std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<ScanBindData>(*this);
    }
};

} // namespace function
} // namespace kuzu
