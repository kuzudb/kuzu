#pragma once

#include "common/copier_config/copier_config.h"
#include "common/types/types.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace function {

struct TableFuncBindData {
    std::vector<std::unique_ptr<common::LogicalType>> returnTypes;
    std::vector<std::string> returnColumnNames;

    TableFuncBindData(std::vector<std::unique_ptr<common::LogicalType>> returnTypes,
        std::vector<std::string> returnColumnNames)
        : returnTypes{std::move(returnTypes)}, returnColumnNames{std::move(returnColumnNames)} {}

    virtual ~TableFuncBindData() = default;

    virtual std::unique_ptr<TableFuncBindData> copy() = 0;
};

struct ScanBindData : public TableFuncBindData {
    common::ReaderConfig config;
    storage::MemoryManager* mm;

    ScanBindData(std::vector<std::unique_ptr<common::LogicalType>> returnTypes,
        std::vector<std::string> returnColumnNames, const common::ReaderConfig config,
        storage::MemoryManager* mm)
        : TableFuncBindData{std::move(returnTypes), std::move(returnColumnNames)}, config{config},
          mm{mm} {}

    std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<ScanBindData>(
            common::LogicalType::copy(returnTypes), returnColumnNames, config, mm);
    }
};

} // namespace function
} // namespace kuzu
