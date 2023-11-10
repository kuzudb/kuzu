#pragma once

#include <vector>

#include "common/copier_config/copier_config.h"
#include "common/types/value/value.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace function {

struct TableFuncBindInput {
    std::vector<std::unique_ptr<common::Value>> inputs;

    TableFuncBindInput() = default;
    virtual ~TableFuncBindInput() = default;
};

struct ScanTableFuncBindInput final : public TableFuncBindInput {
    storage::MemoryManager* mm;
    common::ReaderConfig config;
    std::vector<std::string> expectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> expectedColumnTypes;

    ScanTableFuncBindInput(storage::MemoryManager* mm, const common::ReaderConfig& config,
        std::vector<std::string> expectedColumnNames,
        std::vector<std::unique_ptr<common::LogicalType>> expectedColumnTypes)
        : TableFuncBindInput{}, mm{mm}, config{config},
          expectedColumnNames{std::move(expectedColumnNames)}, expectedColumnTypes{
                                                                   std::move(expectedColumnTypes)} {
        inputs.push_back(common::Value::createValue(config.filePaths[0]).copy());
    }
};

} // namespace function
} // namespace kuzu
