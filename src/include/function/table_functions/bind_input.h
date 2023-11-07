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
    explicit TableFuncBindInput(std::vector<std::unique_ptr<common::Value>> inputs)
        : inputs{std::move(inputs)} {}
    virtual ~TableFuncBindInput() = default;
};

struct ScanTableFuncBindInput final : public TableFuncBindInput {
    const common::ReaderConfig config;
    storage::MemoryManager* mm;

    ScanTableFuncBindInput(const common::ReaderConfig config, storage::MemoryManager* mm)
        : TableFuncBindInput{}, config{config}, mm{mm} {
        inputs.push_back(
            std::make_unique<common::Value>(common::Value::createValue(this->config.filePaths[0])));
    }
};

} // namespace function
} // namespace kuzu
