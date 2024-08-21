#pragma once

#include <vector>

#include "common/copier_config/reader_config.h"
#include "common/types/value/value.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace function {

struct ScanTableFuncBindInput {
    std::vector<common::Value> inputs;
    common::ReaderConfig config;
    std::vector<std::string> expectedColumnNames;
    std::vector<common::LogicalType> expectedColumnTypes;
    main::ClientContext* context;

    ScanTableFuncBindInput() {}
    explicit ScanTableFuncBindInput(common::ReaderConfig config) : config{std::move(config)} {};
    ScanTableFuncBindInput(common::ReaderConfig config,
        std::vector<std::string> expectedColumnNames,
        std::vector<common::LogicalType> expectedColumnTypes, main::ClientContext* context)
        : config{std::move(config)}, expectedColumnNames{std::move(expectedColumnNames)},
          expectedColumnTypes{std::move(expectedColumnTypes)}, context{context} {
        inputs.push_back(common::Value::createValue(this->config.filePaths[0]));
    }
    EXPLICIT_COPY_DEFAULT_MOVE(ScanTableFuncBindInput);

private:
    ScanTableFuncBindInput(const ScanTableFuncBindInput& other)
        : inputs{other.inputs}, config{other.config.copy()},
          expectedColumnNames{other.expectedColumnNames},
          expectedColumnTypes{common::LogicalType::copy(other.expectedColumnTypes)},
          context{other.context} {}
};

} // namespace function
} // namespace kuzu
