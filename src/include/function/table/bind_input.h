#pragma once

#include <vector>

#include "common/copier_config/reader_config.h"
#include "common/types/value/value.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace function {

struct TableFunction;
struct ScanTableFuncBindInput {
    std::vector<common::Value> inputs;
    common::ReaderConfig config;
    std::vector<std::string> expectedColumnNames;
    std::vector<common::LogicalType> expectedColumnTypes;
    main::ClientContext* context;
    function::TableFunction* tableFunction;

    ScanTableFuncBindInput() : context(nullptr), tableFunction(nullptr) {}
    explicit ScanTableFuncBindInput(common::ReaderConfig config)
        : config{std::move(config)}, context(nullptr), tableFunction(nullptr){};
    ScanTableFuncBindInput(common::ReaderConfig config,
        std::vector<std::string> expectedColumnNames,
        std::vector<common::LogicalType> expectedColumnTypes, main::ClientContext* context,
        function::TableFunction* tableFunction)
        : config{std::move(config)}, expectedColumnNames{std::move(expectedColumnNames)},
          expectedColumnTypes{std::move(expectedColumnTypes)}, context{context},
          tableFunction{tableFunction} {
        inputs.push_back(common::Value::createValue(this->config.filePaths[0]));
    }
    EXPLICIT_COPY_DEFAULT_MOVE(ScanTableFuncBindInput);

private:
    ScanTableFuncBindInput(const ScanTableFuncBindInput& other)
        : inputs{other.inputs}, config{other.config.copy()},
          expectedColumnNames{other.expectedColumnNames},
          expectedColumnTypes{common::LogicalType::copy(other.expectedColumnTypes)},
          context{other.context}, tableFunction{other.tableFunction} {}
};

} // namespace function
} // namespace kuzu
