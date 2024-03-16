#pragma once

#include <vector>

#include "common/copier_config/reader_config.h"
#include "common/types/value/value.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace function {

struct TableFuncBindInput {
    std::vector<common::Value> inputs;

    TableFuncBindInput() = default;
    EXPLICIT_COPY_DEFAULT_MOVE(TableFuncBindInput);
    virtual ~TableFuncBindInput() = default;

private:
    TableFuncBindInput(const TableFuncBindInput& other) : inputs{other.inputs} {}
};

struct ScanTableFuncBindInput final : public TableFuncBindInput {
    common::ReaderConfig config;
    std::vector<std::string> expectedColumnNames;
    std::vector<common::LogicalType> expectedColumnTypes;
    main::ClientContext* context;

    explicit ScanTableFuncBindInput(common::ReaderConfig config) : config{std::move(config)} {};
    EXPLICIT_COPY_DEFAULT_MOVE(ScanTableFuncBindInput);
    ScanTableFuncBindInput(common::ReaderConfig config,
        std::vector<std::string> expectedColumnNames,
        std::vector<common::LogicalType> expectedColumnTypes, main::ClientContext* context)
        : TableFuncBindInput{}, config{std::move(config)}, expectedColumnNames{std::move(
                                                               expectedColumnNames)},
          expectedColumnTypes{std::move(expectedColumnTypes)}, context{context} {
        inputs.push_back(common::Value::createValue(this->config.filePaths[0]));
    }

private:
    ScanTableFuncBindInput(const ScanTableFuncBindInput& other) : {}
};

} // namespace function
} // namespace kuzu
