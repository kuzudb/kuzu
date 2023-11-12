#pragma once

#include "common/copier_config/copier_config.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalCopyTo : public LogicalOperator {
public:
    LogicalCopyTo(std::string filePath, common::FileType fileType,
        std::vector<std::string> columnNames,
        std::vector<std::unique_ptr<common::LogicalType>> columnTypes,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::COPY_TO, std::move(child)}, filePath{std::move(
                                                                               filePath)},
          fileType{fileType}, columnNames{std::move(columnNames)}, columnTypes{
                                                                       std::move(columnTypes)} {}

    f_group_pos_set getGroupsPosToFlatten();

    inline std::string getExpressionsForPrinting() const override { return std::string{}; }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getFilePath() const { return filePath; }
    inline common::FileType getFileType() const { return fileType; }
    inline std::vector<std::string> getColumnNames() const { return columnNames; }
    inline const std::vector<std::unique_ptr<common::LogicalType>>& getColumnTypesRef() const {
        return columnTypes;
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopyTo>(filePath, fileType, columnNames,
            common::LogicalType::copy(columnTypes), children[0]->copy());
    }

private:
    std::string filePath;
    common::FileType fileType;
    std::vector<std::string> columnNames;
    std::vector<std::unique_ptr<common::LogicalType>> columnTypes;
};

} // namespace planner
} // namespace kuzu
