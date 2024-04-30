#pragma once

#include "common/copier_config/csv_reader_config.h"
#include "common/copier_config/reader_config.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalCopyTo : public LogicalOperator {
public:
    LogicalCopyTo(std::string filePath, common::FileType fileType,
        std::vector<std::string> columnNames, std::vector<common::LogicalType> columnTypes,
        common::CSVOption copyToOption, std::shared_ptr<LogicalOperator> child,
        bool canParallel = true)
        : LogicalOperator{LogicalOperatorType::COPY_TO, std::move(child)},
          filePath{std::move(filePath)}, fileType{fileType}, columnNames{std::move(columnNames)},
          columnTypes{std::move(columnTypes)}, copyToOption{std::move(copyToOption)},
          canParallel{canParallel} {}

    f_group_pos_set getGroupsPosToFlatten();

    std::string getExpressionsForPrinting() const override { return std::string{}; }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getFilePath() const { return filePath; }
    common::FileType getFileType() const { return fileType; }
    std::vector<std::string> getColumnNames() const { return columnNames; }
    const std::vector<common::LogicalType>& getColumnTypesRef() const { return columnTypes; }
    const common::CSVOption* getCopyOption() const { return &copyToOption; }
    bool getCanParallel() const { return canParallel; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopyTo>(filePath, fileType, columnNames, columnTypes,
            copyToOption.copy(), children[0]->copy(), canParallel);
    }

private:
    std::string filePath;
    common::FileType fileType;
    std::vector<std::string> columnNames;
    std::vector<common::LogicalType> columnTypes;
    common::CSVOption copyToOption;
    bool canParallel;
};

} // namespace planner
} // namespace kuzu
