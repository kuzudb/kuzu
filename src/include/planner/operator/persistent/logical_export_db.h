#pragma once

#include "common/copier_config/csv_reader_config.h"
#include "common/copier_config/reader_config.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalExportDatabase : public LogicalOperator {
public:
    explicit LogicalExportDatabase(std::string filePath, common::FileType fileType,
        common::CSVOption copyToOption, std::vector<std::shared_ptr<LogicalOperator>> plans)
        : LogicalOperator{LogicalOperatorType::EXPORT_DATABASE, std::move(plans)},
          filePath{std::move(filePath)}, fileType(fileType), copyToOption{std::move(copyToOption)} {
    }

    inline std::string getExpressionsForPrinting() const override { return std::string{}; }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getFilePath() const { return filePath; }
    inline common::FileType getFileType() const { return fileType; }
    inline const common::CSVOption* getCopyOption() const { return &copyToOption; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExportDatabase>(
            filePath, fileType, std::move(copyToOption), std::move(children));
    }

private:
    std::string filePath;
    common::FileType fileType;
    common::CSVOption copyToOption;
};

} // namespace planner
} // namespace kuzu
