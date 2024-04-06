#pragma once

#include "common/copier_config/csv_reader_config.h"
#include "common/copier_config/reader_config.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalExportDatabase : public LogicalOperator {
public:
    explicit LogicalExportDatabase(common::ReaderConfig boundFileInfo,
        std::vector<std::shared_ptr<LogicalOperator>> plans)
        : LogicalOperator{LogicalOperatorType::EXPORT_DATABASE, std::move(plans)},
          boundFileInfo{std::move(boundFileInfo)} {}

    inline std::string getExpressionsForPrinting() const override { return std::string{}; }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getFilePath() const { return boundFileInfo.filePaths[0]; }
    inline common::FileType getFileType() const { return boundFileInfo.fileType; }
    inline common::CSVOption getCopyOption() const {
        auto csvConfig = common::CSVReaderConfig::construct(boundFileInfo.options);
        return csvConfig.option.copy();
    }
    inline const common::ReaderConfig* getBoundFileInfo() const { return &boundFileInfo; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExportDatabase>(std::move(boundFileInfo), std::move(children));
    }

private:
    common::ReaderConfig boundFileInfo;
};

} // namespace planner
} // namespace kuzu
