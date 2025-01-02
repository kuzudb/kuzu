#pragma once

#include "common/copier_config/csv_reader_config.h"
#include "common/copier_config/file_scan_info.h"
#include "logical_simple.h"

namespace kuzu {
namespace planner {

class LogicalExportDatabase final : public LogicalSimple {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::EXPORT_DATABASE;

public:
    LogicalExportDatabase(common::FileScanInfo boundFileInfo,
        std::shared_ptr<binder::Expression> outputExpression,
        const std::vector<std::shared_ptr<LogicalOperator>>& plans)
        : LogicalSimple{type_, plans, std::move(outputExpression)},
          boundFileInfo{std::move(boundFileInfo)} {}

    std::string getFilePath() const { return boundFileInfo.filePaths[0]; }
    common::FileType getFileType() const { return boundFileInfo.fileTypeInfo.fileType; }
    common::CSVOption getCopyOption() const {
        auto csvConfig = common::CSVReaderConfig::construct(boundFileInfo.options);
        return csvConfig.option.copy();
    }
    const common::FileScanInfo* getBoundFileInfo() const { return &boundFileInfo; }
    std::string getExpressionsForPrinting() const override { return std::string{}; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExportDatabase>(std::move(boundFileInfo),
            std::move(outputExpression), std::move(children));
    }

private:
    common::FileScanInfo boundFileInfo;
};

} // namespace planner
} // namespace kuzu
