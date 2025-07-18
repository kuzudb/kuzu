#pragma once

#include "common/copier_config/file_scan_info.h"
#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

struct ExportDBPrintInfo final : OPPrintInfo {
    std::string filePath;
    common::case_insensitive_map_t<common::Value> options;

    ExportDBPrintInfo(std::string filePath, common::case_insensitive_map_t<common::Value> options)
        : filePath{std::move(filePath)}, options{std::move(options)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<ExportDBPrintInfo>(new ExportDBPrintInfo(*this));
    }

private:
    ExportDBPrintInfo(const ExportDBPrintInfo& other)
        : OPPrintInfo{other}, filePath{other.filePath}, options{other.options} {}
};

class ExportDB final : public SimpleSink {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::EXPORT_DATABASE;

public:
    ExportDB(common::FileScanInfo boundFileInfo, bool schemaOnly,
        std::shared_ptr<FactorizedTable> messageTable, physical_op_id id,
        std::unique_ptr<OPPrintInfo> printInfo,
        const std::shared_ptr<bool>& canUseParallelCSVReader = std::make_shared<bool>(true))
        : SimpleSink{type_, std::move(messageTable), id, std::move(printInfo)},
          boundFileInfo{std::move(boundFileInfo)}, schemaOnly{schemaOnly}, canUseParallelCSVReader{canUseParallelCSVReader} {}

    void initGlobalStateInternal(ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<ExportDB>(boundFileInfo.copy(), schemaOnly, messageTable, id,
            printInfo->copy(), canUseParallelCSVReader);
    }
    auto setParallelReaderFalse() {
        auto parallelFalse = [this](){*canUseParallelCSVReader = false;};
        return parallelFalse;
    }

private:
    common::FileScanInfo boundFileInfo;
    bool schemaOnly;
    std::shared_ptr<bool> canUseParallelCSVReader;
};
} // namespace processor
} // namespace kuzu
