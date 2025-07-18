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
        const std::shared_ptr<bool>& parallel = std::make_shared<bool>(true))
        : SimpleSink{type_, std::move(messageTable), id, std::move(printInfo)},
          boundFileInfo{std::move(boundFileInfo)}, schemaOnly{schemaOnly}, parallel{parallel} {}

    void initGlobalStateInternal(ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<ExportDB>(boundFileInfo.copy(), schemaOnly, messageTable, id,
            printInfo->copy(), parallel);
    }
    // TODO(TANVIR) outside of this class parallel should only ever be set to false.
    // We should instead expose a function ptr which does exactly this and let
    // the outside class call it.
    bool* getParallel() { return parallel.get(); }

private:
    common::FileScanInfo boundFileInfo;
    bool schemaOnly;
    std::shared_ptr<bool> parallel;
};
} // namespace processor
} // namespace kuzu
