#pragma once

#include "common/copier_config/reader_config.h"
#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

struct ExportDBPrintInfo final : OPPrintInfo {
    std::string filePath;
    std::unordered_map<std::string, common::Value> options;

    ExportDBPrintInfo(std::string filePath, std::unordered_map<std::string, common::Value> options)
        : filePath{std::move(filePath)}, options{std::move(options)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<ExportDBPrintInfo>(new ExportDBPrintInfo(*this));
    }

private:
    ExportDBPrintInfo(const ExportDBPrintInfo& other)
        : OPPrintInfo{other}, filePath{other.filePath}, options{other.options} {}
};

class ExportDB final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::EXPORT_DATABASE;

public:
    ExportDB(common::ReaderConfig boundFileInfo, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, outputPos, id, std::move(printInfo)},
          boundFileInfo{std::move(boundFileInfo)} {}

    void initGlobalStateInternal(ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;
    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ExportDB>(std::move(boundFileInfo), outputPos, id,
            printInfo->copy());
    }

private:
    common::ReaderConfig boundFileInfo;
    std::string extraMsg;
};
} // namespace processor
} // namespace kuzu
