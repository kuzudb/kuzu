#pragma once

#include "common/copier_config/reader_config.h"
#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

class ExportDB final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::EXPORT_DATABASE;

public:
    ExportDB(common::ReaderConfig boundFileInfo, const DataPos& outputPos, uint32_t id,
        std::vector<std::unique_ptr<PhysicalOperator>> children,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, std::move(children), outputPos, id, std::move(printInfo)},
          boundFileInfo{std::move(boundFileInfo)} {}

    void executeInternal(ExecutionContext* context) override;
    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ExportDB>(std::move(boundFileInfo), outputPos, id,
            std::move(children), printInfo->copy());
    }

private:
    common::ReaderConfig boundFileInfo;
    std::string extraMsg;
};
} // namespace processor
} // namespace kuzu
