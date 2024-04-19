#pragma once

#include "common/copier_config/reader_config.h"
#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

class ExportDB : public Simple {
public:
    ExportDB(common::ReaderConfig boundFileInfo, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString, std::vector<std::unique_ptr<PhysicalOperator>> children)
        : Simple{PhysicalOperatorType::EXPORT_DATABASE, std::move(children), outputPos, id,
              paramsString},
          boundFileInfo{std::move(boundFileInfo)} {}

    void executeInternal(ExecutionContext* context) final;
    std::string getOutputMsg() final;
    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ExportDB>(std::move(boundFileInfo), outputPos, id, paramsString,
            std::move(children));
    }

private:
    common::ReaderConfig boundFileInfo;
    std::string extraMsg;
};
} // namespace processor
} // namespace kuzu
