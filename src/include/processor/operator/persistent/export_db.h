#pragma once

#include "common/copier_config/reader_config.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class ExportDB : public PhysicalOperator {
public:
    ExportDB(common::ReaderConfig boundFileInfo, uint32_t id, const std::string& paramsString,
        std::vector<std::unique_ptr<PhysicalOperator>> children)
        : PhysicalOperator{PhysicalOperatorType::EXPORT_DATABASE, std::move(children), id,
              paramsString},
          boundFileInfo{std::move(boundFileInfo)} {}

    bool canParallel() const override { return false; }

    bool isSource() const override { return true; }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ExportDB>(std::move(boundFileInfo), id, paramsString,
            std::move(children));
    }

private:
    common::ReaderConfig boundFileInfo;
};
} // namespace processor
} // namespace kuzu
