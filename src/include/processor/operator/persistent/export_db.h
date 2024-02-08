#pragma once

#include "common/copier_config/csv_reader_config.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class ExportDB : public PhysicalOperator {
public:
    ExportDB(std::string filePath, common::CSVOption copyToOption, uint32_t id,
        const std::string& paramsString, std::vector<std::unique_ptr<PhysicalOperator>> children)
        : PhysicalOperator{PhysicalOperatorType::EXPORT_DATABASE, std::move(children), id,
              paramsString},
          filePath(filePath), copyToOption{std::move(copyToOption)} {}

    bool canParallel() const override { return false; }

    bool isSource() const override { return true; }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ExportDB>(
            filePath, std::move(copyToOption), id, paramsString, std::move(children));
    }

private:
    std::string filePath;
    common::CSVOption copyToOption;
};
} // namespace processor
} // namespace kuzu
