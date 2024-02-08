#pragma once

#include "catalog/catalog.h"
#include "processor/operator/sink.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

class ExportDB : public PhysicalOperator {
public:
    ExportDB(std::string filePath, std::vector<std::string> tableNames,
        std::vector<std::string> tableCyphers, std::vector<std::string> macroCyphers, uint32_t id,
        const std::string& paramsString, std::vector<std::unique_ptr<PhysicalOperator>> children)
        : PhysicalOperator{PhysicalOperatorType::EXPORT_DATABASE, id, paramsString},
          filePath(filePath), tableNames(std::move(tableNames)),
          tableCyphers(std::move(tableCyphers)), macroCyphers(macroCyphers) {
        PhysicalOperator::children = std::move(children);
    }

    inline bool canParallel() const override { return false; }

    bool isSource() const final { return true; }

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<ExportDB>(filePath, std::move(tableNames), std::move(tableCyphers),
            std::move(macroCyphers), id, paramsString, std::move(children));
    }

private:
    std::string filePath;
    std::vector<std::string> tableNames;
    std::vector<std::string> tableCyphers;
    std::vector<std::string> macroCyphers;
};
} // namespace processor
} // namespace kuzu
