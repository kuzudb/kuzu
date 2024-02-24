#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class ImportDB : public PhysicalOperator {
public:
    ImportDB(std::string query, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::IMPORT_DATABASE, id, paramsString}, query{query} {}

    bool canParallel() const override { return false; }

    bool isSource() const override { return true; }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ImportDB>(query, id, paramsString);
    }

private:
    std::string query;
};
} // namespace processor
} // namespace kuzu
