#pragma once

#include <utility>

#include "processor/operator/physical_operator.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

class SchemaMapping : public PhysicalOperator {
public:
    SchemaMapping(
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator(
              PhysicalOperatorType::SCHEMA_MAPPING, std::move(child), id, paramsString) {}

    bool getNextTuplesInternal(ExecutionContext* context) override {
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        return true;
    }

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<SchemaMapping>(children[0]->clone(), id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
