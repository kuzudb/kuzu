#pragma once

#include "processor/operator/ddl/add_property.h"

namespace kuzu {
namespace processor {

class AddNodeProperty final : public AddProperty {
public:
    AddNodeProperty(common::table_id_t tableID, std::string propertyName,
        std::unique_ptr<common::LogicalType> dataType,
        std::unique_ptr<evaluator::ExpressionEvaluator> defaultValueEvaluator,
        const DataPos& outputPos, uint32_t id, const std::string& paramsString)
        : AddProperty{tableID, std::move(propertyName), std::move(dataType),
              std::move(defaultValueEvaluator), outputPos, id, paramsString} {}

    void executeDDLInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AddNodeProperty>(tableID, propertyName, dataType->copy(),
            defaultValueEvaluator->clone(), outputPos, id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
