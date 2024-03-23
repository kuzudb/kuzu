#pragma once

#include "add_property.h"

namespace kuzu {
namespace processor {

class AddRelProperty final : public AddProperty {
public:
    AddRelProperty(common::table_id_t tableID, std::string propertyName,
        std::unique_ptr<common::LogicalType> dataType,
        std::unique_ptr<evaluator::ExpressionEvaluator> expressionEvaluator,
        const DataPos& outputPos, uint32_t id, const std::string& paramsString)
        : AddProperty(tableID, std::move(propertyName), std::move(dataType),
              std::move(expressionEvaluator), outputPos, id, paramsString) {}

    void executeDDLInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AddRelProperty>(tableID, propertyName, dataType->copy(),
            defaultValueEvaluator->clone(), outputPos, id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
