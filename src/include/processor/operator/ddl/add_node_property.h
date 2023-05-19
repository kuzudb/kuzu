#pragma once

#include "add_property.h"

namespace kuzu {
namespace processor {

class AddNodeProperty : public AddProperty {
public:
    AddNodeProperty(catalog::Catalog* catalog, common::table_id_t tableID, std::string propertyName,
        common::LogicalType dataType,
        std::unique_ptr<evaluator::BaseExpressionEvaluator> expressionEvaluator,
        storage::StorageManager& storageManager, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : AddProperty(catalog, tableID, std::move(propertyName), std::move(dataType),
              std::move(expressionEvaluator), storageManager, outputPos, id, paramsString) {}

    void executeDDLInternal() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AddNodeProperty>(catalog, tableID, propertyName, dataType,
            expressionEvaluator->clone(), storageManager, outputPos, id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
