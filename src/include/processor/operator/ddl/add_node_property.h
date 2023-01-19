#pragma once

#include "add_property.h"

namespace kuzu {
namespace processor {

class AddNodeProperty : public AddProperty {
public:
    AddNodeProperty(Catalog* catalog, table_id_t tableID, string propertyName, DataType dataType,
        unique_ptr<evaluator::BaseExpressionEvaluator> expressionEvaluator,
        StorageManager& storageManager, const DataPos& outputPos, uint32_t id,
        const string& paramsString)
        : AddProperty(catalog, tableID, std::move(propertyName), std::move(dataType),
              std::move(expressionEvaluator), storageManager, outputPos, id, paramsString) {}

    void executeDDLInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AddNodeProperty>(catalog, tableID, propertyName, dataType,
            expressionEvaluator->clone(), storageManager, outputPos, id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
