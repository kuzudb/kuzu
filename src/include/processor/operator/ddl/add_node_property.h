#pragma once

#include "processor/operator/ddl/add_property.h"

namespace kuzu {
namespace processor {

class AddNodeProperty : public AddProperty {
public:
    AddNodeProperty(catalog::Catalog* catalog, common::table_id_t tableID, std::string propertyName,
        std::unique_ptr<common::LogicalType> dataType,
        std::unique_ptr<evaluator::ExpressionEvaluator> defaultValueEvaluator,
        storage::StorageManager& storageManager, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : AddProperty{catalog, tableID, std::move(propertyName), std::move(dataType),
              std::move(defaultValueEvaluator), storageManager, outputPos, id, paramsString} {}

    void executeDDLInternal() final;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AddNodeProperty>(catalog, tableID, propertyName, dataType->copy(),
            defaultValueEvaluator->clone(), storageManager, outputPos, id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
