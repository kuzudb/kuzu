#pragma once

#include "ddl.h"
#include "expression_evaluator/base_evaluator.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace processor {

class AddProperty : public DDL {
public:
    AddProperty(catalog::Catalog* catalog, common::table_id_t tableID, std::string propertyName,
        common::LogicalType dataType,
        std::unique_ptr<evaluator::BaseExpressionEvaluator> expressionEvaluator,
        storage::StorageManager& storageManager, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : DDL{PhysicalOperatorType::ADD_PROPERTY, catalog, outputPos, id, paramsString},
          tableID{tableID}, propertyName{std::move(propertyName)}, dataType{std::move(dataType)},
          expressionEvaluator{std::move(expressionEvaluator)}, storageManager{storageManager} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override {
        DDL::initLocalStateInternal(resultSet, context);
        expressionEvaluator->init(*resultSet, context->memoryManager);
    }

    void executeDDLInternal() override;

    std::string getOutputMsg() override { return {"Add Succeed."}; }

protected:
    inline bool isDefaultValueNull() const {
        auto expressionVector = expressionEvaluator->resultVector;
        return expressionVector->isNull(expressionVector->state->selVector->selectedPositions[0]);
    }

    uint8_t* getDefaultVal();

protected:
    common::table_id_t tableID;
    std::string propertyName;
    common::LogicalType dataType;
    std::unique_ptr<evaluator::BaseExpressionEvaluator> expressionEvaluator;
    storage::StorageManager& storageManager;
};

} // namespace processor
} // namespace kuzu
