#pragma once

#include "ddl.h"
#include "expression_evaluator/expression_evaluator.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace processor {

class AddProperty : public DDL {
public:
    AddProperty(catalog::Catalog* catalog, common::table_id_t tableID, std::string propertyName,
        std::unique_ptr<common::LogicalType> dataType,
        std::unique_ptr<evaluator::ExpressionEvaluator> defaultValueEvaluator,
        storage::StorageManager& storageManager, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : DDL{PhysicalOperatorType::ADD_PROPERTY, catalog, outputPos, id, paramsString},
          tableID{tableID}, propertyName{std::move(propertyName)}, dataType{std::move(dataType)},
          defaultValueEvaluator{std::move(defaultValueEvaluator)}, storageManager{storageManager} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override {
        DDL::initLocalStateInternal(resultSet, context);
        defaultValueEvaluator->init(*resultSet, context->memoryManager);
    }

    void executeDDLInternal() override = 0;

    std::string getOutputMsg() override { return {"Add Succeed."}; }

protected:
    common::ValueVector* getDefaultValVector();

protected:
    common::table_id_t tableID;
    std::string propertyName;
    std::unique_ptr<common::LogicalType> dataType;
    std::unique_ptr<evaluator::ExpressionEvaluator> defaultValueEvaluator;
    storage::StorageManager& storageManager;
};

} // namespace processor
} // namespace kuzu
