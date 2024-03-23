#pragma once

#include "ddl.h"
#include "expression_evaluator/expression_evaluator.h"

namespace kuzu {
namespace processor {

class AddProperty : public DDL {
public:
    AddProperty(common::table_id_t tableID, std::string propertyName,
        std::unique_ptr<common::LogicalType> dataType,
        std::unique_ptr<evaluator::ExpressionEvaluator> defaultValueEvaluator,
        const DataPos& outputPos, uint32_t id, const std::string& paramsString)
        : DDL{PhysicalOperatorType::ADD_PROPERTY, outputPos, id, paramsString}, tableID{tableID},
          propertyName{std::move(propertyName)}, dataType{std::move(dataType)},
          defaultValueEvaluator{std::move(defaultValueEvaluator)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override {
        DDL::initLocalStateInternal(resultSet, context);
        defaultValueEvaluator->init(*resultSet, context->clientContext->getMemoryManager());
    }

    void executeDDLInternal(ExecutionContext* context) override = 0;

    std::string getOutputMsg() override { return {"Add Succeed."}; }

protected:
    common::ValueVector* getDefaultValVector(ExecutionContext* context);

protected:
    common::table_id_t tableID;
    std::string propertyName;
    std::unique_ptr<common::LogicalType> dataType;
    std::unique_ptr<evaluator::ExpressionEvaluator> defaultValueEvaluator;
};

} // namespace processor
} // namespace kuzu
