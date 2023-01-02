#pragma once

#include "expression_evaluator/base_evaluator.h"
#include "processor/operator/physical_operator.h"
#include "storage/storage_structure/column.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

class BaseSetNodeProperty : public PhysicalOperator {
protected:
    BaseSetNodeProperty(vector<DataPos> nodeIDPositions,
        vector<unique_ptr<BaseExpressionEvaluator>> expressionEvaluators,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SET_NODE_PROPERTY, std::move(child), id,
              paramsString},
          nodeIDPositions{std::move(nodeIDPositions)}, expressionEvaluators{
                                                           std::move(expressionEvaluators)} {}
    virtual ~BaseSetNodeProperty() override = default;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override = 0;

    unique_ptr<PhysicalOperator> clone() override = 0;

protected:
    vector<DataPos> nodeIDPositions;
    vector<shared_ptr<ValueVector>> nodeIDVectors;
    vector<unique_ptr<BaseExpressionEvaluator>> expressionEvaluators;
};

class SetNodeStructuredProperty : public BaseSetNodeProperty {
public:
    SetNodeStructuredProperty(vector<DataPos> nodeIDPositions,
        vector<unique_ptr<BaseExpressionEvaluator>> expressionEvaluators, vector<Column*> columns,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : BaseSetNodeProperty{std::move(nodeIDPositions), std::move(expressionEvaluators),
              std::move(child), id, paramsString},
          columns{std::move(columns)} {}

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    vector<Column*> columns;
};

} // namespace processor
} // namespace kuzu
