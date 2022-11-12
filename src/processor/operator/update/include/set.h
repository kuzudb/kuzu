#pragma once

#include "src/expression_evaluator/include/base_evaluator.h"
#include "src/processor/operator/include/physical_operator.h"
#include "src/storage/storage_structure/include/column.h"
#include "src/storage/storage_structure/include/lists/unstructured_property_lists.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

class BaseSetNodeProperty : public PhysicalOperator {
protected:
    BaseSetNodeProperty(vector<DataPos> nodeIDPositions,
        vector<unique_ptr<BaseExpressionEvaluator>> expressionEvaluators,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{std::move(child), id, paramsString},
          nodeIDPositions{std::move(nodeIDPositions)}, expressionEvaluators{
                                                           std::move(expressionEvaluators)} {}
    virtual ~BaseSetNodeProperty() override = default;

    PhysicalOperatorType getOperatorType() override = 0;

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override = 0;

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

    inline PhysicalOperatorType getOperatorType() override {
        return PhysicalOperatorType::SET_STRUCTURED_NODE_PROPERTY;
    }

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    vector<Column*> columns;
};

class SetNodeUnstructuredProperty : public BaseSetNodeProperty {
public:
    SetNodeUnstructuredProperty(vector<DataPos> nodeIDPositions,
        vector<unique_ptr<BaseExpressionEvaluator>> expressionEvaluators,
        vector<pair<uint32_t, UnstructuredPropertyLists*>> propertyKeyListPairs,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : BaseSetNodeProperty{std::move(nodeIDPositions), std::move(expressionEvaluators),
              std::move(child), id, paramsString},
          propertyKeyListPairs{std::move(propertyKeyListPairs)} {}

    PhysicalOperatorType getOperatorType() override {
        return PhysicalOperatorType::SET_UNSTRUCTURED_NODE_PROPERTY;
    }

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    vector<pair<uint32_t, UnstructuredPropertyLists*>> propertyKeyListPairs;
};

} // namespace processor
} // namespace kuzu
