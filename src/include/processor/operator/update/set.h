#pragma once

#include "expression_evaluator/base_evaluator.h"
#include "processor/operator/physical_operator.h"
#include "storage/storage_structure/column.h"
#include "storage/store/rel_table.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

struct SetNodePropertyInfo {
    Column* column;
    DataPos nodeIDPos;
    unique_ptr<BaseExpressionEvaluator> evaluator;

    SetNodePropertyInfo(
        Column* column, const DataPos& nodeIDPos, unique_ptr<BaseExpressionEvaluator> evaluator)
        : column{column}, nodeIDPos{nodeIDPos}, evaluator{std::move(evaluator)} {}

    inline unique_ptr<SetNodePropertyInfo> clone() const {
        return make_unique<SetNodePropertyInfo>(column, nodeIDPos, evaluator->clone());
    }
};

class SetNodeProperty : public PhysicalOperator {
public:
    SetNodeProperty(vector<unique_ptr<SetNodePropertyInfo>> infos,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SET_NODE_PROPERTY, std::move(child), id,
              paramsString},
          infos{std::move(infos)} {}

    ~SetNodeProperty() override = default;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    vector<unique_ptr<SetNodePropertyInfo>> infos;

    vector<shared_ptr<ValueVector>> nodeIDVectors;
};

struct SetRelPropertyInfo {
    RelTable* table;
    DataPos srcNodePos;
    DataPos dstNodePos;
    DataPos relIDPos;
    // TODO(Ziyi): see issue 1122 and do a typedef on our column & list idx.
    uint64_t propertyId;
    unique_ptr<BaseExpressionEvaluator> evaluator;

    SetRelPropertyInfo(RelTable* table, const DataPos& srcNodePos, const DataPos& dstNodePos,
        const DataPos& relIDPos, uint64_t propertyId, unique_ptr<BaseExpressionEvaluator> evaluator)
        : table{table}, srcNodePos{srcNodePos}, dstNodePos{dstNodePos}, relIDPos{relIDPos},
          propertyId{propertyId}, evaluator{std::move(evaluator)} {}

    inline unique_ptr<SetRelPropertyInfo> clone() const {
        return make_unique<SetRelPropertyInfo>(
            table, srcNodePos, dstNodePos, relIDPos, propertyId, evaluator->clone());
    }
};

class SetRelProperty : public PhysicalOperator {
public:
    SetRelProperty(vector<unique_ptr<SetRelPropertyInfo>> infos, unique_ptr<PhysicalOperator> child,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SET_NODE_PROPERTY, std::move(child), id,
              paramsString},
          infos{std::move(infos)} {}

    ~SetRelProperty() override = default;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    vector<unique_ptr<SetRelPropertyInfo>> infos;

    vector<shared_ptr<ValueVector>> srcNodeVectors;
    vector<shared_ptr<ValueVector>> dstNodeVectors;
    vector<shared_ptr<ValueVector>> relIDVectors;
};

} // namespace processor
} // namespace kuzu
