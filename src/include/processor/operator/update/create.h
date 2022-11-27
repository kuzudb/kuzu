#pragma once

#include "expression_evaluator/base_evaluator.h"
#include "processor/operator/physical_operator.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {
using namespace kuzu::evaluator;

struct CreateNodeInfo {
    NodeTable* table;
    unique_ptr<BaseExpressionEvaluator> primaryKeyEvaluator;
    vector<RelTable*> relTablesToInit;
    DataPos outNodeIDVectorPos;

    CreateNodeInfo(NodeTable* table, unique_ptr<BaseExpressionEvaluator> primaryKeyEvaluator,
        vector<RelTable*> relTablesToInit, const DataPos& dataPos)
        : table{table}, primaryKeyEvaluator{std::move(primaryKeyEvaluator)},
          relTablesToInit{std::move(relTablesToInit)}, outNodeIDVectorPos{dataPos} {}

    inline unique_ptr<CreateNodeInfo> clone() {
        return make_unique<CreateNodeInfo>(
            table, primaryKeyEvaluator->clone(), relTablesToInit, outNodeIDVectorPos);
    }
};

class CreateNode : public PhysicalOperator {
public:
    CreateNode(vector<unique_ptr<CreateNodeInfo>> createNodeInfos,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{std::move(child), id, paramsString}, createNodeInfos{
                                                                    std::move(createNodeInfos)} {}

    inline PhysicalOperatorType getOperatorType() override {
        return PhysicalOperatorType::CREATE_NODE;
    }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        vector<unique_ptr<CreateNodeInfo>> clonedCreateNodeInfos;
        for (auto& createNodeInfo : createNodeInfos) {
            clonedCreateNodeInfos.push_back(createNodeInfo->clone());
        }
        return make_unique<CreateNode>(
            std::move(clonedCreateNodeInfos), children[0]->clone(), id, paramsString);
    }

private:
    vector<unique_ptr<CreateNodeInfo>> createNodeInfos;
    vector<ValueVector*> outValueVectors;
};

struct CreateRelInfo {
    RelTable* table;
    DataPos srcNodePos;
    table_id_t srcNodeTableID;
    DataPos dstNodePos;
    table_id_t dstNodeTableID;
    vector<unique_ptr<BaseExpressionEvaluator>> evaluators;
    uint32_t relIDEvaluatorIdx;

    CreateRelInfo(RelTable* table, const DataPos& srcNodePos, table_id_t srcNodeTableID,
        const DataPos& dstNodePos, table_id_t dstNodeTableID,
        vector<unique_ptr<BaseExpressionEvaluator>> evaluators, uint32_t relIDEvaluatorIdx)
        : table{table}, srcNodePos{srcNodePos}, srcNodeTableID{srcNodeTableID},
          dstNodePos{dstNodePos}, dstNodeTableID{dstNodeTableID}, evaluators{std::move(evaluators)},
          relIDEvaluatorIdx{relIDEvaluatorIdx} {}

    unique_ptr<CreateRelInfo> clone() {
        vector<unique_ptr<BaseExpressionEvaluator>> clonedEvaluators;
        for (auto& evaluator : evaluators) {
            clonedEvaluators.push_back(evaluator->clone());
        }
        return make_unique<CreateRelInfo>(table, srcNodePos, srcNodeTableID, dstNodePos,
            dstNodeTableID, std::move(clonedEvaluators), relIDEvaluatorIdx);
    }
};

class CreateRel : public PhysicalOperator {
public:
    CreateRel(RelsStatistics& relsStatistics, vector<unique_ptr<CreateRelInfo>> createRelInfos,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{std::move(child), id, paramsString}, relsStatistics{relsStatistics},
          createRelInfos{std::move(createRelInfos)} {}

    PhysicalOperatorType getOperatorType() override { return PhysicalOperatorType::CREATE_REL; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        vector<unique_ptr<CreateRelInfo>> clonedCreateRelInfos;
        for (auto& createRelInfo : createRelInfos) {
            clonedCreateRelInfos.push_back(createRelInfo->clone());
        }
        return make_unique<CreateRel>(relsStatistics, std::move(clonedCreateRelInfos),
            children[0]->clone(), id, paramsString);
    }

private:
    struct CreateRelVectors {
        shared_ptr<ValueVector> srcNodeIDVector;
        shared_ptr<ValueVector> dstNodeIDVector;
        vector<shared_ptr<ValueVector>> propertyVectors;
    };

private:
    RelsStatistics& relsStatistics;
    vector<unique_ptr<CreateRelInfo>> createRelInfos;
    vector<unique_ptr<CreateRelVectors>> createRelVectorsPerRel;
};

} // namespace processor
} // namespace kuzu
