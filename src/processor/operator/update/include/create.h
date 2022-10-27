#pragma once

#include "src/expression_evaluator/include/base_evaluator.h"
#include "src/processor/operator/include/physical_operator.h"
#include "src/storage/store/include/node_table.h"

namespace graphflow {
namespace processor {
using namespace graphflow::evaluator;

struct CreateNodeInfo {
    NodeTable* table;
    DataPos outNodeIDVectorPos;

    CreateNodeInfo(NodeTable* table, const DataPos& dataPos)
        : table{table}, outNodeIDVectorPos{dataPos} {}

    inline unique_ptr<CreateNodeInfo> clone() {
        return make_unique<CreateNodeInfo>(table, outNodeIDVectorPos);
    }
};

class CreateNode : public PhysicalOperator {
public:
    CreateNode(vector<unique_ptr<CreateNodeInfo>> createNodeInfos, RelsStore& relsStore,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{std::move(child), id, paramsString},
          createNodeInfos{std::move(createNodeInfos)}, relsStore{relsStore} {}

    inline PhysicalOperatorType getOperatorType() override {
        return PhysicalOperatorType::CREATE_NODE;
    }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        vector<unique_ptr<CreateNodeInfo>> clonedCreateNodeInfos;
        for (auto& createNodeInfo : createNodeInfos) {
            clonedCreateNodeInfos.push_back(createNodeInfo->clone());
        }
        return make_unique<CreateNode>(
            std::move(clonedCreateNodeInfos), relsStore, children[0]->clone(), id, paramsString);
    }

private:
    vector<unique_ptr<CreateNodeInfo>> createNodeInfos;
    vector<ValueVector*> outValueVectors;
    RelsStore& relsStore;
};

struct CreateRelInfo {
    RelTable* table;
    DataPos srcNodePos;
    DataPos dstNodePos;
    vector<unique_ptr<BaseExpressionEvaluator>> evaluators;
    uint32_t relIDEvaluatorIdx;

    CreateRelInfo(RelTable* table, const DataPos& srcNodePos, const DataPos& dstNodePos,
        vector<unique_ptr<BaseExpressionEvaluator>> evaluators, uint32_t relIDEvaluatorIdx)
        : table{table}, srcNodePos{srcNodePos}, dstNodePos{dstNodePos},
          evaluators{std::move(evaluators)}, relIDEvaluatorIdx{relIDEvaluatorIdx} {}

    unique_ptr<CreateRelInfo> clone() {
        vector<unique_ptr<BaseExpressionEvaluator>> clonedEvaluators;
        for (auto& evaluator : evaluators) {
            clonedEvaluators.push_back(evaluator->clone());
        }
        return make_unique<CreateRelInfo>(
            table, srcNodePos, dstNodePos, std::move(clonedEvaluators), relIDEvaluatorIdx);
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

    bool getNextTuples() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        vector<unique_ptr<CreateRelInfo>> clonedCreateRelInfos;
        for (auto& createRelInfo : createRelInfos) {
            clonedCreateRelInfos.push_back(createRelInfo->clone());
        }
        return make_unique<CreateRel>(relsStatistics, std::move(clonedCreateRelInfos),
            children[0]->clone(), id, paramsString);
    }

private:
    RelsStatistics& relsStatistics;
    vector<unique_ptr<CreateRelInfo>> createRelInfos;
    vector<shared_ptr<ValueVector>> srcNodeIDVectorPerRelTable;
    vector<shared_ptr<ValueVector>> dstNodeIDVectorPerRelTable;
    vector<vector<shared_ptr<ValueVector>>> relPropertyVectorsPerRelTable;
};

} // namespace processor
} // namespace graphflow
