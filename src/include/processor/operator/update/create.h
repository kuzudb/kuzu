#pragma once

#include "expression_evaluator/base_evaluator.h"
#include "processor/operator/physical_operator.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

struct CreateNodeInfo {
    storage::NodeTable* table;
    std::unique_ptr<evaluator::BaseExpressionEvaluator> primaryKeyEvaluator;
    std::vector<storage::RelTable*> relTablesToInit;
    DataPos outNodeIDVectorPos;

    CreateNodeInfo(storage::NodeTable* table,
        std::unique_ptr<evaluator::BaseExpressionEvaluator> primaryKeyEvaluator,
        std::vector<storage::RelTable*> relTablesToInit, const DataPos& dataPos)
        : table{table}, primaryKeyEvaluator{std::move(primaryKeyEvaluator)},
          relTablesToInit{std::move(relTablesToInit)}, outNodeIDVectorPos{dataPos} {}

    inline std::unique_ptr<CreateNodeInfo> clone() {
        return std::make_unique<CreateNodeInfo>(table,
            primaryKeyEvaluator != nullptr ? primaryKeyEvaluator->clone() : nullptr,
            relTablesToInit, outNodeIDVectorPos);
    }
};

class CreateNode : public PhysicalOperator {
public:
    CreateNode(std::vector<std::unique_ptr<CreateNodeInfo>> createNodeInfos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::CREATE_NODE, std::move(child), id, paramsString},
          createNodeInfos{std::move(createNodeInfos)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        std::vector<std::unique_ptr<CreateNodeInfo>> clonedCreateNodeInfos;
        for (auto& createNodeInfo : createNodeInfos) {
            clonedCreateNodeInfos.push_back(createNodeInfo->clone());
        }
        return std::make_unique<CreateNode>(
            std::move(clonedCreateNodeInfos), children[0]->clone(), id, paramsString);
    }

private:
    std::vector<std::unique_ptr<CreateNodeInfo>> createNodeInfos;
    std::vector<common::ValueVector*> outValueVectors;
};

struct CreateRelInfo {
    storage::RelTable* table;
    DataPos srcNodePos;
    common::table_id_t srcNodeTableID;
    DataPos dstNodePos;
    common::table_id_t dstNodeTableID;
    std::vector<std::unique_ptr<evaluator::BaseExpressionEvaluator>> evaluators;
    uint32_t relIDEvaluatorIdx;

    CreateRelInfo(storage::RelTable* table, const DataPos& srcNodePos,
        common::table_id_t srcNodeTableID, const DataPos& dstNodePos,
        common::table_id_t dstNodeTableID,
        std::vector<std::unique_ptr<evaluator::BaseExpressionEvaluator>> evaluators,
        uint32_t relIDEvaluatorIdx)
        : table{table}, srcNodePos{srcNodePos}, srcNodeTableID{srcNodeTableID},
          dstNodePos{dstNodePos}, dstNodeTableID{dstNodeTableID}, evaluators{std::move(evaluators)},
          relIDEvaluatorIdx{relIDEvaluatorIdx} {}

    std::unique_ptr<CreateRelInfo> clone() {
        std::vector<std::unique_ptr<evaluator::BaseExpressionEvaluator>> clonedEvaluators;
        for (auto& evaluator : evaluators) {
            clonedEvaluators.push_back(evaluator->clone());
        }
        return make_unique<CreateRelInfo>(table, srcNodePos, srcNodeTableID, dstNodePos,
            dstNodeTableID, std::move(clonedEvaluators), relIDEvaluatorIdx);
    }
};

class CreateRel : public PhysicalOperator {
public:
    CreateRel(storage::RelsStatistics& relsStatistics,
        std::vector<std::unique_ptr<CreateRelInfo>> createRelInfos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::CREATE_REL, std::move(child), id, paramsString},
          relsStatistics{relsStatistics}, createRelInfos{std::move(createRelInfos)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        std::vector<std::unique_ptr<CreateRelInfo>> clonedCreateRelInfos;
        for (auto& createRelInfo : createRelInfos) {
            clonedCreateRelInfos.push_back(createRelInfo->clone());
        }
        return make_unique<CreateRel>(relsStatistics, std::move(clonedCreateRelInfos),
            children[0]->clone(), id, paramsString);
    }

private:
    struct CreateRelVectors {
        common::ValueVector* srcNodeIDVector;
        common::ValueVector* dstNodeIDVector;
        std::vector<common::ValueVector*> propertyVectors;
    };

private:
    storage::RelsStatistics& relsStatistics;
    std::vector<std::unique_ptr<CreateRelInfo>> createRelInfos;
    std::vector<std::unique_ptr<CreateRelVectors>> createRelVectorsPerRel;
};

} // namespace processor
} // namespace kuzu
