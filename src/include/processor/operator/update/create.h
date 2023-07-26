#pragma once

#include "insert_manager.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {


//struct CreateNodeInfo {
//    catalog::NodeTableSchema* schema;
//    storage::NodeTable* table;
//    std::unique_ptr<evaluator::BaseExpressionEvaluator> primaryKeyEvaluator;
//    std::vector<storage::RelTable*> relTablesToInit;
//    DataPos outNodeIDVectorPos;
//
//    CreateNodeInfo(catalog::NodeTableSchema* schema, storage::NodeTable* table,
//        std::unique_ptr<evaluator::BaseExpressionEvaluator> primaryKeyEvaluator,
//        std::vector<storage::RelTable*> relTablesToInit, const DataPos& dataPos)
//        : schema{schema}, table{table}, primaryKeyEvaluator{std::move(primaryKeyEvaluator)},
//          relTablesToInit{std::move(relTablesToInit)}, outNodeIDVectorPos{dataPos} {}
//
//    inline std::unique_ptr<CreateNodeInfo> clone() {
//        return std::make_unique<CreateNodeInfo>(schema, table,
//            primaryKeyEvaluator != nullptr ? primaryKeyEvaluator->clone() : nullptr,
//            relTablesToInit, outNodeIDVectorPos);
//    }
//};

class CreateNode : public PhysicalOperator {
public:
    CreateNode(std::vector<std::unique_ptr<NodeTableInsertManager>> managers,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::CREATE_NODE, std::move(child), id, paramsString},
          managers{std::move(managers)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    std::vector<std::unique_ptr<NodeTableInsertManager>> managers;
};

class CreateRel : public PhysicalOperator {
public:
    CreateRel(std::vector<std::unique_ptr<RelTableInsertManager>> managers,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::CREATE_REL, std::move(child), id, paramsString},
          managers{std::move(managers)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    std::vector<std::unique_ptr<RelTableInsertManager>> managers;
};

} // namespace processor
} // namespace kuzu
