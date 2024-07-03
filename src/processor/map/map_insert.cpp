#include "binder/expression/rel_expression.h"
#include "common/cast.h"
#include "planner/operator/persistent/logical_insert.h"
#include "processor/operator/persistent/insert.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"

using namespace kuzu::evaluator;
using namespace kuzu::planner;
using namespace kuzu::storage;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace processor {

static std::vector<DataPos> populateReturnVectorsPos(const LogicalInsertInfo& info,
    const Schema& schema) {
    std::vector<DataPos> result;
    for (auto i = 0u; i < info.columnDataExprs.size(); ++i) {
        if (info.isReturnColumnExprs[i]) {
            result.emplace_back(schema.getExpressionPos(*info.columnExprs[i]));
        } else {
            result.push_back(DataPos::getInvalidPos());
        }
    }
    return result;
}

std::unique_ptr<NodeInsertExecutor> PlanMapper::getNodeInsertExecutor(const LogicalInsertInfo* info,
    const Schema& inSchema, const Schema& outSchema) const {
    auto storageManager = clientContext->getStorageManager();
    auto& node = info->pattern->constCast<NodeExpression>();
    auto nodeTableID = node.getSingleTableID();
    auto table = storageManager->getTable(nodeTableID)->ptrCast<NodeTable>();
    auto nodeIDPos = getDataPos(*node.getInternalID(), outSchema);
    auto returnVectorsPos = populateReturnVectorsPos(*info, outSchema);
    std::vector<std::unique_ptr<ExpressionEvaluator>> evaluators;
    evaluators.reserve(info->columnDataExprs.size());
    auto exprMapper = ExpressionMapper(&inSchema);
    for (auto& expr : info->columnDataExprs) {
        evaluators.push_back(exprMapper.getEvaluator(expr));
    }
    return std::make_unique<NodeInsertExecutor>(table, nodeIDPos, std::move(returnVectorsPos),
        std::move(evaluators), info->conflictAction);
}

std::unique_ptr<RelInsertExecutor> PlanMapper::getRelInsertExecutor(
    const planner::LogicalInsertInfo* info, const planner::Schema& inSchema,
    const planner::Schema& outSchema) const {
    auto storageManager = clientContext->getStorageManager();
    auto rel = ku_dynamic_cast<Expression*, RelExpression*>(info->pattern.get());
    auto relTableID = rel->getSingleTableID();
    auto table = ku_dynamic_cast<Table*, RelTable*>(storageManager->getTable(relTableID));
    auto srcNode = rel->getSrcNode();
    auto dstNode = rel->getDstNode();
    auto srcNodePos = DataPos(inSchema.getExpressionPos(*srcNode->getInternalID()));
    auto dstNodePos = DataPos(inSchema.getExpressionPos(*dstNode->getInternalID()));
    auto returnVectorsPos = populateReturnVectorsPos(*info, outSchema);
    std::vector<std::unique_ptr<ExpressionEvaluator>> evaluators;
    evaluators.reserve(info->columnDataExprs.size());
    auto exprMapper = ExpressionMapper(&outSchema);
    for (auto& expr : info->columnDataExprs) {
        evaluators.push_back(exprMapper.getEvaluator(expr));
    }
    return std::make_unique<RelInsertExecutor>(storageManager->getRelsStatistics(), table,
        srcNodePos, dstNodePos, std::move(returnVectorsPos), std::move(evaluators));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapInsert(LogicalOperator* logicalOperator) {
    auto& logicalInsert = logicalOperator->constCast<LogicalInsert>();
    auto inSchema = logicalInsert.getChild(0)->getSchema();
    auto outSchema = logicalInsert.getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<NodeInsertExecutor> nodeExecutors;
    std::vector<RelInsertExecutor> relExecutors;
    for (auto& info : logicalInsert.getInfosRef()) {
        switch (info.tableType) {
        case TableType::NODE: {
            nodeExecutors.push_back(getNodeInsertExecutor(&info, *inSchema, *outSchema)->copy());
        } break;
        case TableType::REL: {
            relExecutors.push_back(getRelInsertExecutor(&info, *inSchema, *outSchema)->copy());
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
    auto printInfo = std::make_unique<OPPrintInfo>(logicalInsert.getExpressionsForPrinting());
    return std::make_unique<Insert>(std::move(nodeExecutors), std::move(relExecutors),
        std::move(prevOperator), getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
