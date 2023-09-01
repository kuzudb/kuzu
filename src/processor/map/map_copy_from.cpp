#include "planner/logical_plan/copy/logical_copy_from.h"
#include "processor/operator/persistent/copy_node.h"
#include "processor/operator/persistent/copy_rel.h"
#include "processor/operator/persistent/reader.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyFrom(LogicalOperator* logicalOperator) {
    auto copyFrom = (LogicalCopyFrom*)logicalOperator;
    auto tableSchema = copyFrom->getTableSchema();
    switch (tableSchema->getTableType()) {
    case catalog::TableType::NODE:
        return mapCopyNode(logicalOperator);
    case catalog::TableType::REL:
        return mapCopyRel(logicalOperator);
    default:
        throw NotImplementedException{"PlanMapper::mapCopy"};
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyNode(
    planner::LogicalOperator* logicalOperator) {
    auto copyFrom = (LogicalCopyFrom*)logicalOperator;
    auto fileType = copyFrom->getCopyDescription()->fileType;
    assert(fileType == CopyDescription::FileType::CSV ||
           fileType == CopyDescription::FileType::PARQUET ||
           fileType == CopyDescription::FileType::NPY);
    auto tableSchema = (catalog::NodeTableSchema*)copyFrom->getTableSchema();
    auto copyDesc = copyFrom->getCopyDescription();
    // Map reader.
    auto readerSharedState = std::make_shared<ReaderSharedState>(copyDesc->copy(), tableSchema);
    auto outSchema = copyFrom->getSchema();
    std::vector<DataPos> dataColumnsPos;
    for (auto& expression : copyFrom->getColumnExpressions()) {
        dataColumnsPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    auto nodeOffsetPos = DataPos(outSchema->getExpressionPos(*copyFrom->getNodeOffsetExpression()));
    auto readInfo =
        std::make_unique<ReaderInfo>(nodeOffsetPos, dataColumnsPos, copyFrom->orderPreserving());
    auto reader = std::make_unique<Reader>(std::move(readInfo), readerSharedState, getOperatorID(),
        copyFrom->getExpressionsForPrinting());
    // Map copy CSV.
    auto nodeTable = storageManager.getNodesStore().getNodeTable(tableSchema->tableID);
    auto copyNodeSharedState = std::make_shared<CopyNodeSharedState>(
        readerSharedState->getNumRowsRef(), tableSchema, nodeTable, *copyDesc, memoryManager);
    CopyNodeInfo copyNodeDataInfo{dataColumnsPos, nodeOffsetPos, *copyDesc, nodeTable,
        &storageManager.getRelsStore(), catalog, storageManager.getWAL(),
        copyFrom->orderPreserving()};
    auto copyNode = std::make_unique<CopyNode>(copyNodeSharedState, copyNodeDataInfo,
        std::make_unique<ResultSetDescriptor>(copyFrom->getSchema()), std::move(reader),
        getOperatorID(), copyFrom->getExpressionsForPrinting());
    auto outputExpressions = binder::expression_vector{copyFrom->getOutputExpression()};
    return createFactorizedTableScanAligned(outputExpressions, outSchema,
        copyNodeSharedState->fTable, DEFAULT_VECTOR_CAPACITY /* maxMorselSize */,
        std::move(copyNode));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyRel(
    planner::LogicalOperator* logicalOperator) {
    auto copy = (LogicalCopyFrom*)logicalOperator;
    auto relsStatistics = &storageManager.getRelsStore().getRelsStatistics();
    auto table = storageManager.getRelsStore().getRelTable(copy->getTableSchema()->tableID);
    return std::make_unique<CopyRel>(catalog, *copy->getCopyDescription(), table,
        storageManager.getWAL(), relsStatistics, storageManager.getNodesStore(), getOperatorID(),
        copy->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
