#include "planner/logical_plan/copy/logical_copy_from.h"
#include "planner/logical_plan/copy/logical_copy_to.h"
#include "processor/operator/persistent/copy_node.h"
#include "processor/operator/persistent/copy_rel.h"
#include "processor/operator/persistent/copy_to.h"
#include "processor/operator/persistent/reader.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyFrom(LogicalOperator* logicalOperator) {
    auto copy = (LogicalCopyFrom*)logicalOperator;
    auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(copy->getTableID());
    switch (tableSchema->getTableType()) {
    case catalog::TableType::NODE:
        return mapCopyNode(logicalOperator);
    case catalog::TableType::REL:
        return mapCopyRel(logicalOperator);
    default:
        throw NotImplementedException{"PlanMapper::mapCopy"};
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyTo(LogicalOperator* logicalOperator) {
    auto copy = (LogicalCopyTo*)logicalOperator;
    auto childSchema = logicalOperator->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<DataPos> vectorsToCopyPos;
    for (auto& expression : childSchema->getExpressionsInScope()) {
        vectorsToCopyPos.emplace_back(childSchema->getExpressionPos(*expression));
    }
    auto sharedState = std::make_shared<WriteCSVFileSharedState>();
    return std::make_unique<CopyTo>(sharedState, std::move(vectorsToCopyPos),
        copy->getCopyDescription(), getOperatorID(), copy->getExpressionsForPrinting(),
        std::move(prevOperator));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyNode(
    planner::LogicalOperator* logicalOperator) {
    auto copy = (LogicalCopyFrom*)logicalOperator;
    auto fileType = copy->getCopyDescription().fileType;
    if (fileType != CopyDescription::FileType::CSV &&
        fileType != CopyDescription::FileType::PARQUET &&
        fileType != CopyDescription::FileType::NPY) {
        throw NotImplementedException{"PlanMapper::mapLogicalCopyFromToPhysical"};
    }
    auto initReadDataFunc = ReaderFunctions::getInitDataFunc(fileType);
    auto readRowsFunc = ReaderFunctions::getReadRowsFunc(fileType);
    auto nodeTableSchema = catalog->getReadOnlyVersion()->getNodeTableSchema(copy->getTableID());
    auto readerSharedState =
        std::make_shared<ReaderSharedState>(fileType, copy->getCopyDescription().filePaths,
            *copy->getCopyDescription().csvReaderConfig, nodeTableSchema);

    auto outSchema = copy->getSchema();
    auto dataColumnExpressions = copy->getDataColumnExpressions();
    std::vector<DataPos> dataColumnPoses;
    dataColumnPoses.reserve(dataColumnExpressions.size());
    for (auto& dataColumnExpr : dataColumnExpressions) {
        dataColumnPoses.emplace_back(outSchema->getExpressionPos(*dataColumnExpr));
    }
    auto nodeOffsetPos = DataPos(outSchema->getExpressionPos(*copy->getNodeOffsetExpression()));
    auto reader = std::make_unique<Reader>(
        ReaderInfo{nodeOffsetPos, dataColumnPoses, copy->isPreservingOrder(),
            std::move(readRowsFunc), std::move(initReadDataFunc)},
        readerSharedState, getOperatorID(), copy->getExpressionsForPrinting());

    auto copyNodeSharedState =
        std::make_shared<CopyNodeSharedState>(readerSharedState->getNumRowsRef(),
            catalog->getReadOnlyVersion()->getNodeTableSchema(copy->getTableID()),
            storageManager.getNodesStore().getNodeTable(copy->getTableID()),
            copy->getCopyDescription(), memoryManager);
    CopyNodeInfo copyNodeDataInfo{dataColumnPoses, nodeOffsetPos, copy->getCopyDescription(),
        storageManager.getNodesStore().getNodeTable(copy->getTableID()),
        &storageManager.getRelsStore(), catalog, storageManager.getWAL(),
        copy->isPreservingOrder()};
    auto copyNode = std::make_unique<CopyNode>(copyNodeSharedState, copyNodeDataInfo,
        std::make_unique<ResultSetDescriptor>(copy->getSchema()), std::move(reader),
        getOperatorID(), copy->getExpressionsForPrinting());
    auto outputExpressions = binder::expression_vector{copy->getOutputExpression()};
    return createFactorizedTableScanAligned(outputExpressions, outSchema,
        copyNodeSharedState->fTable, DEFAULT_VECTOR_CAPACITY /* maxMorselSize */,
        std::move(copyNode));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyRel(
    planner::LogicalOperator* logicalOperator) {
    auto copy = (LogicalCopyFrom*)logicalOperator;
    auto relsStatistics = &storageManager.getRelsStore().getRelsStatistics();
    auto table = storageManager.getRelsStore().getRelTable(copy->getTableID());
    return std::make_unique<CopyRel>(catalog, copy->getCopyDescription(), table,
        storageManager.getWAL(), relsStatistics, storageManager.getNodesStore(), getOperatorID(),
        copy->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
