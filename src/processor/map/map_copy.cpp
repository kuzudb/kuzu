#include "planner/logical_plan/copy/logical_copy_from.h"
#include "planner/logical_plan/copy/logical_copy_to.h"
#include "processor/operator/copy_from/copy_node.h"
#include "processor/operator/copy_from/copy_rel.h"
#include "processor/operator/copy_from/read_csv.h"
#include "processor/operator/copy_from/read_file.h"
#include "processor/operator/copy_from/read_npy.h"
#include "processor/operator/copy_from/read_parquet.h"
#include "processor/operator/copy_to/copy_to.h"
#include "processor/plan_mapper.h"

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
        throw common::NotImplementedException{"PlanMapper::mapCopy"};
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
    if (fileType != common::CopyDescription::FileType::CSV &&
        fileType != common::CopyDescription::FileType::PARQUET &&
        fileType != common::CopyDescription::FileType::NPY) {
        throw common::NotImplementedException{"PlanMapper::mapLogicalCopyFromToPhysical"};
    }
    std::unique_ptr<ReadFile> readFile;
    std::shared_ptr<ReadFileSharedState> readFileSharedState;
    auto outSchema = copy->getSchema();
    auto dataColumnExpressions = copy->getDataColumnExpressions();
    std::vector<DataPos> dataColumnPoses;
    dataColumnPoses.reserve(dataColumnExpressions.size());
    for (auto& dataColumnExpr : dataColumnExpressions) {
        dataColumnPoses.emplace_back(outSchema->getExpressionPos(*dataColumnExpr));
    }
    auto nodeTableSchema = catalog->getReadOnlyVersion()->getNodeTableSchema(copy->getTableID());
    switch (copy->getCopyDescription().fileType) {
    case (common::CopyDescription::FileType::CSV): {
        readFileSharedState =
            std::make_shared<ReadCSVSharedState>(copy->getCopyDescription().filePaths,
                *copy->getCopyDescription().csvReaderConfig, nodeTableSchema);
        readFile = std::make_unique<ReadCSV>(dataColumnPoses, readFileSharedState, getOperatorID(),
            copy->getExpressionsForPrinting());
    } break;
    case (common::CopyDescription::FileType::PARQUET): {
        readFileSharedState =
            std::make_shared<ReadParquetSharedState>(copy->getCopyDescription().filePaths,
                *copy->getCopyDescription().csvReaderConfig, nodeTableSchema);
        readFile = std::make_unique<ReadParquet>(dataColumnPoses, readFileSharedState,
            getOperatorID(), copy->getExpressionsForPrinting());
    } break;
    case (common::CopyDescription::FileType::NPY): {
        readFileSharedState =
            std::make_shared<ReadNPYSharedState>(copy->getCopyDescription().filePaths,
                *copy->getCopyDescription().csvReaderConfig, nodeTableSchema);
        readFile = std::make_unique<ReadNPY>(dataColumnPoses, readFileSharedState, getOperatorID(),
            copy->getExpressionsForPrinting());
    } break;
    default:
        throw common::NotImplementedException("PlanMapper::mapLogicalCopyNodeToPhysical");
    }
    auto copyNodeSharedState = std::make_shared<CopyNodeSharedState>(readFileSharedState->numRows,
        catalog->getReadOnlyVersion()->getNodeTableSchema(copy->getTableID()),
        storageManager.getNodesStore().getNodeTable(copy->getTableID()), copy->getCopyDescription(),
        memoryManager);
    CopyNodeInfo copyNodeDataInfo{
        dataColumnPoses,
        copy->getCopyDescription(),
        storageManager.getNodesStore().getNodeTable(copy->getTableID()),
        &storageManager.getRelsStore(),
        catalog,
        storageManager.getWAL(),
    };
    auto copyNode = std::make_unique<CopyNode>(copyNodeSharedState, copyNodeDataInfo,
        std::make_unique<ResultSetDescriptor>(copy->getSchema()), std::move(readFile),
        getOperatorID(), copy->getExpressionsForPrinting());
    auto outputExpressions = binder::expression_vector{copy->getOutputExpression()};
    return createFactorizedTableScan(
        outputExpressions, outSchema, copyNodeSharedState->fTable, std::move(copyNode));
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
