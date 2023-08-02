#include "planner/logical_plan/logical_operator/logical_copy.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/copy/copy_node.h"
#include "processor/operator/copy/copy_rel.h"
#include "processor/operator/copy/read_csv.h"
#include "processor/operator/copy/read_file.h"
#include "processor/operator/copy/read_npy.h"
#include "processor/operator/copy/read_parquet.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopy(LogicalOperator* logicalOperator) {
    auto copy = (LogicalCopy*)logicalOperator;
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

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyNode(
    planner::LogicalOperator* logicalOperator) {
    auto copy = (LogicalCopy*)logicalOperator;
    auto fileType = copy->getCopyDescription().fileType;
    if (fileType != common::CopyDescription::FileType::CSV &&
        fileType != common::CopyDescription::FileType::PARQUET &&
        fileType != common::CopyDescription::FileType::NPY) {
        throw common::NotImplementedException{"PlanMapper::mapLogicalCopyToPhysical"};
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
    auto nodeOffsetPos = DataPos(outSchema->getExpressionPos(*copy->getNodeOffsetExpression()));
    auto nodeTableSchema = catalog->getReadOnlyVersion()->getNodeTableSchema(copy->getTableID());
    switch (copy->getCopyDescription().fileType) {
    case (common::CopyDescription::FileType::CSV): {
        readFileSharedState =
            std::make_shared<ReadCSVSharedState>(copy->getCopyDescription().filePaths,
                *copy->getCopyDescription().csvReaderConfig, nodeTableSchema);
        readFile = std::make_unique<ReadCSV>(nodeOffsetPos, dataColumnPoses, readFileSharedState,
            getOperatorID(), copy->getExpressionsForPrinting());
    } break;
    case (common::CopyDescription::FileType::PARQUET): {
        readFileSharedState =
            std::make_shared<ReadParquetSharedState>(copy->getCopyDescription().filePaths,
                *copy->getCopyDescription().csvReaderConfig, nodeTableSchema);
        readFile = std::make_unique<ReadParquet>(nodeOffsetPos, dataColumnPoses,
            readFileSharedState, getOperatorID(), copy->getExpressionsForPrinting());
    } break;
    case (common::CopyDescription::FileType::NPY): {
        readFileSharedState =
            std::make_shared<ReadNPYSharedState>(copy->getCopyDescription().filePaths,
                *copy->getCopyDescription().csvReaderConfig, nodeTableSchema);
        readFile = std::make_unique<ReadNPY>(nodeOffsetPos, dataColumnPoses, readFileSharedState,
            getOperatorID(), copy->getExpressionsForPrinting());
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
        nodeOffsetPos,
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
    auto copy = (LogicalCopy*)logicalOperator;
    auto relsStatistics = &storageManager.getRelsStore().getRelsStatistics();
    auto table = storageManager.getRelsStore().getRelTable(copy->getTableID());
    return std::make_unique<CopyRel>(catalog, copy->getCopyDescription(), table,
        storageManager.getWAL(), relsStatistics, storageManager.getNodesStore(), getOperatorID(),
        copy->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
