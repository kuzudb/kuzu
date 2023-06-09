#include "planner/logical_plan/logical_operator/logical_add_property.h"
#include "planner/logical_plan/logical_operator/logical_copy.h"
#include "planner/logical_plan/logical_operator/logical_create_node_table.h"
#include "planner/logical_plan/logical_operator/logical_create_rel_table.h"
#include "planner/logical_plan/logical_operator/logical_drop_property.h"
#include "planner/logical_plan/logical_operator/logical_drop_table.h"
#include "planner/logical_plan/logical_operator/logical_rename_property.h"
#include "planner/logical_plan/logical_operator/logical_rename_table.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/copy/copy_node.h"
#include "processor/operator/copy/copy_npy.h"
#include "processor/operator/copy/copy_rel.h"
#include "processor/operator/copy/read_csv.h"
#include "processor/operator/copy/read_parquet.h"
#include "processor/operator/ddl/add_node_property.h"
#include "processor/operator/ddl/add_rel_property.h"
#include "processor/operator/ddl/create_node_table.h"
#include "processor/operator/ddl/create_rel_table.h"
#include "processor/operator/ddl/drop_property.h"
#include "processor/operator/ddl/drop_table.h"
#include "processor/operator/ddl/rename_property.h"
#include "processor/operator/ddl/rename_table.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static DataPos getOutputPos(LogicalDDL* logicalDDL) {
    auto outSchema = logicalDDL->getSchema();
    auto outputExpression = logicalDDL->getOutputExpression();
    return DataPos(outSchema->getExpressionPos(*outputExpression));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateNodeTableToPhysical(
    LogicalOperator* logicalOperator) {
    auto createNodeTable = (LogicalCreateNodeTable*)logicalOperator;
    return std::make_unique<CreateNodeTable>(catalog, createNodeTable->getTableName(),
        createNodeTable->getPropertyNameDataTypes(), createNodeTable->getPrimaryKeyIdx(),
        getOutputPos(createNodeTable), getOperatorID(),
        createNodeTable->getExpressionsForPrinting(),
        &storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateRelTableToPhysical(
    LogicalOperator* logicalOperator) {
    auto createRelTable = (LogicalCreateRelTable*)logicalOperator;
    return std::make_unique<CreateRelTable>(catalog, createRelTable->getTableName(),
        createRelTable->getPropertyNameDataTypes(), createRelTable->getRelMultiplicity(),
        createRelTable->getSrcTableID(), createRelTable->getDstTableID(),
        getOutputPos(createRelTable), getOperatorID(), createRelTable->getExpressionsForPrinting(),
        &storageManager.getRelsStore().getRelsStatistics());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCopyToPhysical(
    LogicalOperator* logicalOperator) {
    auto copy = (LogicalCopy*)logicalOperator;
    auto tableName = catalog->getReadOnlyVersion()->getTableName(copy->getTableID());
    if (catalog->getReadOnlyVersion()->containNodeTable(tableName)) {
        return mapLogicalCopyNodeToPhysical(copy);
    } else {
        return mapLogicalCopyRelToPhysical(copy);
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCopyNodeToPhysical(LogicalCopy* copy) {
    switch (copy->getCopyDescription().fileType) {
    case common::CopyDescription::FileType::CSV:
    case common::CopyDescription::FileType::PARQUET: {
        std::unique_ptr<ReadFile> readFile;
        std::shared_ptr<ReadFileSharedState> readFileSharedState;
        auto outSchema = copy->getSchema();
        auto arrowColumnExpressions = copy->getArrowColumnExpressions();
        std::vector<DataPos> arrowColumnPoses;
        arrowColumnPoses.reserve(arrowColumnExpressions.size());
        for (auto& arrowColumnPos : arrowColumnExpressions) {
            arrowColumnPoses.emplace_back(outSchema->getExpressionPos(*arrowColumnPos));
        }
        auto offsetExpression = copy->getOffsetExpression();
        auto offsetVectorPos = DataPos(outSchema->getExpressionPos(*offsetExpression));
        if (copy->getCopyDescription().fileType == common::CopyDescription::FileType::CSV) {
            readFileSharedState =
                std::make_shared<ReadCSVSharedState>(*copy->getCopyDescription().csvReaderConfig,
                    catalog->getReadOnlyVersion()->getNodeTableSchema(copy->getTableID()),
                    copy->getCopyDescription().filePaths);
            readFile = std::make_unique<ReadCSV>(arrowColumnPoses, offsetVectorPos,
                readFileSharedState, getOperatorID(), copy->getExpressionsForPrinting());
        } else {
            readFileSharedState =
                std::make_shared<ReadParquetSharedState>(copy->getCopyDescription().filePaths);
            readFile = std::make_unique<ReadParquet>(arrowColumnPoses, offsetVectorPos,
                readFileSharedState, getOperatorID(), copy->getExpressionsForPrinting());
        }
        auto copyNodeSharedState =
            std::make_shared<CopyNodeSharedState>(readFileSharedState->numRows, memoryManager);
        auto localState = std::make_unique<CopyNodeLocalState>(copy->getCopyDescription(),
            storageManager.getNodesStore().getNodeTable(copy->getTableID()),
            &storageManager.getRelsStore(), catalog, storageManager.getWAL(), offsetVectorPos,
            arrowColumnPoses);
        auto copyNode = std::make_unique<CopyNode>(std::move(localState), copyNodeSharedState,
            std::make_unique<ResultSetDescriptor>(copy->getSchema()), std::move(readFile),
            getOperatorID(), copy->getExpressionsForPrinting());
        // We need to create another pipeline to return the copy message to the user.
        // The new pipeline only contains a factorizedTableScan and a resultCollector.
        auto outputExpression = copy->getOutputExpression();
        auto outputVectorPos = DataPos(outSchema->getExpressionPos(*outputExpression));
        auto ftSharedState = std::make_shared<FTableSharedState>(
            copyNodeSharedState->table, common::DEFAULT_VECTOR_CAPACITY);
        return std::make_unique<FactorizedTableScan>(std::vector<DataPos>{outputVectorPos},
            std::vector<uint32_t>{0} /* colIndicesToScan */, ftSharedState, std::move(copyNode),
            getOperatorID(), copy->getExpressionsForPrinting());
    }
    case common::CopyDescription::FileType::NPY: {
        auto table = storageManager.getNodesStore().getNodeTable(copy->getTableID());
        return std::make_unique<CopyNPY>(catalog, copy->getCopyDescription(), table,
            storageManager.getWAL(), table->getNodeStatisticsAndDeletedIDs(),
            storageManager.getRelsStore(), getOperatorID(), copy->getExpressionsForPrinting());
    }
    default:
        throw common::NotImplementedException{"PlanMapper::mapLogicalCopyToPhysical"};
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCopyRelToPhysical(LogicalCopy* copy) {
    auto relsStatistics = &storageManager.getRelsStore().getRelsStatistics();
    auto table = storageManager.getRelsStore().getRelTable(copy->getTableID());
    return std::make_unique<CopyRel>(catalog, copy->getCopyDescription(), table,
        storageManager.getWAL(), relsStatistics, storageManager.getNodesStore(), getOperatorID(),
        copy->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDropTableToPhysical(
    LogicalOperator* logicalOperator) {
    auto dropTable = (LogicalDropTable*)logicalOperator;
    return std::make_unique<DropTable>(catalog, dropTable->getTableID(), getOutputPos(dropTable),
        getOperatorID(), dropTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalRenameTableToPhysical(
    LogicalOperator* logicalOperator) {
    auto renameTable = (LogicalRenameTable*)logicalOperator;
    return std::make_unique<RenameTable>(catalog, renameTable->getTableID(),
        renameTable->getNewName(), getOutputPos(renameTable), getOperatorID(),
        renameTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalAddPropertyToPhysical(
    LogicalOperator* logicalOperator) {
    auto addProperty = (LogicalAddProperty*)logicalOperator;
    auto expressionEvaluator =
        expressionMapper.mapExpression(addProperty->getDefaultValue(), *addProperty->getSchema());
    if (catalog->getReadOnlyVersion()->containNodeTable(addProperty->getTableID())) {
        return std::make_unique<AddNodeProperty>(catalog, addProperty->getTableID(),
            addProperty->getPropertyName(), addProperty->getDataType(),
            std::move(expressionEvaluator), storageManager, getOutputPos(addProperty),
            getOperatorID(), addProperty->getExpressionsForPrinting());
    } else {
        return std::make_unique<AddRelProperty>(catalog, addProperty->getTableID(),
            addProperty->getPropertyName(), addProperty->getDataType(),
            std::move(expressionEvaluator), storageManager, getOutputPos(addProperty),
            getOperatorID(), addProperty->getExpressionsForPrinting());
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDropPropertyToPhysical(
    LogicalOperator* logicalOperator) {
    auto dropProperty = (LogicalDropProperty*)logicalOperator;
    return std::make_unique<DropProperty>(catalog, dropProperty->getTableID(),
        dropProperty->getPropertyID(), getOutputPos(dropProperty), getOperatorID(),
        dropProperty->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalRenamePropertyToPhysical(
    LogicalOperator* logicalOperator) {
    auto renameProperty = (LogicalRenameProperty*)logicalOperator;
    return std::make_unique<RenameProperty>(catalog, renameProperty->getTableID(),
        renameProperty->getPropertyID(), renameProperty->getNewName(), getOutputPos(renameProperty),
        getOperatorID(), renameProperty->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
