#include "planner/logical_plan/copy/logical_copy_from.h"
#include "processor/operator/index_lookup.h"
#include "processor/operator/persistent/copy_node.h"
#include "processor/operator/persistent/copy_rel.h"
#include "processor/operator/persistent/copy_rel_columns.h"
#include "processor/operator/persistent/copy_rel_lists.h"
#include "processor/operator/persistent/reader.h"
#include "processor/plan_mapper.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyFrom(LogicalOperator* logicalOperator) {
    auto copyFrom = (LogicalCopyFrom*)logicalOperator;
    switch (copyFrom->getInfo()->tableSchema->getTableType()) {
    case TableType::NODE:
        return mapCopyNodeFrom(logicalOperator);
    case TableType::REL:
        return mapCopyRelFrom(logicalOperator);
        // LCOV_EXCL_START
    default:
        throw NotImplementedException{"PlanMapper::mapCopy"};
    }
    // LCOV_EXCL_STOP
}

std::unique_ptr<PhysicalOperator> PlanMapper::createReader(CopyDescription* copyDesc,
    TableSchema* tableSchema, Schema* outSchema,
    const std::vector<std::shared_ptr<Expression>>& dataColumnExpressions,
    const std::shared_ptr<Expression>& offsetExpression, bool containsSerial) {
    auto readerSharedState = std::make_shared<ReaderSharedState>(copyDesc->copy(), tableSchema);
    std::vector<DataPos> dataColumnsPos;
    dataColumnsPos.reserve(dataColumnExpressions.size());
    for (auto& expression : dataColumnExpressions) {
        dataColumnsPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    auto nodeOffsetPos = DataPos(outSchema->getExpressionPos(*offsetExpression));
    auto readInfo = std::make_unique<ReaderInfo>(nodeOffsetPos, dataColumnsPos, containsSerial);
    return std::make_unique<Reader>(
        std::move(readInfo), readerSharedState, getOperatorID(), tableSchema->tableName);
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyNodeFrom(
    planner::LogicalOperator* logicalOperator) {
    auto copyFrom = (LogicalCopyFrom*)logicalOperator;
    auto copyFromInfo = copyFrom->getInfo();
    auto tableSchema = (catalog::NodeTableSchema*)copyFromInfo->tableSchema;
    // Map reader.
    auto reader = createReader(copyFromInfo->copyDesc.get(), copyFromInfo->tableSchema,
        copyFrom->getSchema(), copyFromInfo->columnExpressions, copyFromInfo->offsetExpression,
        copyFromInfo->containsSerial);
    auto readerOp = reinterpret_cast<Reader*>(reader.get());
    auto readerInfo = readerOp->getReaderInfo();
    auto nodeTable = storageManager.getNodesStore().getNodeTable(tableSchema->tableID);
    // Map copy node.
    auto copyNodeSharedState =
        std::make_shared<CopyNodeSharedState>(readerOp->getSharedState()->getNumRowsRef(),
            tableSchema, nodeTable, *copyFromInfo->copyDesc, memoryManager);
    CopyNodeInfo copyNodeDataInfo{readerInfo->dataColumnsPos, *copyFromInfo->copyDesc, nodeTable,
        &storageManager.getRelsStore(), catalog, storageManager.getWAL(),
        copyFromInfo->containsSerial};
    auto copyNode = std::make_unique<CopyNode>(copyNodeSharedState, copyNodeDataInfo,
        std::make_unique<ResultSetDescriptor>(copyFrom->getSchema()), std::move(reader),
        getOperatorID(), copyFrom->getExpressionsForPrinting());
    auto outputExpressions = binder::expression_vector{copyFrom->getOutputExpression()->copy()};
    return createFactorizedTableScanAligned(outputExpressions, copyFrom->getSchema(),
        copyNodeSharedState->fTable, DEFAULT_VECTOR_CAPACITY /* maxMorselSize */,
        std::move(copyNode));
}

std::unique_ptr<PhysicalOperator> PlanMapper::createIndexLookup(RelTableSchema* tableSchema,
    const std::vector<DataPos>& dataPoses, const DataPos& boundOffsetDataPos,
    const DataPos& nbrOffsetDataPos, std::unique_ptr<PhysicalOperator> readerOp) {
    auto boundNodeTableID = tableSchema->getBoundTableID(RelDataDirection::FWD);
    auto boundNodePKIndex =
        storageManager.getNodesStore().getNodeTable(boundNodeTableID)->getPKIndex();
    auto nbrNodeTableID = tableSchema->getNbrTableID(RelDataDirection::FWD);
    auto nbrNodePKIndex = storageManager.getNodesStore().getNodeTable(nbrNodeTableID)->getPKIndex();
    std::vector<std::unique_ptr<IndexLookupInfo>> indexLookupInfos;
    // We assume that dataPoses[0] and dataPoses[1] are REL_FROM and REL_TO columns.
    // TODO: Should be refactored to remove the implicit assumption.
    indexLookupInfos.push_back(
        std::make_unique<IndexLookupInfo>(tableSchema->getSrcPKDataType()->copy(), boundNodePKIndex,
            dataPoses[0], boundOffsetDataPos));
    indexLookupInfos.push_back(std::make_unique<IndexLookupInfo>(
        tableSchema->getDstPKDataType()->copy(), nbrNodePKIndex, dataPoses[1], nbrOffsetDataPos));
    return std::make_unique<IndexLookup>(
        std::move(indexLookupInfos), std::move(readerOp), getOperatorID(), tableSchema->tableName);
}

std::unique_ptr<PhysicalOperator> PlanMapper::createCopyRelColumnsOrLists(
    std::shared_ptr<CopyRelSharedState> sharedState, LogicalCopyFrom* copyFrom, bool isColumns,
    std::unique_ptr<PhysicalOperator> copyRelColumns) {
    auto copyFromInfo = copyFrom->getInfo();
    auto outFSchema = copyFrom->getSchema();
    auto tableSchema = reinterpret_cast<RelTableSchema*>(copyFromInfo->tableSchema);
    auto reader = createReader(copyFromInfo->copyDesc.get(), copyFromInfo->tableSchema, outFSchema,
        copyFromInfo->columnExpressions, copyFromInfo->offsetExpression,
        copyFromInfo->containsSerial);
    auto readerOp = reinterpret_cast<Reader*>(reader.get());
    auto readerInfo = readerOp->getReaderInfo();
    auto offsetDataPos = DataPos{outFSchema->getExpressionPos(*copyFromInfo->offsetExpression)};
    auto boundOffsetDataPos =
        DataPos{outFSchema->getExpressionPos(*copyFromInfo->boundOffsetExpression)};
    auto nbrOffsetDataPos =
        DataPos{outFSchema->getExpressionPos(*copyFromInfo->nbrOffsetExpression)};
    auto indexLookup = createIndexLookup(tableSchema, readerInfo->dataColumnsPos,
        boundOffsetDataPos, nbrOffsetDataPos, std::move(reader));
    CopyRelInfo copyRelInfo{tableSchema, readerInfo->dataColumnsPos, offsetDataPos,
        boundOffsetDataPos, nbrOffsetDataPos, storageManager.getWAL()};
    if (isColumns) {
        return std::make_unique<CopyRelColumns>(copyRelInfo, std::move(sharedState),
            std::make_unique<ResultSetDescriptor>(outFSchema), std::move(indexLookup),
            getOperatorID(), copyFrom->getExpressionsForPrinting());
    } else {
        return std::make_unique<CopyRelLists>(copyRelInfo, std::move(sharedState),
            std::make_unique<ResultSetDescriptor>(outFSchema), std::move(indexLookup),
            std::move(copyRelColumns), getOperatorID(), copyFrom->getExpressionsForPrinting());
    }
}

static std::unique_ptr<DirectedInMemRelData> initializeDirectedInMemRelData(
    common::RelDataDirection direction, RelTableSchema* schema, NodesStore& nodesStore,
    const std::string& outputDirectory, const CopyDescription* copyDescription) {
    auto directedInMemRelData = std::make_unique<DirectedInMemRelData>();
    auto boundTableID = schema->getBoundTableID(direction);
    auto numNodes =
        nodesStore.getNodesStatisticsAndDeletedIDs().getMaxNodeOffsetPerTable().at(boundTableID) +
        1;
    if (schema->isSingleMultiplicityInDirection(direction)) {
        // columns.
        auto relColumns = std::make_unique<DirectedInMemRelColumns>();
        relColumns->adjColumn =
            std::make_unique<InMemColumn>(StorageUtils::getAdjColumnFName(outputDirectory,
                                              schema->tableID, direction, DBFileType::ORIGINAL),
                LogicalType(LogicalTypeID::INTERNAL_ID));
        relColumns->adjColumnChunk =
            relColumns->adjColumn->createInMemColumnChunk(0, numNodes - 1, copyDescription->copy());
        for (auto i = 0u; i < schema->getNumProperties(); ++i) {
            auto propertyID = schema->properties[i]->getPropertyID();
            auto propertyDataType = schema->properties[i]->getDataType();
            auto fName = StorageUtils::getRelPropertyColumnFName(
                outputDirectory, schema->tableID, direction, propertyID, DBFileType::ORIGINAL);
            relColumns->propertyColumns.emplace(
                propertyID, std::make_unique<InMemColumn>(fName, *propertyDataType));
            relColumns->propertyColumnChunks.emplace(
                propertyID, relColumns->propertyColumns.at(propertyID)
                                ->createInMemColumnChunk(0, numNodes - 1, copyDescription->copy()));
        }
        directedInMemRelData->setColumns(std::move(relColumns));
    } else {
        // lists.
        auto relLists = std::make_unique<DirectedInMemRelLists>();
        relLists->adjList =
            std::make_unique<InMemAdjLists>(StorageUtils::getAdjListsFName(outputDirectory,
                                                schema->tableID, direction, DBFileType::ORIGINAL),
                numNodes);
        relLists->relListsSizes = std::make_unique<atomic_uint64_vec_t>(numNodes);
        for (auto i = 0u; i < schema->getNumProperties(); ++i) {
            auto property = schema->getProperty(i);
            auto fName = StorageUtils::getRelPropertyListsFName(outputDirectory, schema->tableID,
                direction, property->getPropertyID(), DBFileType::ORIGINAL);
            relLists->propertyLists.emplace(property->getPropertyID(),
                InMemListsFactory::getInMemPropertyLists(fName, *property->getDataType(), numNodes,
                    copyDescription->copy(), relLists->adjList->getListHeadersBuilder()));
        }
        directedInMemRelData->setRelLists(std::move(relLists));
    }
    return directedInMemRelData;
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyRelFrom(
    planner::LogicalOperator* logicalOperator) {
    auto copyFrom = (LogicalCopyFrom*)logicalOperator;
    auto copyFromInfo = copyFrom->getInfo();
    auto outFSchema = copyFrom->getSchema();
    auto tableSchema = reinterpret_cast<RelTableSchema*>(copyFromInfo->tableSchema);
    auto fwdRelData = initializeDirectedInMemRelData(RelDataDirection::FWD, tableSchema,
        storageManager.getNodesStore(), storageManager.getDirectory(),
        copyFromInfo->copyDesc.get());
    auto bwdRelData = initializeDirectedInMemRelData(RelDataDirection::BWD, tableSchema,
        storageManager.getNodesStore(), storageManager.getDirectory(),
        copyFromInfo->copyDesc.get());
    auto copyRelSharedState = std::make_shared<CopyRelSharedState>(tableSchema->tableID,
        &storageManager.getRelsStore().getRelsStatistics(), std::move(fwdRelData),
        std::move(bwdRelData), memoryManager);

    auto copyRelColumns = createCopyRelColumnsOrLists(
        copyRelSharedState, copyFrom, true /* isColumns */, nullptr /* child */);
    auto copyRelLists = createCopyRelColumnsOrLists(
        copyRelSharedState, copyFrom, false /* isColumns */, std::move(copyRelColumns));
    auto outputExpressions = expression_vector{copyFrom->getOutputExpression()->copy()};
    return createFactorizedTableScanAligned(outputExpressions, outFSchema,
        copyRelSharedState->getFTable(), DEFAULT_VECTOR_CAPACITY /* maxMorselSize */,
        std::move(copyRelLists));
}

} // namespace processor
} // namespace kuzu
