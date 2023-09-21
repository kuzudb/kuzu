#include "binder/copy/bound_copy_from.h"
#include "planner/operator/persistent/logical_copy_from.h"
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

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyNodeFrom(
    planner::LogicalOperator* logicalOperator) {
    auto copyFrom = (LogicalCopyFrom*)logicalOperator;
    auto copyFromInfo = copyFrom->getInfo();
    auto readerConfig = copyFromInfo->fileScanInfo->readerConfig.get();
    auto tableSchema = (catalog::NodeTableSchema*)copyFromInfo->tableSchema;
    // Map reader.
    auto prevOperator = mapOperator(copyFrom->getChild(0).get());
    auto reader = reinterpret_cast<Reader*>(prevOperator.get());
    auto readerInfo = reader->getReaderInfo();
    // Map copy node.
    auto nodeTable = storageManager.getNodesStore().getNodeTable(tableSchema->tableID);
    bool isCopyRdf = readerConfig->fileType == FileType::TURTLE;
    auto copyNodeSharedState = std::make_shared<CopyNodeSharedState>(
        reader->getSharedState()->getNumRowsRef(), tableSchema, nodeTable, memoryManager, isCopyRdf,
        readerConfig->csvReaderConfig->copy());
    CopyNodeInfo copyNodeDataInfo{readerInfo->dataColumnsPos, nodeTable,
        &storageManager.getRelsStore(), catalog, storageManager.getWAL(),
        copyFromInfo->containsSerial};
    auto copyNode = std::make_unique<CopyNode>(copyNodeSharedState, copyNodeDataInfo,
        std::make_unique<ResultSetDescriptor>(copyFrom->getSchema()), std::move(prevOperator),
        getOperatorID(), copyFrom->getExpressionsForPrinting());
    auto outputExpressions = binder::expression_vector{copyFrom->getOutputExpression()->copy()};
    return createFactorizedTableScanAligned(outputExpressions, copyFrom->getSchema(),
        copyNodeSharedState->fTable, DEFAULT_VECTOR_CAPACITY /* maxMorselSize */,
        std::move(copyNode));
}

std::unique_ptr<PhysicalOperator> PlanMapper::createCopyRelColumnsOrLists(
    std::shared_ptr<CopyRelSharedState> sharedState, LogicalCopyFrom* copyFrom, bool isColumns,
    std::unique_ptr<PhysicalOperator> copyRelColumns) {
    auto copyFromInfo = copyFrom->getInfo();
    auto outFSchema = copyFrom->getSchema();
    auto prevOperator = mapOperator(copyFrom->getChild(0).get());
    assert(prevOperator->getChild(0)->getOperatorType() == PhysicalOperatorType::READER);
    auto reader = (Reader*)prevOperator->getChild(0);
    auto tableSchema = reinterpret_cast<RelTableSchema*>(copyFromInfo->tableSchema);
    auto offsetDataPos = DataPos{outFSchema->getExpressionPos(*copyFromInfo->fileScanInfo->offset)};
    DataPos srcOffsetPos;
    DataPos dstOffsetPos;
    std::vector<DataPos> dataColumnPositions;
    if (copyFromInfo->fileScanInfo->readerConfig->fileType == FileType::TURTLE) {
        auto extraInfo = reinterpret_cast<ExtraBoundCopyRdfRelInfo*>(copyFromInfo->extraInfo.get());
        srcOffsetPos = DataPos{outFSchema->getExpressionPos(*extraInfo->subjectOffset)};
        dstOffsetPos = DataPos{outFSchema->getExpressionPos(*extraInfo->objectOffset)};
        dataColumnPositions.emplace_back(outFSchema->getExpressionPos(*extraInfo->predicateOffset));
    } else {
        auto extraInfo = reinterpret_cast<ExtraBoundCopyRelInfo*>(copyFromInfo->extraInfo.get());
        srcOffsetPos = DataPos{outFSchema->getExpressionPos(*extraInfo->srcOffset)};
        dstOffsetPos = DataPos{outFSchema->getExpressionPos(*extraInfo->dstOffset)};
        dataColumnPositions = reader->getReaderInfo()->dataColumnsPos;
    }
    CopyRelInfo copyRelInfo{tableSchema, dataColumnPositions, offsetDataPos, srcOffsetPos,
        dstOffsetPos, storageManager.getWAL(), copyFromInfo->containsSerial};
    if (isColumns) {
        return std::make_unique<CopyRelColumns>(copyRelInfo, std::move(sharedState),
            std::make_unique<ResultSetDescriptor>(outFSchema), std::move(prevOperator),
            getOperatorID(), copyFrom->getExpressionsForPrinting());
    } else {
        return std::make_unique<CopyRelLists>(copyRelInfo, std::move(sharedState),
            std::make_unique<ResultSetDescriptor>(outFSchema), std::move(prevOperator),
            std::move(copyRelColumns), getOperatorID(), copyFrom->getExpressionsForPrinting());
    }
}

static std::unique_ptr<DirectedInMemRelData> initializeDirectedInMemRelData(
    common::RelDataDirection direction, RelTableSchema* schema, NodesStore& nodesStore,
    const std::string& outputDirectory, CSVReaderConfig* csvReaderConfig) {
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
            relColumns->adjColumn->createInMemColumnChunk(0, numNodes - 1, csvReaderConfig);
        for (auto i = 0u; i < schema->getNumProperties(); ++i) {
            auto propertyID = schema->properties[i]->getPropertyID();
            auto propertyDataType = schema->properties[i]->getDataType();
            auto fName = StorageUtils::getRelPropertyColumnFName(
                outputDirectory, schema->tableID, direction, propertyID, DBFileType::ORIGINAL);
            relColumns->propertyColumns.emplace(
                propertyID, std::make_unique<InMemColumn>(fName, *propertyDataType));
            relColumns->propertyColumnChunks.emplace(
                propertyID, relColumns->propertyColumns.at(propertyID)
                                ->createInMemColumnChunk(0, numNodes - 1, csvReaderConfig));
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
                    csvReaderConfig, relLists->adjList->getListHeadersBuilder()));
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
        copyFromInfo->fileScanInfo->readerConfig->csvReaderConfig.get());
    auto bwdRelData = initializeDirectedInMemRelData(RelDataDirection::BWD, tableSchema,
        storageManager.getNodesStore(), storageManager.getDirectory(),
        copyFromInfo->fileScanInfo->readerConfig->csvReaderConfig.get());
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
