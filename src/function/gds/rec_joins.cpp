#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/exception/runtime.h"
#include "function/gds/gds.h"
#include "function/gds/rec_joins.h"
#include "main/client_context.h"
#include "processor/operator/gds_call.h"
#include "processor/result/factorized_table.h"
#include "storage/buffer_manager/memory_manager.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_utils.h"

// TODO(Semih): Remove
#include <iostream>
using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

RJOutputWriter::RJOutputWriter(main::ClientContext* context) {
    auto mm = context->getMemoryManager();
    srcNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
    dstNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
    srcNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
    dstNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
    vectors.push_back(srcNodeIDVector.get());
    vectors.push_back(dstNodeIDVector.get());
}

// TODO(Semih): Remove
RJCompState::RJCompState(std::unique_ptr<RJOutputs> outputs,
    std::unique_ptr<function::Frontiers> frontiers,
    std::unique_ptr<function::FrontierCompute> frontierCompute)
    : outputs{std::move(outputs)}, frontiers{std::move(frontiers)},
      frontierCompute{std::move(frontierCompute)} {}

void RJAlgorithm::bind(const binder::expression_vector& params, binder::Binder* binder,
    graph::GraphEntry& graphEntry) {
    KU_ASSERT(params.size() == 4);
    auto nodeInput = params[1];
    auto nodeOutput = bindNodeOutput(binder, graphEntry);
    auto upperBound = ExpressionUtil::getLiteralValue<int64_t>(*params[2]);
    if (upperBound < 1 || upperBound >= 255) {
        throw RuntimeException(
            "shortest_paths function only works for positive max iterations that are up to "
            "254. Given upper bound is: " +
            std::to_string(upperBound) + ".");
    }
    auto outputProperty = ExpressionUtil::getLiteralValue<bool>(*params[3]);
    bindData = std::make_unique<RJBindData>(nodeInput, nodeOutput, outputProperty, upperBound);
}

binder::expression_vector RJAlgorithm::getResultColumns(Binder* binder) const {
    binder::expression_vector columns;
    auto& inputNode = bindData->getNodeInput()->constCast<NodeExpression>();
    columns.push_back(inputNode.getInternalID());
    auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
    columns.push_back(outputNode.getInternalID());
    if (RJOutputType::LENGTHS == outputType || RJOutputType::PATHS == outputType) {
        columns.push_back(binder->createVariable(LENGTH_COLUMN_NAME, LogicalType::INT64()));
        if (RJOutputType::PATHS == outputType) {
            columns.push_back(binder->createVariable(PATH_NODE_IDS_COLUMN_NAME,
                LogicalType::LIST(LogicalType::INTERNAL_ID())));
        }
    }
    return columns;
}

void RJAlgorithm::exec(processor::ExecutionContext* executionContext) {
    for (auto& tableID : sharedState->graph->getNodeTableIDs()) {
        if (!sharedState->inputNodeOffsetMasks.contains(tableID)) {
            continue;
        }
        auto mask = sharedState->inputNodeOffsetMasks.at(tableID).get();
        for (auto offset = 0u; offset < sharedState->graph->getNumNodes(tableID); ++offset) {
            if (!mask->isMasked(offset)) {
                continue;
            }
            auto sourceNodeID = nodeID_t{offset, tableID};
            std::unique_ptr<RJCompState> rjCompState =
                getFrontiersAndFrontiersCompute(executionContext, sourceNodeID);
            rjCompState->initRJFromSource(sourceNodeID);
            // Note that spInfo contains an SingleSPOutputs outputs field but it is not explicitly
            // passed to GDSUtils::runFrontiersUntilConvergence below. That is because
            // whatever the frontierCompute function is needed should already be passed
            // to it as a field, so GDSUtils::runFrontiersUntilConvergence does not need
            // to pass outputs field to frontierCompute.
            GDSUtils::runFrontiersUntilConvergence(executionContext, *rjCompState,
                sharedState->graph.get(), bindData->ptrCast<RJBindData>()->upperBound);
            outputWriter->materialize(sharedState->graph.get(), rjCompState->outputs.get(),
                *sharedState->fTable);
        }
    }
}

PathLengths::PathLengths(
    std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes,
    storage::MemoryManager* mm) {
    for (const auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
        nodeTableIDAndNumNodesMap[tableID] = numNodes;
        auto memBuffer = mm->allocateBuffer(false, numNodes * sizeof(std::atomic<uint8_t>));
        std::atomic<uint8_t>* memBufferPtr =
            reinterpret_cast<std::atomic<uint8_t>*>(memBuffer.get()->buffer.data());
        for (uint64_t i = 0; i < numNodes; ++i) {
            memBufferPtr[i].store(UNVISITED, std::memory_order_relaxed);
        }
        masks.insert({tableID, move(memBuffer)});
    }
}

void PathLengths::fixCurFrontierNodeTable(common::table_id_t tableID) {
    KU_ASSERT(masks.contains(tableID));
    curTableID.store(tableID, std::memory_order_relaxed);
    curFrontierFixedMask.store(
        reinterpret_cast<std::atomic<uint8_t>*>(masks.at(tableID).get()->buffer.data()),
        std::memory_order_relaxed);
    maxNodesInCurFrontierFixedMask.store(
        nodeTableIDAndNumNodesMap[curTableID.load(std::memory_order_relaxed)],
        std::memory_order_relaxed);
}

void PathLengths::fixNextFrontierNodeTable(common::table_id_t tableID) {
    KU_ASSERT(masks.contains(tableID));
    nextFrontierFixedMask.store(
        reinterpret_cast<std::atomic<uint8_t>*>(masks.at(tableID).get()->buffer.data()),
        std::memory_order_relaxed);
}

bool PathLengthsFrontiers::getNextFrontierMorsel(RangeFrontierMorsel& frontierMorsel) {
    auto numNodesInFrontier = isDense ? pathLengths->getNumNodesInCurFrontierFixedNodeTable() : sparseOffsetsSize.load(std::memory_order_relaxed);
    auto beginOffset = nextOffset.fetch_add(frontierMorselSize, std::memory_order_acq_rel);
    if (beginOffset >= numNodesInFrontier) {
        return false;
    }
    auto endOffsetExclusive =
        beginOffset + frontierMorselSize > numNodesInFrontier ?
                         numNodesInFrontier : beginOffset + frontierMorselSize;
    frontierMorsel.initMorsel(pathLengths->curTableID, beginOffset, endOffsetExclusive, isDense,
        sparseOffsets.get());
    return true;
}

SPOutputs::SPOutputs(graph::Graph* graph, nodeID_t sourceNodeID, RJOutputType outputType,
    storage::MemoryManager* mm) : RJOutputs(sourceNodeID, outputType) {
    for (common::table_id_t tableID : graph->getNodeTableIDs()) {
        auto numNodes = graph->getNumNodes(tableID);
        nodeTableIDAndNumNodes.push_back({tableID, numNodes});
    }
    pathLengths = std::make_unique<PathLengths>(nodeTableIDAndNumNodes, mm);
}

void PathVectorWriter::beginWritingNewPath(uint64_t length) {
    nextPathPos = length > 1 ? length - 1 : 0;
    pathNodeIDsVector->resetAuxiliaryBuffer();
    curPathListEntry = ListVector::addList(pathNodeIDsVector, nextPathPos);
    pathNodeIDsVector->setValue(0, curPathListEntry);
}

void PathVectorWriter::addNewNodeID(nodeID_t curIntNode) {
    ListVector::getDataVector(pathNodeIDsVector)
        ->setValue(curPathListEntry.offset + nextPathPos - 1, curIntNode);
    nextPathPos--;
}

void SPOutputWriterDsts::materialize(
    graph::Graph* graph, RJOutputs* rjOutputs, processor::FactorizedTable& fTable) const {
    auto spOutputs = rjOutputs->ptrCast<SPOutputs>();
    srcNodeIDVector->setValue<nodeID_t>(0, rjOutputs->sourceNodeID);
    for (auto tableID : graph->getNodeTableIDs()) {
        spOutputs->pathLengths->fixCurFrontierNodeTable(tableID);
        fixOtherStructuresToOutputDstsFromNodeTable(rjOutputs, tableID);
        for (offset_t dstNodeOffset = 0;  dstNodeOffset < 5;
             // dstNodeOffset < spOutputs->pathLengths->getNumNodesInCurFrontierFixedNodeTable();
             ++dstNodeOffset) {
            auto length =
                spOutputs->pathLengths->getMaskValueFromCurFrontierFixedMask(dstNodeOffset);
            if (length == PathLengths::UNVISITED) {
                continue;
            }
            auto dstNodeID = nodeID_t{dstNodeOffset, tableID};
            dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
            writeMoreAndAppend(fTable, rjOutputs, dstNodeID, length);
        }
    }
}

void SPOutputWriterDsts::writeMoreAndAppend(
    processor::FactorizedTable& fTable, RJOutputs*, nodeID_t, uint8_t) const {
    fTable.append(vectors);
}
}
}