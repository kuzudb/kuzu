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
#include <thread>

// TODO(Semih): Remove
#include <iostream>
using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

RJOutputWriter::RJOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs) : context{context}, rjOutputs{rjOutputs} {
    auto mm = context->getMemoryManager();
    srcNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
    dstNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
    srcNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
    srcNodeIDVector->setValue<nodeID_t>(0, rjOutputs->sourceNodeID);
    dstNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
    vectors.push_back(srcNodeIDVector.get());
    vectors.push_back(dstNodeIDVector.get());
}

RJCompState::RJCompState(std::unique_ptr<function::Frontiers> frontiers,
    std::unique_ptr<function::EdgeCompute> edgeCompute,
    std::unique_ptr<RJOutputs> outputs,
    std::unique_ptr<RJOutputWriter> outputWriter)
    : outputs{std::move(outputs)}, frontiers{std::move(frontiers)},
      edgeCompute{std::move(edgeCompute)}, outputWriter{std::move(outputWriter)} {}

void RJAlgorithm::bind(const binder::expression_vector& params, binder::Binder* binder,
    graph::GraphEntry& graphEntry) {
    KU_ASSERT(hasLowerBoundInput ? params.size() == 5 : params.size() == 4);
    auto nodeInput = params[1];
    auto nodeOutput = bindNodeOutput(binder, graphEntry);
    // Note: This is where we ensure that for any recursive join algorithm other than variable
    // length joins, lower bound is 1. See the comment in RJBindData::lowerBound field.
    auto lowerBound = hasLowerBoundInput ? ExpressionUtil::getLiteralValue<int64_t>(*params[2]) : 1;
    auto upperBound = ExpressionUtil::getLiteralValue<int64_t>(*params[hasLowerBoundInput ? 3 : 2]);
    if (lowerBound < 0 || upperBound < 0) {
        // TODO(Semih): Add test case covering this.
        throw RuntimeException("Lower and upper bound lengths of recursive join operations need to be non-negative. Given lower bound is: " +
                               std::to_string(lowerBound) + " and upper bound is: " + std::to_string(upperBound) + ".");
    }
    if (lowerBound > upperBound) {
        throw RuntimeException("Lower bound length of recursive join operations need to be less than or equal to upper bound. Given lower bound is: " +
            std::to_string(lowerBound) + " and upper bound is: " + std::to_string(upperBound) + ".");
    }
    if (upperBound >= RJBindData::DEFAULT_MAXIMUM_ALLOWED_UPPER_BOUND) {
        // TODO(Semih): Add test case covering this.
        throw RuntimeException(
            "Recursive join operations only works for non-positive upper bound iterations that are up to " +
            std::to_string(RJBindData::DEFAULT_MAXIMUM_ALLOWED_UPPER_BOUND) +
            ". Given upper bound is: " + std::to_string(upperBound) + ".");
    }
    if (!hasLowerBoundInput && upperBound == 0) {
        // TODO(Semih): Add test case covering this.
        throw RuntimeException("Shortest path operations only works for positive upper bound "
                               "iterations. Given upper bound is: " +
                               std::to_string(upperBound) + ".");
    }
    auto outputProperty = ExpressionUtil::getLiteralValue<bool>(*params[hasLowerBoundInput ? 4 : 3]);
    bindData = std::make_unique<RJBindData>(nodeInput, nodeOutput, outputProperty, lowerBound, upperBound);
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

class RJOutputWriterVCSharedState {
public:
    RJOutputWriterVCSharedState(storage::MemoryManager* mm, processor::FactorizedTable* globalFT,
        RJOutputWriter* rjOutputWriter)
        : mm{mm}, globalFT{globalFT},  rjOutputWriter{rjOutputWriter} {}

    std::mutex mtx;
    storage::MemoryManager* mm;
    processor::FactorizedTable* globalFT;
    RJOutputWriter* rjOutputWriter;
};

class RJOutputWriterVC : public VertexCompute {
public:
    RJOutputWriterVC(RJOutputWriterVCSharedState* sharedState) : sharedState{sharedState} {
        localFT = std::make_unique<processor::FactorizedTable>(sharedState->mm,
            sharedState->globalFT->getTableSchema()->copy());
        localRJOutputWriter = sharedState->rjOutputWriter->clone();
//        localRJOutputWriter->initWritingFromSource();
    }

    void beginVertexComputeOnTable(table_id_t tableID) override {
        localRJOutputWriter->beginWritingForDstNodesInTable(tableID);
    }

    void vertexCompute(nodeID_t nodeID) override {
        if (localRJOutputWriter->skipWriting(nodeID)) {
            return;
        }
        localRJOutputWriter->write(*localFT, nodeID);
    }

    void finalizeWorkerThread() override {
        std::unique_lock lck(sharedState->mtx);
        sharedState->globalFT->merge(*localFT);
    }

    std::unique_ptr<VertexCompute> clone() override {
        return std::make_unique<RJOutputWriterVC>(sharedState);
    }

private:
    RJOutputWriterVCSharedState* sharedState;
    std::unique_ptr<processor::FactorizedTable> localFT;
    std::unique_ptr<RJOutputWriter> localRJOutputWriter;
};

void SPOutputWriterDsts::write(processor::FactorizedTable& fTable, nodeID_t dstNodeID) const {
    auto length = rjOutputs->ptrCast<SPOutputs>()->pathLengths->getMaskValueFromCurFrontierFixedMask(dstNodeID.offset);
    dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
    writeMoreAndAppend(fTable, dstNodeID, length);
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
                getRJCompState(executionContext, sourceNodeID);
            rjCompState->initRJFromSource(sourceNodeID);
            GDSUtils::runFrontiersUntilConvergence(executionContext, *rjCompState,
                sharedState->graph.get(), bindData->ptrCast<RJBindData>()->upperBound);
//            outputWriter->initWritingFromSource(sourceNodeID);
            auto writerVCSharedState = std::make_unique<RJOutputWriterVCSharedState>(
                executionContext->clientContext->getMemoryManager(), sharedState->fTable.get(),
                rjCompState->outputWriter.get());
            auto writerVC = std::make_unique<RJOutputWriterVC>(writerVCSharedState.get());
            GDSUtils::runVertexComputeIteration(executionContext, sharedState->graph.get(),
                *writerVC);
        }
    }
}

PathLengths::PathLengths(
    std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes,
    storage::MemoryManager* mm) {
    for (const auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
        nodeTableIDAndNumNodesMap[tableID] = numNodes;
        auto memBuffer = mm->allocateBuffer(false, numNodes * sizeof(std::atomic<uint16_t>));
        std::atomic<uint16_t>* memBufferPtr =
            reinterpret_cast<std::atomic<uint16_t>*>(memBuffer.get()->buffer.data());
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
        reinterpret_cast<std::atomic<uint16_t>*>(masks.at(tableID).get()->buffer.data()),
        std::memory_order_relaxed);
    maxNodesInCurFrontierFixedMask.store(
        nodeTableIDAndNumNodesMap[curTableID.load(std::memory_order_relaxed)],
        std::memory_order_relaxed);
}

void PathLengths::fixNextFrontierNodeTable(common::table_id_t tableID) {
    KU_ASSERT(masks.contains(tableID));
    nextFrontierFixedMask.store(
        reinterpret_cast<std::atomic<uint16_t>*>(masks.at(tableID).get()->buffer.data()),
        std::memory_order_relaxed);
}

void PathLengthsFrontiers::beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
    table_id_t nextFrontierTableID) {
    pathLengths->fixCurFrontierNodeTable(curFrontierTableID);
    pathLengths->fixNextFrontierNodeTable(nextFrontierTableID);
    morselizer->init(curFrontierTableID, pathLengths->getNumNodesInCurFrontierFixedNodeTable());
}
// TODO(Semih): Consider removing this function and directly getting morsels from the FrontierMorselizer.
bool PathLengthsFrontiers::getNextRangeMorsel(FrontierMorsel& frontierMorsel) {
    return morselizer->getNextRangeMorsel(frontierMorsel);
}

DoublePathLengthsFrontiers::DoublePathLengthsFrontiers(std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes,
    uint64_t maxThreadsForExec, storage::MemoryManager* mm)
    : Frontiers(move(std::make_shared<PathLengths>(nodeTableIDAndNumNodes, mm)),
          move(std::make_shared<PathLengths>(nodeTableIDAndNumNodes, mm)), 1 /* initial num active nodes */, maxThreadsForExec) {
    morselizer = std::make_unique<FrontierMorselizer>(maxThreadsForExec);
}

bool DoublePathLengthsFrontiers::getNextRangeMorsel(FrontierMorsel& frontierMorsel) {
    return morselizer->getNextRangeMorsel(frontierMorsel);
}

void DoublePathLengthsFrontiers::beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
    table_id_t nextFrontierTableID) {
    curFrontier->ptrCast<PathLengths>()->fixCurFrontierNodeTable(curFrontierTableID);
    nextFrontier->ptrCast<PathLengths>()->fixNextFrontierNodeTable(nextFrontierTableID);
    morselizer->init(curFrontierTableID, curFrontier->ptrCast<PathLengths>()->getNumNodesInCurFrontierFixedNodeTable());
}

RJOutputs::RJOutputs(graph::Graph* graph, nodeID_t sourceNodeID)
    : sourceNodeID{sourceNodeID} {
    for (common::table_id_t tableID : graph->getNodeTableIDs()) {
        auto numNodes = graph->getNumNodes(tableID);
        nodeTableIDAndNumNodes.push_back({tableID, numNodes});
    }
}

SPOutputs::SPOutputs(graph::Graph* graph, nodeID_t sourceNodeID, storage::MemoryManager* mm) : RJOutputs(graph, sourceNodeID) {
    pathLengths = std::make_shared<PathLengths>(nodeTableIDAndNumNodes, mm);
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

void SPOutputWriterDsts::writeMoreAndAppend(processor::FactorizedTable& fTable, nodeID_t, uint16_t) const {
    fTable.append(vectors);
}
}
}