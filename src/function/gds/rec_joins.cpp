#include "function/gds/rec_joins.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/exception/runtime.h"
#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_utils.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"
#include "storage/buffer_manager/memory_manager.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

RJOutputWriter::RJOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs)
    : context{context}, rjOutputs{rjOutputs} {
    auto mm = context->getMemoryManager();
    srcNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
    dstNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
    srcNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
    srcNodeIDVector->setValue<nodeID_t>(0, rjOutputs->sourceNodeID);
    dstNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
    vectors.push_back(srcNodeIDVector.get());
    vectors.push_back(dstNodeIDVector.get());
}

RJCompState::RJCompState(std::unique_ptr<function::FrontierPair> frontierPair,
    std::unique_ptr<function::EdgeCompute> edgeCompute, std::unique_ptr<RJOutputs> outputs,
    std::unique_ptr<RJOutputWriter> outputWriter)
    : frontierPair{std::move(frontierPair)}, edgeCompute{std::move(edgeCompute)},
      outputs{std::move(outputs)}, outputWriter{std::move(outputWriter)} {}

void RJAlgorithm::validateLowerUpperBound(int64_t lowerBound, int64_t upperBound) {
    if (lowerBound < 0 || upperBound < 0) {
        throw RuntimeException(
            stringFormat("Lower and upper bound lengths of recursive join operations need to be "
                         "non-negative. Given lower bound is: {} and upper bound is: {}.",
                lowerBound, upperBound));
    }
    if (lowerBound > upperBound) {
        throw RuntimeException(
            stringFormat("Lower bound length of recursive join operations need to be less than or "
                         "equal to upper bound. Given lower bound is: {} and upper bound is: {}.",
                lowerBound, upperBound));
    }
    if (upperBound >= RJBindData::DEFAULT_MAXIMUM_ALLOWED_UPPER_BOUND) {
        throw RuntimeException(
            stringFormat("Recursive join operations only works for non-positive upper bound "
                         "iterations that are up to {}. Given upper bound is: {}.",
                RJBindData::DEFAULT_MAXIMUM_ALLOWED_UPPER_BOUND, upperBound));
    }
}

expression_vector RJAlgorithm::getNodeIDResultColumns() const {
    expression_vector columns;
    auto& inputNode = bindData->getNodeInput()->constCast<NodeExpression>();
    columns.push_back(inputNode.getInternalID());
    auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
    columns.push_back(outputNode.getInternalID());
    return columns;
}

std::shared_ptr<binder::Expression> RJAlgorithm::getLengthColumn(Binder* binder) const {
    return binder->createVariable(LENGTH_COLUMN_NAME, LogicalType::INT64());
}

std::shared_ptr<binder::Expression> RJAlgorithm::getPathNodeIDsColumn(Binder* binder) const {
    return binder->createVariable(PATH_NODE_IDS_COLUMN_NAME,
        LogicalType::LIST(LogicalType::INTERNAL_ID()));
}

static void validateSPUpperBound(int64_t upperBound) {
    if (upperBound == 0) {
        throw RuntimeException(stringFormat("Shortest path operations only works for positive "
                                            "upper bound iterations. Given upper bound is: {}.",
            upperBound));
    }
}

void SPAlgorithm::bind(const expression_vector& params, Binder* binder,
    graph::GraphEntry& graphEntry) {
    KU_ASSERT(params.size() == 4);
    auto nodeInput = params[1];
    auto nodeOutput = bindNodeOutput(binder, graphEntry);
    auto lowerBound = 1;
    auto upperBound = ExpressionUtil::getLiteralValue<int64_t>(*params[2]);
    validateSPUpperBound(upperBound);
    validateLowerUpperBound(lowerBound, upperBound);
    auto outputProperty = ExpressionUtil::getLiteralValue<bool>(*params[3]);
    bindData =
        std::make_unique<RJBindData>(nodeInput, nodeOutput, outputProperty, lowerBound, upperBound);
}

class RJOutputWriterVCSharedState {
public:
    RJOutputWriterVCSharedState(storage::MemoryManager* mm, processor::FactorizedTable* globalFT,
        RJOutputWriter* rjOutputWriter)
        : mm{mm}, globalFT{globalFT}, rjOutputWriter{rjOutputWriter} {}

    std::mutex mtx;
    storage::MemoryManager* mm;
    processor::FactorizedTable* globalFT;
    RJOutputWriter* rjOutputWriter;
};

class RJOutputWriterVC : public VertexCompute {
public:
    explicit RJOutputWriterVC(RJOutputWriterVCSharedState* sharedState) : sharedState{sharedState} {
        localFT = std::make_unique<processor::FactorizedTable>(sharedState->mm,
            sharedState->globalFT->getTableSchema()->copy());
        localRJOutputWriter = sharedState->rjOutputWriter->copy();
    }

    void beginOnTable(table_id_t tableID) override {
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

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<RJOutputWriterVC>(sharedState);
    }

private:
    RJOutputWriterVCSharedState* sharedState;
    std::unique_ptr<processor::FactorizedTable> localFT;
    std::unique_ptr<RJOutputWriter> localRJOutputWriter;
};

void SPOutputWriterDsts::write(processor::FactorizedTable& fTable, nodeID_t dstNodeID) const {
    auto length =
        rjOutputs->ptrCast<SPOutputs>()->pathLengths->getMaskValueFromCurFrontierFixedMask(
            dstNodeID.offset);
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
            if (!mask->isMasked(offset, offset)) {
                continue;
            }
            auto sourceNodeID = nodeID_t{offset, tableID};
            RJCompState rjCompState = getRJCompState(executionContext, sourceNodeID);
            rjCompState.initRJFromSource(sourceNodeID);
            GDSUtils::runFrontiersUntilConvergence(executionContext, rjCompState,
                sharedState->graph.get(), bindData->ptrCast<RJBindData>()->upperBound);
            auto writerVCSharedState = std::make_unique<RJOutputWriterVCSharedState>(
                executionContext->clientContext->getMemoryManager(), sharedState->fTable.get(),
                rjCompState.outputWriter.get());
            auto writerVC = std::make_unique<RJOutputWriterVC>(writerVCSharedState.get());
            GDSUtils::runVertexComputeIteration(executionContext, sharedState->graph.get(),
                *writerVC);
        }
    }
}

PathLengths::PathLengths(std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
    storage::MemoryManager* mm) {
    curIter.store(0);
    for (const auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
        nodeTableIDAndNumNodesMap[tableID] = numNodes;
        auto memBuffer = mm->allocateBuffer(false, numNodes * sizeof(std::atomic<uint16_t>));
        std::atomic<uint16_t>* memBufferPtr =
            reinterpret_cast<std::atomic<uint16_t>*>(memBuffer.get()->buffer.data());
        for (uint64_t i = 0; i < numNodes; ++i) {
            memBufferPtr[i].store(UNVISITED, std::memory_order_relaxed);
        }
        masks.insert({tableID, std::move(memBuffer)});
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

void SinglePathLengthsFrontierPair::beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
    table_id_t nextFrontierTableID) {
    pathLengths->fixCurFrontierNodeTable(curFrontierTableID);
    pathLengths->fixNextFrontierNodeTable(nextFrontierTableID);
    morselDispatcher.init(curFrontierTableID,
        pathLengths->getNumNodesInCurFrontierFixedNodeTable());
}

bool SinglePathLengthsFrontierPair::getNextRangeMorsel(FrontierMorsel& frontierMorsel) {
    return morselDispatcher.getNextRangeMorsel(frontierMorsel);
}

void SinglePathLengthsFrontierPair::initRJFromSource(nodeID_t source) {
    pathLengths->fixNextFrontierNodeTable(source.tableID);
    pathLengths->setActive(source);
}

DoublePathLengthsFrontierPair::DoublePathLengthsFrontierPair(
    std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
    uint64_t maxThreadsForExec, storage::MemoryManager* mm)
    : FrontierPair(std::make_shared<PathLengths>(nodeTableIDAndNumNodes, mm),
          std::make_shared<PathLengths>(nodeTableIDAndNumNodes, mm),
          1 /* initial num active nodes */, maxThreadsForExec) {
    morselDispatcher = std::make_unique<FrontierMorselDispatcher>(maxThreadsForExec);
}

bool DoublePathLengthsFrontierPair::getNextRangeMorsel(FrontierMorsel& frontierMorsel) {
    return morselDispatcher->getNextRangeMorsel(frontierMorsel);
}

void DoublePathLengthsFrontierPair::beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
    table_id_t nextFrontierTableID) {
    curFrontier->ptrCast<PathLengths>()->fixCurFrontierNodeTable(curFrontierTableID);
    nextFrontier->ptrCast<PathLengths>()->fixNextFrontierNodeTable(nextFrontierTableID);
    morselDispatcher->init(curFrontierTableID,
        curFrontier->ptrCast<PathLengths>()->getNumNodesInCurFrontierFixedNodeTable());
}

void DoublePathLengthsFrontierPair::initRJFromSource(nodeID_t source) {
    nextFrontier->ptrCast<PathLengths>()->fixNextFrontierNodeTable(source.tableID);
    nextFrontier->ptrCast<PathLengths>()->setActive(source);
}

RJOutputs::RJOutputs(nodeID_t sourceNodeID) : sourceNodeID{sourceNodeID} {}

SPOutputs::SPOutputs(std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
    nodeID_t sourceNodeID, storage::MemoryManager* mm)
    : RJOutputs(sourceNodeID) {
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

void SPOutputWriterDsts::writeMoreAndAppend(processor::FactorizedTable& fTable, nodeID_t,
    uint16_t) const {
    fTable.append(vectors);
}
} // namespace function
} // namespace kuzu