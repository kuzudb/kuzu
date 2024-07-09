#include "processor/operator/csr_index_build.h"

#include <thread>

#include "processor/operator/scan/scan_node_table.h"

namespace kuzu {
namespace processor {

void CSRIndexBuild::initGlobalStateInternal(kuzu::processor::ExecutionContext* context) {
    auto child = children[0].get();
    while (child && !child->isSource()) {
        child = child->getChild(0);
    }
    auto scanNodeID = (ScanNodeTable*)child;
    auto nodeTable = scanNodeID->getSharedState(0).getNodeTable();
    auto maxNodeOffset = nodeTable->getMaxNodeOffset(context->clientContext->getTx());
    size_t totalSize = std::ceil((double)(maxNodeOffset + 1) / MORSEL_SIZE);
    csrSharedState->csr = std::vector<graph::MorselCSR*>(totalSize, nullptr);
}

void CSRIndexBuild::initLocalStateInternal(kuzu::processor::ResultSet* resultSet,
    kuzu::processor::ExecutionContext* context) {
    boundNodeVector = resultSet->getValueVector(boundNodeVectorPos).get();
    nbrNodeVector = std::make_unique<common::ValueVector>(common::LogicalType::INTERNAL_ID(),
        context->clientContext->getMemoryManager());
    nbrNodeVector->state = std::make_shared<common::DataChunkState>();
    fwdReadState = std::make_unique<storage::RelTableScanState>(columnIDs, direction);
    fwdReadState->nodeIDVector = boundNodeVector;
    fwdReadState->outputVectors.push_back(nbrNodeVector.get());
}

uint64_t CSRIndexBuild::calculateDegree(ExecutionContext *context) {
    uint64_t totalSize = 0u;
    auto relReadState = common::ku_dynamic_cast<storage::TableDataScanState*,
        storage::RelDataReadState*>(fwdReadState->dataScanState.get());
    relTable->initializeScanState(context->clientContext->getTx(), *fwdReadState.get());
    auto firstNode = boundNodeVector->getValue<common::nodeID_t>(0);
    auto prevNGIdx = storage::StorageUtils::getNodeGroupIdx(firstNode.offset);
    for (auto i = 0u; i < boundNodeVector->state->getSelVector().getSelSize(); i++) {
        auto nodeID = boundNodeVector->getValue<common::nodeID_t>(i);
        auto nextNGIdx = storage::StorageUtils::getNodeGroupIdx(nodeID.offset);
        if (prevNGIdx != nextNGIdx) {
            boundNodeVector->setValue(0, nodeID);
            relTable->initializeScanState(context->clientContext->getTx(), *fwdReadState.get());
            prevNGIdx = nextNGIdx;
        }
        relReadState->currentNodeOffset = nodeID.offset;
        totalSize += relReadState->getNumNbrs();
    }
    boundNodeVector->setValue(0, firstNode);
    return totalSize;
}

void CSRIndexBuild::executeInternal(kuzu::processor::ExecutionContext* context) {
    auto duration1 = std::chrono::system_clock::now().time_since_epoch();
    auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
    std::ostringstream oss;
    oss << std::this_thread::get_id();
    printf("Thread %s starting at %ld\n", oss.str().c_str(), millis1);
    auto& csr = csrSharedState->csr;
    int totalNbrsHandled = 0;
    // use this to determine when to sum up the csr_v vector
    // basically we keep summing at (offset + 1) position the total no. of neighbours
    // at the end we need to sum from position 1 to 65 to update neighbour ranges
    graph::MorselCSR* lastMorselCSRHandled = nullptr;
    // track how much of the block has been used to write neighbour offsets
    // when blockSize < (currBlockSizeUsed + next value vector size)
    uint64_t currBlockSizeUsed = 0u;
    auto txn = context->clientContext->getTx();
    while (children[0]->getNextTuple(context)) {
        for (auto i = 0u; i < boundNodeVector->state->getSelVector().getSelSize(); i++) {
            auto pos = boundNodeVector->state->getSelVector().operator[](i);
            auto boundNode = boundNodeVector->getValue<common::nodeID_t>(pos);
            auto csrPos = (boundNode.offset >> RIGHT_SHIFT);
            auto &morselCSR = csr[csrPos];
            // Encountering a new group for the 1st time, calculate degree of morsel and allot memory.
            if (!morselCSR) {
                auto totalSize = calculateDegree(context);
                auto newEntry = new graph::MorselCSR(totalSize);
                __atomic_store_n(&csr[csrPos], newEntry, __ATOMIC_RELEASE);
                // Sum up the cs_v array to get the range of neighbours for each offset.
                if (lastMorselCSRHandled) {
                    for (auto j = 1; j < (MORSEL_SIZE + 1); j++) {
                        lastMorselCSRHandled->csr_v[j] += lastMorselCSRHandled->csr_v[j - 1];
                    }
                }
                lastMorselCSRHandled = morselCSR;
                currBlockSizeUsed = 0u;
            }
            // add reading the actual nbrs code here later
            boundNodeVector->setValue(0, boundNode);
            relTable->initializeScanState(txn, *fwdReadState.get());
            while (fwdReadState->hasMoreToRead(txn)) {
                relTable->scan(txn, *fwdReadState);
                auto totalNbrs = nbrNodeVector->state->getSelVector().getSelSize();
                for (auto idx = 0u; idx < totalNbrs; idx++) {
                    auto nbrIdPos = nbrNodeVector->state->getSelVectorUnsafe().getSelectedPositions()[idx];
                    auto nbrNode = nbrNodeVector->getValue<common::nodeID_t>(nbrIdPos);
                    morselCSR->nbrNodeOffsets[currBlockSizeUsed] = nbrNode.offset;
                    currBlockSizeUsed++;
                }
                morselCSR->csr_v[(boundNode.offset & OFFSET_DIV) + 1] += totalNbrs;
                totalNbrsHandled += totalNbrs;
            }
        }
    }
    if (lastMorselCSRHandled) {
        for (auto i = 1; i < (MORSEL_SIZE + 1); i++) {
            lastMorselCSRHandled->csr_v[i] += lastMorselCSRHandled->csr_v[i - 1];
        }
    }
    // If this causes performance problems, switch to memory_order_acq_rel
    std::atomic_thread_fence(std::memory_order_seq_cst);
    auto duration2 = std::chrono::system_clock::now().time_since_epoch();
    auto millis2 = std::chrono::duration_cast<std::chrono::milliseconds>(duration2).count();
    printf("Thread %s finished at %lu, total time taken %lu ms, total neighbours: %d \n",
        oss.str().c_str(), millis2, (millis2 - millis1), totalNbrsHandled);
}

} // namespace processor
} // namespace kuzu
