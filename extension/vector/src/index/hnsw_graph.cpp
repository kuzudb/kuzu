#include "index/hnsw_graph.h"

#include "index/hnsw_index_utils.h"
#include "storage/local_cached_column.h"
#include "storage/table/list_chunk_data.h"
#include "storage/table/node_table.h"
#include "storage/table/rel_table.h"
#include "transaction/transaction.h"

using namespace kuzu::storage;

namespace kuzu {
namespace vector_extension {

EmbeddingHandle::EmbeddingHandle(common::offset_t offset, GetEmbeddingsScanState* lifetimeManager)
    : offsetInData(offset), lifetimeManager(lifetimeManager) {
    if (lifetimeManager) {
        KU_ASSERT(!isNull());
        lifetimeManager->addEmbedding(*this);
    }
}

EmbeddingHandle::~EmbeddingHandle() {
    if (!isNull() && lifetimeManager) {
        lifetimeManager->reclaimEmbedding(*this);
    }
}

// NOLINTNEXTLINE Uses move assignment operator to assign to fields
EmbeddingHandle::EmbeddingHandle(EmbeddingHandle&& o) noexcept {
    *this = std::move(o);
}

EmbeddingHandle& EmbeddingHandle::operator=(EmbeddingHandle&& o) noexcept {
    if (this != &o) {
        offsetInData = o.offsetInData;
        lifetimeManager = o.lifetimeManager;
        o.offsetInData = common::INVALID_OFFSET;
    }
    return *this;
}

struct InMemEmbeddingLocalState : GetEmbeddingsScanState {
    explicit InMemEmbeddingLocalState(const CachedColumn* data, const common::LogicalType& type)
        : data(data), type(type) {}

    void* getEmbeddingPtr(const EmbeddingHandle&) override;

    void addEmbedding(const EmbeddingHandle&) override {}
    void reclaimEmbedding(const EmbeddingHandle&) override {}

    const CachedColumn* data;
    const common::LogicalType& type;
};

void* InMemEmbeddingLocalState::getEmbeddingPtr(const EmbeddingHandle& handle) {
    void* val = nullptr;
    if (!handle.isNull()) {
        common::TypeUtils::visit(type, [&]<typename T>(T) {
            auto [nodeGroupIdx, offsetInGroup] =
                StorageUtils::getNodeGroupIdxAndOffsetInChunk(handle.offsetInData);
            KU_ASSERT(nodeGroupIdx < data->columnChunks.size());
            const auto& listChunk = data->columnChunks[nodeGroupIdx]->cast<ListChunkData>();
            val = &listChunk.getDataColumnChunk()
                       ->getData<T>()[listChunk.getListStartOffset(offsetInGroup)];
        });
        KU_ASSERT(val != nullptr);
    }
    return val;
}

InMemEmbeddings::InMemEmbeddings(transaction::Transaction* transaction,
    common::ArrayTypeInfo typeInfo, common::table_id_t tableID, common::column_id_t columnID)
    : HNSWIndexEmbeddings{std::move(typeInfo)} {
    auto& cacheManager = transaction->getLocalCacheManager();
    const auto key = CachedColumn::getKey(tableID, columnID);
    if (cacheManager.contains(key)) {
        data = transaction->getLocalCacheManager().at(key).cast<CachedColumn>();
    } else {
        throw common::RuntimeException("Miss cached column, which should never happen.");
    }
}

std::unique_ptr<GetEmbeddingsScanState> InMemEmbeddings::constructScanState() const {
    return std::make_unique<InMemEmbeddingLocalState>(data, info.typeInfo.getChildType());
}

static bool isNull(common::offset_t offset, CachedColumn* data) {
    auto [nodeGroupIdx, offsetInGroup] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(offset);
    KU_ASSERT(nodeGroupIdx < data->columnChunks.size());
    const auto& listChunk = data->columnChunks[nodeGroupIdx]->cast<ListChunkData>();
    return listChunk.isNull(offsetInGroup);
}

EmbeddingHandle InMemEmbeddings::getEmbedding(common::offset_t offset,
    GetEmbeddingsScanState& scanState) const {
    if (isNull(offset, data)) {
        return EmbeddingHandle::createNullHandle();
    }
    return EmbeddingHandle{offset, &scanState};
}

std::vector<EmbeddingHandle> InMemEmbeddings::getEmbeddings(
    std::span<const common::offset_t> offsets, GetEmbeddingsScanState& scanState) const {
    std::vector<EmbeddingHandle> ret;
    ret.reserve(offsets.size());
    for (common::offset_t offset : offsets) {
        ret.push_back(getEmbedding(offset, scanState));
    }
    return ret;
}

OnDiskEmbeddings::OnDiskEmbeddings(transaction::Transaction* transaction,
    storage::MemoryManager* mm, common::ArrayTypeInfo typeInfo, storage::NodeTable& nodeTable,
    common::column_id_t columnID)
    : HNSWIndexEmbeddings(std::move(typeInfo)), transaction(transaction), mm(mm),
      nodeTable(nodeTable), columnID(columnID) {}

std::unique_ptr<GetEmbeddingsScanState> OnDiskEmbeddings::constructScanState() const {
    return std::make_unique<OnDiskEmbeddingScanState>(transaction, mm, nodeTable, columnID,
        info.getDimension());
}

EmbeddingHandle OnDiskEmbeddings::getEmbedding(common::offset_t offset,
    GetEmbeddingsScanState& embeddingScanState) const {
    auto& scanState = embeddingScanState.cast<OnDiskEmbeddingScanState>().getScanState();
    if (!nodeTable.isVisibleNoLock(transaction, offset)) {
        return EmbeddingHandle::createNullHandle();
    }
    scanState.nodeIDVector->setValue(0, common::internalID_t{offset, nodeTable.getTableID()});
    scanState.nodeIDVector->state->getSelVectorUnsafe().setToUnfiltered(1);
    const auto source = scanState.source;
    const auto nodeGroupIdx = scanState.nodeGroupIdx;
    if (transaction->isUnCommitted(nodeTable.getTableID(), offset)) {
        scanState.source = TableScanSource::UNCOMMITTED;
        scanState.nodeGroupIdx = StorageUtils::getNodeGroupIdx(
            transaction->getLocalRowIdx(nodeTable.getTableID(), offset));
    } else {
        scanState.source = TableScanSource::COMMITTED;
        scanState.nodeGroupIdx = StorageUtils::getNodeGroupIdx(offset);
    }
    if (source == scanState.source && nodeGroupIdx == scanState.nodeGroupIdx) {
        // If the scan state is already initialized for the same source and node group, we can skip
        // re-initialization.
    } else {
        nodeTable.initScanState(transaction, scanState);
    }
    const auto result = nodeTable.lookup<false>(transaction, scanState);
    KU_ASSERT(scanState.outputVectors.size() == 1 &&
              scanState.outputVectors[0]->state->getSelVector()[0] == 0);
    if (!result || scanState.outputVectors[0]->isNull(0)) {
        return EmbeddingHandle::createNullHandle();
    }
    const auto value = scanState.outputVectors[0]->getValue<common::list_entry_t>(0);
    KU_ASSERT(value.size == info.typeInfo.getNumElements());
    return EmbeddingHandle{value.offset, &embeddingScanState};
}

std::vector<EmbeddingHandle> OnDiskEmbeddings::getEmbeddings(
    std::span<const common::offset_t> offsets, GetEmbeddingsScanState& embeddingScanState) const {
    auto& scanState = embeddingScanState.cast<OnDiskEmbeddingScanState>().getScanState();
    KU_ASSERT(scanState.nodeIDVector->state == scanState.outState);

    // We skip scanning deleted nodes
    common::sel_t numSelectedValues = 0;
    auto& selVector = scanState.nodeIDVector->state->getSelVectorUnsafe();
    selVector.setToFiltered();
    for (auto i = 0u; i < offsets.size(); i++) {
        if (nodeTable.isVisible(transaction, offsets[i])) {
            scanState.nodeIDVector->setValue(i,
                common::internalID_t{offsets[i], nodeTable.getTableID()});
            selVector[numSelectedValues] = i;
            ++numSelectedValues;
        }
    }
    selVector.setSelSize(numSelectedValues);

    KU_ASSERT(
        scanState.outputVectors[0]->dataType.getLogicalTypeID() == common::LogicalTypeID::ARRAY);
    [[maybe_unused]] const auto lookupSuccess =
        nodeTable.lookupMultiple<false>(transaction, scanState);
    KU_ASSERT(lookupSuccess);
    std::vector<EmbeddingHandle> embeddings;
    embeddings.reserve(offsets.size());
    KU_ASSERT(selVector.getSelSize() == numSelectedValues);
    for (common::sel_t i = 0u; i < selVector.getSelSize(); i++) {
        const auto pos = selVector[i];
        while (embeddings.size() < pos) {
            // Return a null embedding for deleted nodes
            embeddings.emplace_back(EmbeddingHandle::createNullHandle());
        }
        if (scanState.outputVectors[0]->isNull(i)) {
            embeddings.emplace_back(EmbeddingHandle::createNullHandle());
        } else {
            const auto value = scanState.outputVectors[0]->getValue<common::list_entry_t>(pos);
            KU_ASSERT(value.size == info.typeInfo.getNumElements());
            embeddings.emplace_back(value.offset, &embeddingScanState);
        }
    }
    return embeddings;
}

OnDiskEmbeddingScanState::OnDiskEmbeddingScanState(const transaction::Transaction* transaction,
    MemoryManager* mm, NodeTable& nodeTable, common::column_id_t columnID,
    common::offset_t embeddingDim)
    : embeddingDim(embeddingDim) {
    std::vector columnIDs{columnID};
    // The first ValueVector in scanChunk is reserved for nodeIDs.
    std::vector<common::LogicalType> types;
    types.emplace_back(common::LogicalType::INTERNAL_ID());
    types.emplace_back(nodeTable.getColumn(columnID).getDataType().copy());
    scanChunk = Table::constructDataChunk(mm, std::move(types));
    std::vector outVectors{&scanChunk.getValueVectorMutable(1)};
    scanState = std::make_unique<NodeTableScanState>(&scanChunk.getValueVectorMutable(0),
        outVectors, scanChunk.state);
    scanState->setToTable(transaction, &nodeTable, std::move(columnIDs));
}

void* OnDiskEmbeddingScanState::getEmbeddingPtr(const EmbeddingHandle& handle) {
    KU_ASSERT(!handle.isNull());
    void* val = nullptr;
    const auto dataVector = common::ListVector::getDataVector(scanState->outputVectors[0]);
    common::TypeUtils::visit(
        dataVector->dataType,
        [&]<VectorElementType T>(
            T) { val = reinterpret_cast<T*>(dataVector->getData()) + handle.offsetInData; },
        [&](auto) { KU_UNREACHABLE; });
    KU_ASSERT(val != nullptr);
    return val;
}

void OnDiskEmbeddingScanState::addEmbedding(const EmbeddingHandle& handle) {
    KU_ASSERT(!handle.isNull());
    KU_ASSERT(handle.offsetInData % embeddingDim == 0);
    const auto offsetToAdd = handle.offsetInData / embeddingDim;
    allocatedOffsets.set(offsetToAdd);
    KU_ASSERT(usedEmbeddingOffsets.empty() || offsetToAdd > usedEmbeddingOffsets.top());
    usedEmbeddingOffsets.push(offsetToAdd);
}

void OnDiskEmbeddingScanState::reclaimEmbedding(const EmbeddingHandle& handle) {
    KU_ASSERT(!handle.isNull());
    KU_ASSERT(handle.offsetInData % embeddingDim == 0);
    const auto offsetToRemove = handle.offsetInData / embeddingDim;
    allocatedOffsets.reset(offsetToRemove);

    /**
     * Reclaim space in output data vector
     * The general idea for managing the used space is:
     * - Each embedding is represented by its offset in the output list data vector
     * - When an embedding is allocated, it is marked as used in 'allocatedOffsets', it is unmarked
     * when it is freed.
     * - When an embedding is allocated, it is also pushed to a stack. The offsets of the stack
     * should be sorted in increasing order
     * - After each free, we pop unmarked offsets off the stack. We then shrink the output list data
     * vector based on the largest offset still on the stack.
     */
    while (!usedEmbeddingOffsets.empty() && !allocatedOffsets.test(usedEmbeddingOffsets.top())) {
        usedEmbeddingOffsets.pop();
    }
    const auto numDataElemsToResizeTo =
        usedEmbeddingOffsets.empty() ? 0 : (usedEmbeddingOffsets.top() + 1) * embeddingDim;
    for (auto* outputVector : scanState->outputVectors) {
        common::ListVector::resizeDataVector(outputVector, numDataElemsToResizeTo);
    }
}

namespace {
template<std::integral CompressedType>
struct TypedCompressedView final : CompressedOffsetsView {
    explicit TypedCompressedView(const uint8_t* data, common::offset_t numEntries)
        : dstNodes(reinterpret_cast<std::atomic<CompressedType>*>(const_cast<uint8_t*>(data)),
              numEntries),
          invalidOffset{std::numeric_limits<CompressedType>::max()} {}

    common::offset_t getNodeOffsetAtomic(common::offset_t idx) const override {
        return dstNodes[idx].load(std::memory_order_relaxed);
    }

    void setNodeOffsetAtomic(common::offset_t idx, common::offset_t nodeOffset) override {
        KU_ASSERT(nodeOffset <= invalidOffset);
        dstNodes[idx].store(static_cast<CompressedType>(nodeOffset), std::memory_order_relaxed);
    }

    common::offset_t getInvalidOffset() const override { return invalidOffset; }

    std::span<std::atomic<CompressedType>> dstNodes;
    common::offset_t invalidOffset;
};

common::offset_t minNumBytesToStore(common::offset_t value) {
    const auto bitWidth = std::bit_width(value);
    static constexpr decltype(bitWidth) bitsPerByte = 8;
    return std::bit_ceil(static_cast<common::offset_t>(common::ceilDiv(bitWidth, bitsPerByte)));
}
} // namespace

CompressedNodeOffsetBuffer::CompressedNodeOffsetBuffer(MemoryManager* mm, common::offset_t numNodes,
    common::length_t maxDegree) {
    const auto numEntries = numNodes * maxDegree;
    // leave one extra slot open for INVALID_ENTRY
    switch (minNumBytesToStore(numNodes + 1)) {
    case 8: {
        buffer = mm->allocateBuffer(false, numEntries * sizeof(std::atomic<uint64_t>));
        view = std::make_unique<TypedCompressedView<uint64_t>>(buffer->getData(), numEntries);
    } break;
    case 4: {
        buffer = mm->allocateBuffer(false, numEntries * sizeof(std::atomic<uint32_t>));
        view = std::make_unique<TypedCompressedView<uint32_t>>(buffer->getData(), numEntries);
    } break;
    case 2: {
        buffer = mm->allocateBuffer(false, numEntries * sizeof(std::atomic<uint16_t>));
        view = std::make_unique<TypedCompressedView<uint16_t>>(buffer->getData(), numEntries);
    } break;
    case 1: {
        buffer = mm->allocateBuffer(false, numEntries * sizeof(std::atomic<uint8_t>));
        view = std::make_unique<TypedCompressedView<uint8_t>>(buffer->getData(), numEntries);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

compressed_offsets_t CompressedNodeOffsetBuffer::getNeighbors(common::offset_t nodeOffset,
    common::offset_t maxDegree, common::offset_t numNbrs) const {
    const auto startOffset = nodeOffset * maxDegree;
    return compressed_offsets_t{*view, startOffset, startOffset + numNbrs};
}

InMemHNSWGraph::InMemHNSWGraph(MemoryManager* mm, common::offset_t numNodes,
    common::length_t maxDegree)
    : numNodes{numNodes}, dstNodes(mm, numNodes, maxDegree), maxDegree{maxDegree} {
    csrLengthBuffer = mm->allocateBuffer(true, numNodes * sizeof(std::atomic<uint16_t>));
    csrLengths = reinterpret_cast<std::atomic<uint16_t>*>(csrLengthBuffer->getData());
    resetCSRLengthAndDstNodes();
}

void InMemHNSWGraph::resetCSRLengthAndDstNodes() {
    for (common::offset_t i = 0; i < numNodes; i++) {
        setCSRLength(i, 0);
    }
    for (common::offset_t i = 0; i < numNodes * maxDegree; i++) {
        setDstNodeInvalid(i);
    }
}

NodeToHNSWGraphOffsetMap::NodeToHNSWGraphOffsetMap(common::offset_t numNodesInTable,
    const common::NullMask* selectedNodes)
    : numNodesInGraph(selectedNodes->countNulls()), numNodesInTable(numNodesInTable),
      graphToNodeMap(std::make_unique<common::offset_t[]>(numNodesInGraph)) {
    common::offset_t curOffset = 0;
    for (common::offset_t i = 0; i < numNodesInTable; ++i) {
        if (selectedNodes->isNull(i)) {
            graphToNodeMap[curOffset] = i;
            ++curOffset;
        }
    }
}

common::offset_t NodeToHNSWGraphOffsetMap::nodeToGraphOffset(common::offset_t nodeOffset,
    [[maybe_unused]] bool exactMatch) const {
    if (isTrivialMapping()) {
        return nodeOffset;
    }
    const auto it =
        std::lower_bound(graphToNodeMap.get(), graphToNodeMap.get() + numNodesInGraph, nodeOffset);
    const common::offset_t pos = it - graphToNodeMap.get();
    KU_ASSERT(!exactMatch || pos >= numNodesInGraph || *it == nodeOffset);
    return pos;
}

common::offset_t NodeToHNSWGraphOffsetMap::graphToNodeOffset(common::offset_t graphOffset) const {
    return isTrivialMapping() ? graphOffset : graphToNodeMap[graphOffset];
}

} // namespace vector_extension
} // namespace kuzu
