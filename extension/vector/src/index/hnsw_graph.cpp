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

InMemEmbeddings::InMemEmbeddings(transaction::Transaction* transaction,
    common::ArrayTypeInfo typeInfo, common::table_id_t tableID, common::column_id_t columnID)
    : CreateHNSWIndexEmbeddings{std::move(typeInfo)} {
    auto& cacheManager = transaction->getLocalCacheManager();
    const auto key = CachedColumn::getKey(tableID, columnID);
    if (cacheManager.contains(key)) {
        data = transaction->getLocalCacheManager().at(key).cast<CachedColumn>();
    } else {
        throw common::RuntimeException("Miss cached column, which should never happen.");
    }
}

VectorEmbedding InMemEmbeddings::getEmbedding(common::offset_t offset) const {
    void* val = nullptr;
    if (!isNull(offset)) {
        common::TypeUtils::visit(info.typeInfo.getChildType(), [&]<typename T>(T) {
            auto [nodeGroupIdx, offsetInGroup] =
                StorageUtils::getNodeGroupIdxAndOffsetInChunk(offset);
            KU_ASSERT(nodeGroupIdx < data->columnChunks.size());
            const auto& listChunk = data->columnChunks[nodeGroupIdx]->cast<ListChunkData>();
            val = &listChunk.getDataColumnChunk()
                       ->getData<T>()[listChunk.getListStartOffset(offsetInGroup)];
        });
        KU_ASSERT(val != nullptr);
    }
    return VectorEmbedding{val};
}

bool InMemEmbeddings::isNull(common::offset_t offset) const {
    auto [nodeGroupIdx, offsetInGroup] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(offset);
    KU_ASSERT(nodeGroupIdx < data->columnChunks.size());
    const auto& listChunk = data->columnChunks[nodeGroupIdx]->cast<ListChunkData>();
    return listChunk.isNull(offsetInGroup);
}

OnDiskEmbeddingScanState::OnDiskEmbeddingScanState(const transaction::Transaction* transaction,
    MemoryManager* mm, NodeTable& nodeTable, common::column_id_t columnID) {
    initScanState(transaction, mm, nodeTable, columnID, scanChunk, scanState);
}

void OnDiskEmbeddingScanState::initScanState(const transaction::Transaction* transaction,
    MemoryManager* mm, NodeTable& nodeTable, common::column_id_t columnID,
    common::DataChunk& scanChunk, std::unique_ptr<storage::NodeTableScanState>& scanState) {
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

void* OnDiskEmbeddings::getEmbedding(transaction::Transaction* transaction,
    NodeTableScanState& scanState, common::offset_t offset) const {
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
    scanState.outputVectors[0]->resetAuxiliaryBuffer();
    const auto result = nodeTable.lookupNoLock(transaction, scanState);
    KU_ASSERT(scanState.outputVectors.size() == 1 &&
              scanState.outputVectors[0]->state->getSelVector()[0] == 0);
    if (!result || scanState.outputVectors[0]->isNull(0)) {
        return nullptr;
    }
    const auto value = scanState.outputVectors[0]->getValue<common::list_entry_t>(0);
    KU_ASSERT(value.size == info.typeInfo.getNumElements());
    KU_UNUSED(value);
    const auto dataVector = common::ListVector::getDataVector(scanState.outputVectors[0]);
    void* val = nullptr;
    common::TypeUtils::visit(
        info.typeInfo.getChildType(),
        [&]<VectorElementType T>(
            T) { val = reinterpret_cast<T*>(dataVector->getData()) + value.offset; },
        [&](auto) { KU_UNREACHABLE; });
    KU_ASSERT(val != nullptr);
    return val;
}

std::vector<void*> OnDiskEmbeddings::getEmbeddings(transaction::Transaction* transaction,
    NodeTableScanState& scanState, const std::vector<common::offset_t>& offsets) const {
    for (auto i = 0u; i < offsets.size(); i++) {
        scanState.nodeIDVector->setValue(i,
            common::internalID_t{offsets[i], nodeTable.getTableID()});
    }
    scanState.nodeIDVector->state->getSelVectorUnsafe().setToUnfiltered(offsets.size());
    KU_ASSERT(
        scanState.outputVectors[0]->dataType.getLogicalTypeID() == common::LogicalTypeID::ARRAY);
    common::ListVector::resizeDataVector(scanState.outputVectors[0],
        offsets.size() * info.typeInfo.getNumElements());
    scanState.outputVectors[0]->resetAuxiliaryBuffer();
    nodeTable.lookupMultiple(transaction, scanState);
    std::vector<void*> embeddings;
    embeddings.reserve(offsets.size());
    for (auto i = 0u; i < offsets.size(); i++) {
        if (scanState.outputVectors[0]->isNull(i)) {
            embeddings.push_back(nullptr);
        } else {
            const auto value = scanState.outputVectors[0]->getValue<common::list_entry_t>(i);
            KU_ASSERT(value.size == info.typeInfo.getNumElements());
            const auto dataVector = common::ListVector::getDataVector(scanState.outputVectors[0]);
            void* val = nullptr;
            common::TypeUtils::visit(
                info.typeInfo.getChildType(),
                [&]<VectorElementType T>(
                    T) { val = reinterpret_cast<T*>(dataVector->getData()) + value.offset; },
                [&](auto) { KU_UNREACHABLE; });
            KU_ASSERT(val != nullptr);
            embeddings.push_back(val);
        }
    }
    return embeddings;
}

OnDiskCreateHNSWIndexEmbeddings::OnDiskCreateHNSWIndexEmbeddings(
    transaction::Transaction* transaction, storage::MemoryManager* mm,
    common::ArrayTypeInfo typeInfo, storage::NodeTable& nodeTable, common::column_id_t columnID)
    : CreateHNSWIndexEmbeddings(std::move(typeInfo)), embeddings(std::move(typeInfo), nodeTable),
      transaction(transaction), mm(mm), nodeTable(nodeTable), columnID(columnID) {
    types.emplace_back(common::LogicalType::INTERNAL_ID());
    types.emplace_back(nodeTable.getColumn(columnID).getDataType().copy());
    scanChunkState = std::make_shared<common::DataChunkState>();
}

// static void setScanStateToChunk(NodeTableScanState& scanState, common::DataChunk& scanChunk) {
//     scanState.nodeIDVector = &scanChunk.getValueVectorMutable(0);
//     scanState.outputVectors = std::vector{&scanChunk.getValueVectorMutable(1)};
//     scanState.outState = scanChunk.state;
//     scanState.rowIdxVector->setState(scanChunk.state);
// }

VectorEmbedding OnDiskCreateHNSWIndexEmbeddings::getEmbedding(common::offset_t offset) const {
    auto scanChunk = std::make_unique<common::DataChunk>(
        Table::constructDataChunk(mm, copyVector(types), scanChunkState));
    auto scanState = NodeTableScanState{&scanChunk->getValueVectorMutable(0),
        std::vector{&scanChunk->getValueVectorMutable(1)}, scanChunk->state};
    scanState.setToTable(transaction, &nodeTable, std::vector{columnID});
    auto* embeddingPtr = embeddings.getEmbedding(transaction, scanState, offset);
    KU_ASSERT(embeddingPtr != nullptr);
    return VectorEmbedding{embeddingPtr, std::move(scanChunk)};
}

bool OnDiskCreateHNSWIndexEmbeddings::isNull(common::offset_t offset) const {
    auto scanChunk = std::make_unique<common::DataChunk>(
        Table::constructDataChunk(mm, copyVector(types), scanChunkState));
    auto scanState = NodeTableScanState{&scanChunk->getValueVectorMutable(0),
        std::vector{&scanChunk->getValueVectorMutable(1)}, scanChunk->state};
    scanState.setToTable(transaction, &nodeTable, std::vector{columnID});
    return embeddings.getEmbedding(transaction, scanState, offset) == nullptr;
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
    : numNodes{numNodes}, dstNodes(mm, numNodes, maxDegree), maxDegree{maxDegree},
      invalidOffset(dstNodes.getInvalidOffset()) {
    KU_ASSERT(invalidOffset > 0);
    csrLengthBuffer = mm->allocateBuffer(true, numNodes * sizeof(std::atomic<uint16_t>));
    csrLengths = reinterpret_cast<std::atomic<uint16_t>*>(csrLengthBuffer->getData());
    resetCSRLengthAndDstNodes();
}

void InMemHNSWGraph::resetCSRLengthAndDstNodes() {
    for (common::offset_t i = 0; i < numNodes; i++) {
        setCSRLength(i, 0);
    }
    for (common::offset_t i = 0; i < numNodes * maxDegree; i++) {
        setDstNode(i, getInvalidOffset());
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
