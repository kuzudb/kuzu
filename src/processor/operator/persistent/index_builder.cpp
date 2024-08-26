#include "processor/operator/persistent/index_builder.h"

#include <thread>

#include "common/assert.h"
#include "common/cast.h"
#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/type_utils.h"
#include "common/types/ku_string.h"
#include "storage/index/hash_index_utils.h"
#include "storage/store/node_table.h"
#include "storage/store/string_chunk_data.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::storage;

CopyIndexErrors::CopyIndexErrors(main::ClientContext* clientContext, common::LogicalTypeID pkType)
    : clientContext(clientContext), pkType(pkType) {}

void CopyIndexErrors::append(const CopyIndexErrors& errors) {
    for (idx_t i = 0; i < errors.offsetVector.size(); ++i) {
        offsetVector.insert(offsetVector.end(), errors.offsetVector.begin(),
            errors.offsetVector.end());
        keyVector.insert(keyVector.end(), errors.keyVector.begin(), errors.keyVector.end());
        currentVectorSize = errors.currentVectorSize;
    }
}

void CopyIndexErrors::addNewVectors() {
    offsetVector.push_back(std::make_shared<ValueVector>(LogicalTypeID::INTERNAL_ID,
        clientContext->getMemoryManager()));
    offsetVector.back()->state = DataChunkState::getSingleValueDataChunkState();
    keyVector.push_back(std::make_shared<ValueVector>(pkType, clientContext->getMemoryManager()));
    keyVector.back()->state = DataChunkState::getSingleValueDataChunkState();
}

IndexBuilderGlobalQueues::IndexBuilderGlobalQueues(transaction::Transaction* transaction,
    NodeTable* nodeTable)
    : nodeTable(nodeTable), transaction{transaction} {
    TypeUtils::visit(
        pkTypeID(), [&](ku_string_t) { queues.emplace<Queue<std::string>>(); },
        [&]<HashablePrimitive T>(T) { queues.emplace<Queue<T>>(); }, [](auto) { KU_UNREACHABLE; });
}

PhysicalTypeID IndexBuilderGlobalQueues::pkTypeID() const {
    return nodeTable->getPKIndex()->keyTypeID();
}

void IndexBuilderGlobalQueues::consume(CopyIndexErrors& errors) {
    for (auto index = 0u; index < NUM_HASH_INDEXES; index++) {
        maybeConsumeIndex(index, errors);
    }
}

void IndexBuilderGlobalQueues::maybeConsumeIndex(size_t index, CopyIndexErrors& errors) {
    if (!mutexes[index].try_lock()) {
        return;
    }

    std::visit(
        [&](auto&& queues) {
            using T = std::decay_t<decltype(queues.type)>;
            std::unique_lock lck{mutexes[index], std::adopt_lock};
            IndexBuffer<T> buffer;
            while (queues.array[index].pop(buffer)) {
                auto numValuesInserted =
                    nodeTable->appendPKWithIndexPos(transaction, buffer, index);
                if (numValuesInserted < buffer.size()) {
                    // if constexpr (std::same_as<T, std::string>) {
                    //     throw CopyException(ExceptionMessage::duplicatePKException(
                    //         std::move(buffer[numValuesInserted].first)));
                    // } else {
                    //     throw CopyException(ExceptionMessage::duplicatePKException(
                    //         TypeUtils::toString(buffer[numValuesInserted].first)));
                    // }
                    errors.addError(buffer[numValuesInserted].first,
                        nodeID_t{buffer[numValuesInserted].second, nodeTable->getTableID()});
                }
            }
            return;
        },
        std::move(queues));
}

IndexBuilderLocalBuffers::IndexBuilderLocalBuffers(IndexBuilderGlobalQueues& globalQueues)
    : globalQueues(&globalQueues) {
    TypeUtils::visit(
        globalQueues.pkTypeID(),
        [&](ku_string_t) { buffers = std::make_unique<Buffers<std::string>>(); },
        [&]<HashablePrimitive T>(T) { buffers = std::make_unique<Buffers<T>>(); },
        [](auto) { KU_UNREACHABLE; });
}

void IndexBuilderLocalBuffers::flush(CopyIndexErrors& errors) {
    std::visit(
        [&](auto&& buffers) {
            for (auto i = 0u; i < buffers->size(); i++) {
                globalQueues->insert(i, std::move((*buffers)[i]), errors);
            }
        },
        buffers);
}

IndexBuilder::IndexBuilder(std::shared_ptr<IndexBuilderSharedState> sharedState)
    : sharedState(std::move(sharedState)), localBuffers(this->sharedState->globalQueues) {}

void IndexBuilderSharedState::quitProducer() {
    if (producers.fetch_sub(1, std::memory_order_relaxed) == 1) {
        done.store(true, std::memory_order_relaxed);
    }
}

void IndexBuilder::insert(const ColumnChunkData& chunk, offset_t nodeOffset, offset_t numNodes,
    CopyIndexErrors& errors) {
    checkNonNullConstraint(chunk, nodeOffset, numNodes, errors);
    TypeUtils::visit(
        chunk.getDataType().getPhysicalType(),
        [&]<HashablePrimitive T>(T) {
            for (auto i = 0u; i < numNodes; i++) {
                auto value = chunk.getValue<T>(i);
                localBuffers.insert(value, nodeOffset + i, errors);
            }
        },
        [&](ku_string_t) {
            auto& stringColumnChunk =
                ku_dynamic_cast<const ColumnChunkData&, const StringChunkData&>(chunk);
            for (auto i = 0u; i < numNodes; i++) {
                auto value = stringColumnChunk.getValue<std::string>(i);
                localBuffers.insert(std::move(value), nodeOffset + i, errors);
            }
        },
        [&](auto) {
            throw CopyException(ExceptionMessage::invalidPKType(chunk.getDataType().toString()));
            // TypeUtils::visit(chunk.getDataType().getPhysicalType(), [&]<typename T>(T) {
            //     errors.addError(T{}, nodeID_t{0, sharedState->nodeTable->getTableID()});
            // });
        });
}

void IndexBuilder::finishedProducing(CopyIndexErrors& errors) {
    localBuffers.flush(errors);
    sharedState->consume(errors);
    while (!sharedState->isDone()) {
        std::this_thread::sleep_for(std::chrono::microseconds(500));
        sharedState->consume(errors);
    }
}

void IndexBuilder::finalize(ExecutionContext* /*context*/, CopyIndexErrors& errors) {
    // Flush anything added by last node group.
    localBuffers.flush(errors);

    sharedState->consume(errors);
}

void IndexBuilder::checkNonNullConstraint(const ColumnChunkData& chunk, offset_t nodeOffset,
    offset_t numNodes, CopyIndexErrors& errors) {
    const auto& nullChunk = chunk.getNullData();
    for (auto i = 0u; i < numNodes; i++) {
        if (nullChunk.isNull(i)) {
            // throw CopyException(ExceptionMessage::nullPKException());
            // errors.addError(T key, common::nodeID_t offset)
            TypeUtils::visit(
                chunk.getDataType().getPhysicalType(),
                [&](struct_entry_t) {
                    // TODO handle
                },
                [&]<typename T>(T) {
                    errors.addError(T{},
                        nodeID_t{nodeOffset + i, sharedState->nodeTable->getTableID()});
                });
        }
    }
}

} // namespace processor
} // namespace kuzu
