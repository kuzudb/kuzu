#include "processor/operator/persistent/index_builder.h"

#include <thread>

#include "common/assert.h"
#include "common/cast.h"
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

void IndexBuilderGlobalQueues::consume(IndexBuilderErrorHandler& errors) {
    for (auto index = 0u; index < NUM_HASH_INDEXES; index++) {
        maybeConsumeIndex(index, errors);
    }
}

void IndexBuilderGlobalQueues::maybeConsumeIndex(size_t index, IndexBuilderErrorHandler& errors) {
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
                    errors.handleOrStoreError<T>(
                        {.message = ExceptionMessage::duplicatePKException(
                             TypeUtils::toString(buffer[numValuesInserted].first)),
                            .key = buffer[numValuesInserted].first,
                            .nodeID = nodeID_t{
                                buffer[numValuesInserted].second,
                                nodeTable->getTableID(),
                            }});
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

void IndexBuilderLocalBuffers::flush(IndexBuilderErrorHandler& errors) {
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
    IndexBuilderErrorHandler& errors) {
    TypeUtils::visit(
        chunk.getDataType().getPhysicalType(),
        [&]<HashablePrimitive T>(T) {
            for (auto i = 0u; i < numNodes; i++) {
                if (checkNonNullConstraint(chunk, nodeOffset, i, errors)) {
                    auto value = chunk.getValue<T>(i);
                    localBuffers.insert(value, nodeOffset + i, errors);
                }
            }
        },
        [&](ku_string_t) {
            auto& stringColumnChunk =
                ku_dynamic_cast<const ColumnChunkData&, const StringChunkData&>(chunk);
            for (auto i = 0u; i < numNodes; i++) {
                if (checkNonNullConstraint(chunk, nodeOffset, i, errors)) {
                    auto value = stringColumnChunk.getValue<std::string>(i);
                    localBuffers.insert(std::move(value), nodeOffset + i, errors);
                }
            }
        },
        [&]<typename T>(T) {
            throw CopyException(ExceptionMessage::invalidPKType(chunk.getDataType().toString()));
        });
}

void IndexBuilder::finishedProducing(IndexBuilderErrorHandler& errors) {
    localBuffers.flush(errors);
    sharedState->consume(errors);
    while (!sharedState->isDone()) {
        std::this_thread::sleep_for(std::chrono::microseconds(500));
        sharedState->consume(errors);
    }
}

void IndexBuilder::finalize(ExecutionContext* /*context*/, IndexBuilderErrorHandler& errors) {
    // Flush anything added by last node group.
    localBuffers.flush(errors);

    sharedState->consume(errors);
}

bool IndexBuilder::checkNonNullConstraint(const ColumnChunkData& chunk, offset_t nodeOffset,
    offset_t chunkOffset, IndexBuilderErrorHandler& errors) {
    const auto& nullChunk = chunk.getNullData();
    if (nullChunk.isNull(chunkOffset)) {
        TypeUtils::visit(
            chunk.getDataType().getPhysicalType(),
            [&](struct_entry_t) {
                // primary key cannot be struct
                KU_UNREACHABLE;
            },
            [&]<typename T>(T) {
                errors.handleOrStoreError<T>({.message = ExceptionMessage::nullPKException(),
                    .key = {},
                    .nodeID =
                        nodeID_t{nodeOffset + chunkOffset, sharedState->nodeTable->getTableID()}});
            });
        return false;
    }
    return true;
}

} // namespace processor
} // namespace kuzu
