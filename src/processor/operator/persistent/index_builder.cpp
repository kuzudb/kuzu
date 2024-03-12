#include "processor/operator/persistent/index_builder.h"

#include <thread>

#include "common/assert.h"
#include "common/cast.h"
#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/type_utils.h"
#include "common/types/ku_string.h"
#include "storage/index/hash_index_utils.h"
#include "storage/store/string_column_chunk.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::storage;

IndexBuilderGlobalQueues::IndexBuilderGlobalQueues(PrimaryKeyIndex* pkIndex) : pkIndex(pkIndex) {
    TypeUtils::visit(
        pkTypeID(), [&](ku_string_t) { queues.emplace<Queue<std::string>>(); },
        [&]<HashablePrimitive T>(T) { queues.emplace<Queue<T>>(); }, [](auto) { KU_UNREACHABLE; });
}

void IndexBuilderGlobalQueues::consume() {
    for (auto index = 0u; index < NUM_HASH_INDEXES; index++) {
        maybeConsumeIndex(index);
    }
}

void IndexBuilderGlobalQueues::maybeConsumeIndex(size_t index) {
    if (!mutexes[index].try_lock()) {
        return;
    }

    std::visit(
        [&](auto&& queues) {
            using T = std::decay_t<decltype(queues.type)>;
            std::unique_lock lck{mutexes[index], std::adopt_lock};
            IndexBuffer<T> buffer;
            while (queues.array[index].pop(buffer)) {
                auto numValuesInserted = pkIndex->appendWithIndexPos(buffer, index);
                if (numValuesInserted < buffer.size()) {
                    if constexpr (std::same_as<T, std::string>) {
                        throw CopyException(ExceptionMessage::duplicatePKException(
                            std::move(buffer[numValuesInserted].first)));
                    } else {
                        throw CopyException(ExceptionMessage::duplicatePKException(
                            TypeUtils::toString(buffer[numValuesInserted].first)));
                    }
                }
            }
            return;
        },
        std::move(queues));
}

void IndexBuilderGlobalQueues::flushToDisk() const {
    pkIndex->prepareCommit();
}

IndexBuilderLocalBuffers::IndexBuilderLocalBuffers(IndexBuilderGlobalQueues& globalQueues)
    : globalQueues(&globalQueues) {
    TypeUtils::visit(
        globalQueues.pkTypeID(),
        [&](ku_string_t) { buffers = std::make_unique<Buffers<std::string>>(); },
        [&]<HashablePrimitive T>(T) { buffers = std::make_unique<Buffers<T>>(); },
        [](auto) { KU_UNREACHABLE; });
}

void IndexBuilderLocalBuffers::flush() {
    std::visit(
        [&](auto&& buffers) {
            for (auto i = 0u; i < buffers->size(); i++) {
                globalQueues->insert(i, std::move((*buffers)[i]));
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

void IndexBuilder::insert(const ColumnChunk& chunk, offset_t nodeOffset, offset_t numNodes) {
    checkNonNullConstraint(chunk.getNullChunk(), numNodes);

    TypeUtils::visit(
        chunk.getDataType().getPhysicalType(),
        [&]<HashablePrimitive T>(T) {
            for (auto i = 0u; i < numNodes; i++) {
                auto value = chunk.getValue<T>(i);
                localBuffers.insert(value, nodeOffset + i);
            }
        },
        [&](ku_string_t) {
            auto& stringColumnChunk =
                ku_dynamic_cast<const ColumnChunk&, const StringColumnChunk&>(chunk);
            for (auto i = 0u; i < numNodes; i++) {
                auto value = stringColumnChunk.getValue<std::string>(i);
                localBuffers.insert(std::move(value), nodeOffset + i);
            }
        },
        [&](auto) {
            throw CopyException(ExceptionMessage::invalidPKType(chunk.getDataType().toString()));
        });
}

void IndexBuilder::finishedProducing() {
    localBuffers.flush();
    sharedState->consume();
    while (!sharedState->isDone()) {
        std::this_thread::sleep_for(std::chrono::microseconds(500));
        sharedState->consume();
    }
}

void IndexBuilder::finalize(ExecutionContext* /*context*/) {
    // Flush anything added by last node group.
    localBuffers.flush();

    sharedState->consume();
    sharedState->flush();
}

void IndexBuilder::checkNonNullConstraint(const NullColumnChunk& nullChunk, offset_t numNodes) {
    for (auto i = 0u; i < numNodes; i++) {
        if (nullChunk.isNull(i)) {
            throw CopyException(ExceptionMessage::nullPKException());
        }
    }
}

} // namespace processor
} // namespace kuzu
