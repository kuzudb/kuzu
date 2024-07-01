#pragma once

#include <array>
#include <variant>

#include "common/copy_constructors.h"
#include "common/mpsc_queue.h"
#include "common/static_vector.h"
#include "common/types/int128_t.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "processor/execution_context.h"
#include "storage/index/hash_index.h"
#include "storage/index/hash_index_utils.h"
#include "storage/store/column_chunk_data.h"

namespace kuzu {
namespace processor {

const size_t SHOULD_FLUSH_QUEUE_SIZE = 32;

class IndexBuilderGlobalQueues {
public:
    explicit IndexBuilderGlobalQueues(storage::PrimaryKeyIndex* pkIndex);

    void flushToDisk() const;

    template<typename T>
    void insert(size_t index, storage::IndexBuffer<T> elem) {
        auto& typedQueues = std::get<Queue<T>>(queues).array;
        typedQueues[index].push(std::move(elem));
        if (typedQueues[index].approxSize() < SHOULD_FLUSH_QUEUE_SIZE) {
            return;
        }
        maybeConsumeIndex(index);
    }

    void consume();

    common::PhysicalTypeID pkTypeID() const { return pkIndex->keyTypeID(); }

private:
    void maybeConsumeIndex(size_t index);

    std::array<std::mutex, storage::NUM_HASH_INDEXES> mutexes;
    storage::PrimaryKeyIndex* pkIndex;

    template<typename T>
    struct Queue {
        std::array<common::MPSCQueue<storage::IndexBuffer<T>>, storage::NUM_HASH_INDEXES> array;
        // Type information to help std::visit. Value is not used
        T type;
    };

    // Queues for distributing primary keys.
    std::variant<Queue<std::string>, Queue<int64_t>, Queue<int32_t>, Queue<int16_t>, Queue<int8_t>,
        Queue<uint64_t>, Queue<uint32_t>, Queue<uint16_t>, Queue<uint8_t>, Queue<common::int128_t>,
        Queue<float>, Queue<double>>
        queues;
};

class IndexBuilderLocalBuffers {
public:
    explicit IndexBuilderLocalBuffers(IndexBuilderGlobalQueues& globalQueues);

    void insert(std::string key, common::offset_t value) {
        auto indexPos = storage::HashIndexUtils::getHashIndexPosition(std::string_view(key));
        auto& stringBuffer = (*std::get<UniqueBuffers<std::string>>(buffers))[indexPos];
        if (stringBuffer.full()) {
            // StaticVector's move constructor leaves the original vector valid and empty
            globalQueues->insert(indexPos, std::move(stringBuffer));
        }
        stringBuffer.push_back(std::make_pair(key, value)); // NOLINT(bugprone-use-after-move)
    }

    template<common::HashablePrimitive T>
    void insert(T key, common::offset_t value) {
        auto indexPos = storage::HashIndexUtils::getHashIndexPosition(key);
        auto& buffer = (*std::get<UniqueBuffers<T>>(buffers))[indexPos];
        if (buffer.full()) {
            globalQueues->insert(indexPos, std::move(buffer));
        }
        buffer.push_back(std::make_pair(key, value)); // NOLINT(bugprone-use-after-move)
    }

    void flush();

private:
    IndexBuilderGlobalQueues* globalQueues;

    // These arrays are much too large to be inline.
    template<typename T>
    using Buffers = std::array<storage::IndexBuffer<T>, storage::NUM_HASH_INDEXES>;
    template<typename T>
    using UniqueBuffers = std::unique_ptr<Buffers<T>>;
    std::variant<UniqueBuffers<std::string>, UniqueBuffers<int64_t>, UniqueBuffers<int32_t>,
        UniqueBuffers<int16_t>, UniqueBuffers<int8_t>, UniqueBuffers<uint64_t>,
        UniqueBuffers<uint32_t>, UniqueBuffers<uint16_t>, UniqueBuffers<uint8_t>,
        UniqueBuffers<common::int128_t>, UniqueBuffers<float>, UniqueBuffers<double>>
        buffers;
};

class IndexBuilderSharedState {
    friend class IndexBuilder;

public:
    explicit IndexBuilderSharedState(storage::PrimaryKeyIndex* pkIndex) : globalQueues{pkIndex} {}
    inline void consume() { globalQueues.consume(); }
    inline void flush() { globalQueues.flushToDisk(); }

    inline void addProducer() { producers.fetch_add(1, std::memory_order_relaxed); }
    void quitProducer();
    inline bool isDone() { return done.load(std::memory_order_relaxed); }

private:
    IndexBuilderGlobalQueues globalQueues;

    std::atomic<size_t> producers;
    std::atomic<bool> done;
};

// RAII for producer counting.
class ProducerToken {
public:
    explicit ProducerToken(std::shared_ptr<IndexBuilderSharedState> sharedState)
        : sharedState(std::move(sharedState)) {
        this->sharedState->addProducer();
    }
    DELETE_COPY_DEFAULT_MOVE(ProducerToken);

    void quit() {
        sharedState->quitProducer();
        sharedState.reset();
    }
    ~ProducerToken() {
        if (sharedState) {
            quit();
        }
    }

private:
    std::shared_ptr<IndexBuilderSharedState> sharedState;
};

class IndexBuilder {

public:
    DELETE_COPY_DEFAULT_MOVE(IndexBuilder);
    explicit IndexBuilder(std::shared_ptr<IndexBuilderSharedState> sharedState);

    IndexBuilder clone() { return IndexBuilder(sharedState); }

    void insert(const storage::ColumnChunkData& chunk, common::offset_t nodeOffset,
        common::offset_t numNodes);

    ProducerToken getProducerToken() const { return ProducerToken(sharedState); }

    void finishedProducing();
    void finalize(ExecutionContext* context);

private:
    void checkNonNullConstraint(const storage::NullChunkData& nullChunk, common::offset_t numNodes);
    std::shared_ptr<IndexBuilderSharedState> sharedState;

    IndexBuilderLocalBuffers localBuffers;
};

} // namespace processor
} // namespace kuzu
