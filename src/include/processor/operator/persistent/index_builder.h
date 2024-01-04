#pragma once

#include <variant>

#include "common/copy_constructors.h"
#include "common/mpsc_queue.h"
#include "common/static_vector.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "processor/execution_context.h"
#include "storage/index/hash_index_builder.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace processor {

constexpr size_t BUFFER_SIZE = 1024;
using IntBuffer = common::StaticVector<std::pair<int64_t, common::offset_t>, BUFFER_SIZE>;
using StringBuffer = common::StaticVector<std::pair<std::string, common::offset_t>, BUFFER_SIZE>;

class IndexBuilderGlobalQueues {
public:
    explicit IndexBuilderGlobalQueues(storage::PrimaryKeyIndexBuilder* pkIndex);

    void flushToDisk() const;

    void insert(size_t index, StringBuffer elem);
    void insert(size_t index, IntBuffer elem);

    void consume();

    common::LogicalTypeID pkTypeID() const { return pkIndex->keyTypeID(); }

private:
    void maybeConsumeIndex(size_t index);

    std::array<std::mutex, storage::NUM_HASH_INDEXES> mutexes;
    storage::PrimaryKeyIndexBuilder* pkIndex;

    using StringQueues = std::array<common::MPSCQueue<StringBuffer>, storage::NUM_HASH_INDEXES>;
    using IntQueues = std::array<common::MPSCQueue<IntBuffer>, storage::NUM_HASH_INDEXES>;

    // Queues for distributing primary keys.
    std::variant<StringQueues, IntQueues> queues;
};

class IndexBuilderLocalBuffers {
public:
    explicit IndexBuilderLocalBuffers(IndexBuilderGlobalQueues& globalQueues);

    void insert(std::string key, common::offset_t value);
    void insert(int64_t key, common::offset_t value);

    void flush();

private:
    IndexBuilderGlobalQueues* globalQueues;

    // These arrays are much too large to be inline.
    using StringBuffers = std::array<StringBuffer, storage::NUM_HASH_INDEXES>;
    using IntBuffers = std::array<IntBuffer, storage::NUM_HASH_INDEXES>;
    std::unique_ptr<StringBuffers> stringBuffers;
    std::unique_ptr<IntBuffers> intBuffers;
};

class IndexBuilderSharedState {
    friend class IndexBuilder;

public:
    explicit IndexBuilderSharedState(storage::PrimaryKeyIndexBuilder* pkIndex)
        : globalQueues{pkIndex} {}
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

    void insert(
        storage::ColumnChunk* chunk, common::offset_t nodeOffset, common::offset_t numNodes);

    ProducerToken getProducerToken() const { return ProducerToken(sharedState); }

    void finishedProducing();
    void finalize(ExecutionContext* context);

private:
    void checkNonNullConstraint(storage::NullColumnChunk* nullChunk, common::offset_t numNodes);
    std::shared_ptr<IndexBuilderSharedState> sharedState;

    IndexBuilderLocalBuffers localBuffers;
};

} // namespace processor
} // namespace kuzu
