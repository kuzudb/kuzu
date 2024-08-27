#pragma once

#include <array>
#include <variant>

#include "common/copy_constructors.h"
#include "common/mpsc_queue.h"
#include "common/static_vector.h"
#include "common/types/int128_t.h"
#include "common/types/types.h"
#include "processor/execution_context.h"
#include "processor/operator/persistent/node_batch_insert_error_handler.h"
#include "storage/index/hash_index.h"
#include "storage/index/hash_index_utils.h"
#include "storage/store/column_chunk_data.h"

namespace kuzu {
namespace transaction {
class Transaction;
};
namespace storage {
class NodeTable;
};
namespace processor {

const size_t SHOULD_FLUSH_QUEUE_SIZE = 32;

constexpr size_t BUFFER_SIZE = 64;
using OptionalExtraDataBuffer = std::optional<common::StaticVector<WarningSourceData, BUFFER_SIZE>>;

using OptionalWarningSourceData = std::optional<WarningSourceData>;

class IndexBuilderGlobalQueues {
public:
    explicit IndexBuilderGlobalQueues(transaction::Transaction* transaction,
        storage::NodeTable* nodeTable);

    template<typename T>
    void insert(size_t index, storage::IndexBuffer<T> elem, OptionalExtraDataBuffer extraData,
        NodeBatchInsertErrorHandler& errorHandler) {
        auto& extraDataQueues = std::get<Queue<T>>(queues).extraDataArray;
        extraDataQueues[index].push(std::move(extraData));
        auto& typedQueues = std::get<Queue<T>>(queues).array;
        typedQueues[index].push(std::move(elem));
        if (typedQueues[index].approxSize() < SHOULD_FLUSH_QUEUE_SIZE) {
            return;
        }
        maybeConsumeIndex(index, errorHandler);
    }

    void consume(NodeBatchInsertErrorHandler& errorHandler);

    common::PhysicalTypeID pkTypeID() const;

private:
    void maybeConsumeIndex(size_t index, NodeBatchInsertErrorHandler& errorHandler);

    std::array<std::mutex, storage::NUM_HASH_INDEXES> mutexes;
    storage::NodeTable* nodeTable;

    template<typename T>
    struct Queue {
        std::array<common::MPSCQueue<storage::IndexBuffer<T>>, storage::NUM_HASH_INDEXES> array;
        std::array<common::MPSCQueue<OptionalExtraDataBuffer>, storage::NUM_HASH_INDEXES>
            extraDataArray;
        // Type information to help std::visit. Value is not used
        T type;
    };

    // Queues for distributing primary keys.
    std::variant<Queue<std::string>, Queue<int64_t>, Queue<int32_t>, Queue<int16_t>, Queue<int8_t>,
        Queue<uint64_t>, Queue<uint32_t>, Queue<uint16_t>, Queue<uint8_t>, Queue<common::int128_t>,
        Queue<float>, Queue<double>>
        queues;
    transaction::Transaction* transaction;
};

class IndexBuilderLocalBuffers {
public:
    explicit IndexBuilderLocalBuffers(IndexBuilderGlobalQueues& globalQueues);

    void insert(std::string key, OptionalWarningSourceData extraData, common::offset_t value,
        NodeBatchInsertErrorHandler& errorHandler) {
        auto indexPos = storage::HashIndexUtils::getHashIndexPosition(std::string_view(key));
        auto& stringBuffer = (*std::get<UniqueBuffers<std::string>>(buffers))[indexPos];

        auto& extraDataBuffer = extraDataBuffers[indexPos];

        if (stringBuffer.full() || (extraDataBuffer.has_value() && extraDataBuffer->full())) {
            // StaticVector's move constructor leaves the original vector valid and empty
            globalQueues->insert(indexPos, std::move(stringBuffer), std::move(extraDataBuffer),
                errorHandler);
        }

        // NOLINTBEGIN (bugprone-use-after-move) moving the buffer clears it which is the expected
        // behaviour
        stringBuffer.push_back(std::make_pair(key, value));
        optionalAppendExtraData(extraDataBuffer, std::move(extraData));
        // NOLINTEND
    }

    template<common::HashablePrimitive T>
    void insert(T key, OptionalWarningSourceData extraData, common::offset_t value,
        NodeBatchInsertErrorHandler& errorHandler) {
        auto indexPos = storage::HashIndexUtils::getHashIndexPosition(key);
        auto& buffer = (*std::get<UniqueBuffers<T>>(buffers))[indexPos];

        auto& extraDataBuffer = extraDataBuffers[indexPos];

        if (buffer.full() || (extraDataBuffer.has_value() && extraDataBuffer->full())) {
            globalQueues->insert(indexPos, std::move(buffer), std::move(extraDataBuffer),
                errorHandler);
        }

        // NOLINTBEGIN (bugprone-use-after-move) moving the buffer clears it which is the expected
        // behaviour
        buffer.push_back(std::make_pair(key, value));
        optionalAppendExtraData(extraDataBuffer, std::move(extraData));
        // NOLINTEND
    }

    void flush(NodeBatchInsertErrorHandler& errorHandler);

private:
    void optionalAppendExtraData(OptionalExtraDataBuffer& buffer, OptionalWarningSourceData data);

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

    std::array<OptionalExtraDataBuffer, storage::NUM_HASH_INDEXES> extraDataBuffers;
};

class IndexBuilderSharedState {
    friend class IndexBuilder;

public:
    explicit IndexBuilderSharedState(transaction::Transaction* transaction,
        storage::NodeTable* nodeTable)
        : globalQueues{transaction, nodeTable}, nodeTable(nodeTable) {}
    inline void consume(NodeBatchInsertErrorHandler& errorHandler) {
        return globalQueues.consume(errorHandler);
    }

    inline void addProducer() { producers.fetch_add(1, std::memory_order_relaxed); }
    void quitProducer();
    inline bool isDone() { return done.load(std::memory_order_relaxed); }

private:
    IndexBuilderGlobalQueues globalQueues;
    storage::NodeTable* nodeTable;

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

    std::optional<IndexBuilder> clone() { return IndexBuilder(sharedState); }

    void insert(const storage::ColumnChunkData& chunk,
        const std::optional<std::vector<storage::ColumnChunkData*>>& extraData,
        common::offset_t nodeOffset, common::offset_t numNodes,
        NodeBatchInsertErrorHandler& errorHandler);

    ProducerToken getProducerToken() const { return ProducerToken(sharedState); }

    void finishedProducing(NodeBatchInsertErrorHandler& errorHandler);
    void finalize(ExecutionContext* context, NodeBatchInsertErrorHandler& errorHandler);

private:
    bool checkNonNullConstraint(const storage::ColumnChunkData& chunk,
        const std::optional<std::vector<storage::ColumnChunkData*>>& extraData,
        common::offset_t nodeOffset, common::offset_t chunkOffset,
        NodeBatchInsertErrorHandler& errorHandler);
    std::shared_ptr<IndexBuilderSharedState> sharedState;

    IndexBuilderLocalBuffers localBuffers;
};

} // namespace processor
} // namespace kuzu
