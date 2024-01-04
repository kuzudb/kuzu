#include "processor/operator/persistent/index_builder.h"

#include <thread>

#include "common/cast.h"
#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "storage/store/string_column_chunk.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::storage;

IndexBuilderGlobalQueues::IndexBuilderGlobalQueues(PrimaryKeyIndexBuilder* pkIndex)
    : pkIndex(pkIndex) {
    if (this->pkIndex->keyTypeID() == LogicalTypeID::STRING) {
        queues.emplace<StringQueues>();
    } else {
        queues.emplace<IntQueues>();
    }
}

const size_t SHOULD_FLUSH_QUEUE_SIZE = 32;

void IndexBuilderGlobalQueues::insert(size_t index, StringBuffer elem) {
    auto& stringQueues = std::get<StringQueues>(queues);
    stringQueues[index].push(std::move(elem));
    if (stringQueues[index].approxSize() < SHOULD_FLUSH_QUEUE_SIZE) {
        return;
    }
    maybeConsumeIndex(index);
}

void IndexBuilderGlobalQueues::insert(size_t index, IntBuffer elem) {
    auto& intQueues = std::get<IntQueues>(queues);
    intQueues[index].push(std::move(elem));
    if (intQueues[index].approxSize() < SHOULD_FLUSH_QUEUE_SIZE) {
        return;
    }
    maybeConsumeIndex(index);
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
    std::unique_lock lck{mutexes[index], std::adopt_lock};

    auto* stringQueues = std::get_if<StringQueues>(&queues);
    if (stringQueues) {
        StringBuffer elem;
        while ((*stringQueues)[index].pop(elem)) {
            for (auto [key, value] : elem) {
                if (!pkIndex->appendWithIndexPos(key.c_str(), value, index)) {
                    throw CopyException(ExceptionMessage::existedPKException(std::move(key)));
                }
            }
        }
    } else {
        auto& intQueues = std::get<IntQueues>(queues);
        IntBuffer elem;
        while (intQueues[index].pop(elem)) {
            for (auto [key, value] : elem) {
                if (!pkIndex->appendWithIndexPos(key, value, index)) {
                    throw CopyException(ExceptionMessage::existedPKException(std::to_string(key)));
                }
            }
        }
    }
}

void IndexBuilderGlobalQueues::flushToDisk() const {
    pkIndex->flush();
}

IndexBuilderLocalBuffers::IndexBuilderLocalBuffers(IndexBuilderGlobalQueues& globalQueues)
    : globalQueues(&globalQueues) {
    if (globalQueues.pkTypeID() == LogicalTypeID::STRING) {
        stringBuffers = std::make_unique<StringBuffers>();
    } else {
        intBuffers = std::make_unique<IntBuffers>();
    }
}

void IndexBuilderLocalBuffers::insert(std::string key, common::offset_t value) {
    auto indexPos = getHashIndexPosition(key.c_str());
    if ((*stringBuffers)[indexPos].full()) {
        globalQueues->insert(indexPos, std::move((*stringBuffers)[indexPos]));
    }
    (*stringBuffers)[indexPos].push_back(std::make_pair(key, value));
}

void IndexBuilderLocalBuffers::insert(int64_t key, common::offset_t value) {
    auto indexPos = getHashIndexPosition(key);
    if ((*intBuffers)[indexPos].full()) {
        globalQueues->insert(indexPos, std::move((*intBuffers)[indexPos]));
    }
    (*intBuffers)[indexPos].push_back(std::make_pair(key, value));
}

void IndexBuilderLocalBuffers::flush() {
    if (globalQueues->pkTypeID() == LogicalTypeID::STRING) {
        for (auto i = 0u; i < stringBuffers->size(); i++) {
            globalQueues->insert(i, std::move((*stringBuffers)[i]));
        }
    } else {
        for (auto i = 0u; i < intBuffers->size(); i++) {
            globalQueues->insert(i, std::move((*intBuffers)[i]));
        }
    }
}

IndexBuilder::IndexBuilder(std::shared_ptr<IndexBuilderSharedState> sharedState)
    : sharedState(std::move(sharedState)), localBuffers(this->sharedState->globalQueues) {}

void IndexBuilderSharedState::quitProducer() {
    if (producers.fetch_sub(1, std::memory_order_relaxed) == 1) {
        done.store(true, std::memory_order_relaxed);
    }
}

void IndexBuilder::insert(ColumnChunk* chunk, offset_t nodeOffset, offset_t numNodes) {
    checkNonNullConstraint(chunk->getNullChunk(), numNodes);

    switch (chunk->getDataType()->getPhysicalType()) {
    case PhysicalTypeID::INT64: {
        for (auto i = 0u; i < numNodes; i++) {
            auto value = chunk->getValue<int64_t>(i);
            localBuffers.insert(value, nodeOffset + i);
        }
    } break;
    case PhysicalTypeID::STRING: {
        auto stringColumnChunk = ku_dynamic_cast<ColumnChunk*, StringColumnChunk*>(chunk);
        for (auto i = 0u; i < numNodes; i++) {
            auto value = stringColumnChunk->getValue<std::string>(i);
            localBuffers.insert(std::move(value), nodeOffset + i);
        }
    } break;
    default: {
        throw CopyException(ExceptionMessage::invalidPKType(chunk->getDataType()->toString()));
    }
    }
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

void IndexBuilder::checkNonNullConstraint(NullColumnChunk* nullChunk, offset_t numNodes) {
    for (auto i = 0u; i < numNodes; i++) {
        if (nullChunk->isNull(i)) {
            throw CopyException(ExceptionMessage::nullPKException());
        }
    }
}

} // namespace processor
} // namespace kuzu
