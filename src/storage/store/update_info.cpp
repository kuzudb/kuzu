#include "storage/store/update_info.h"

#include "transaction/transaction.h"
#include <common/exception/runtime.h>

using namespace kuzu::transaction;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

void UpdateInfo::update(Transaction* transaction, offset_t offsetInChunk,
    ValueVector& values) {
    const auto vectorIdx = offsetInChunk / DEFAULT_VECTOR_CAPACITY;
    auto& vectorUpdateInfo =
        getVectorInfo(transaction, vectorIdx, offsetInChunk - vectorIdx * DEFAULT_VECTOR_CAPACITY);
    vectorUpdateInfo.data->write(&values, values.state->getSelVector()[0],
        vectorUpdateInfo.numRowsUpdated++);
}

VectorUpdateInfo& UpdateInfo::getVectorInfo(Transaction* transaction, idx_t idx,
    sel_t rowIdxInVector) {
    if (idx >= vectorsInfo.size()) {
        vectorsInfo.resize(idx + 1);
    }
    if (!vectorsInfo[idx]) {
        vectorsInfo[idx] = std::make_unique<VectorUpdateInfo>(transaction->getID());
        return *vectorsInfo[idx];
    }
    auto* current = vectorsInfo[idx].get();
    VectorUpdateInfo* info = nullptr;
    while (current) {
        if (current->version == transaction->getID()) {
            // Same transaction.
            KU_ASSERT(current->version >= Transaction::START_TRANSACTION_ID);
            info = current;
        } else if (current->version > transaction->getStartTS()) {
            // Potentially there can be conflicts. `current` can be uncommitted transaction (version
            // is transaction ID) or committed transaction started after this transaction.
            for (auto i = 0u; i < current->numRowsUpdated; i++) {
                if (current->rowsInVector[i] == rowIdxInVector) {
                    throw RuntimeException("Write-write conflict of updating the same row.");
                }
            }
        }
        current = current->next;
    }
    if (!info) {
        // Create a new version here.
        auto newInfo = std::make_unique<VectorUpdateInfo>(transaction->getID());
        vectorsInfo[idx]->next = newInfo.get();
        newInfo->prev = std::move(vectorsInfo[idx]);
        vectorsInfo[idx] = std::move(newInfo);
        info = vectorsInfo[idx].get();
    }
    return *info;
}

} // namespace storage
} // namespace kuzu
