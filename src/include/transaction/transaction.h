#pragma once

#include <mutex>

#include "common/types/types.h"

namespace kuzu {
namespace binder {
struct BoundAlterInfo;
}
namespace catalog {
class CatalogEntry;
class CatalogSet;
class SequenceCatalogEntry;
struct SequenceRollbackData;
} // namespace catalog
namespace main {
class ClientContext;
} // namespace main
namespace storage {
class LocalStorage;
class UndoBuffer;
class WAL;
class VersionInfo;
class UpdateInfo;
struct VectorUpdateInfo;
class ChunkedNodeGroup;
class VersionRecordHandler;
} // namespace storage
namespace transaction {
class TransactionManager;

enum class TransactionType : uint8_t { READ_ONLY, WRITE, CHECKPOINT, DUMMY, RECOVERY };

class LocalCacheManager;
class KUZU_API LocalCacheObject {
public:
    explicit LocalCacheObject(std::string key) : key{std::move(key)} {}

    virtual ~LocalCacheObject() = default;

    std::string getKey() const { return key; }

    template<typename T>
    T* cast() {
        return common::ku_dynamic_cast<T*>(this);
    }

private:
    std::string key;
};

class LocalCacheManager {
public:
    bool contains(const std::string& key) {
        std::unique_lock lck{mtx};
        return cachedObjects.contains(key);
    }
    LocalCacheObject& at(const std::string& key) {
        std::unique_lock lck{mtx};
        return *cachedObjects.at(key);
    }
    bool put(std::unique_ptr<LocalCacheObject> object);

    void remove(const std::string& key) {
        std::unique_lock lck{mtx};
        cachedObjects.erase(key);
    }

private:
    std::unordered_map<std::string, std::unique_ptr<LocalCacheObject>> cachedObjects;
    std::mutex mtx;
};

class KUZU_API Transaction {
    friend class TransactionManager;

public:
    static constexpr common::transaction_t DUMMY_TRANSACTION_ID = 0;
    static constexpr common::transaction_t DUMMY_START_TIMESTAMP = 0;
    static constexpr common::transaction_t START_TRANSACTION_ID =
        static_cast<common::transaction_t>(1) << 63;

    Transaction(main::ClientContext& clientContext, TransactionType transactionType,
        common::transaction_t transactionID, common::transaction_t startTS);

    explicit Transaction(TransactionType transactionType) noexcept;
    Transaction(TransactionType transactionType, common::transaction_t ID,
        common::transaction_t startTS) noexcept;

    ~Transaction();

    TransactionType getType() const { return type; }
    bool isReadOnly() const { return TransactionType::READ_ONLY == type; }
    bool isWriteTransaction() const { return TransactionType::WRITE == type; }
    bool isDummy() const { return TransactionType::DUMMY == type; }
    bool isRecovery() const { return TransactionType::RECOVERY == type; }
    common::transaction_t getID() const { return ID; }
    common::transaction_t getStartTS() const { return startTS; }
    common::transaction_t getCommitTS() const { return commitTS; }
    int64_t getCurrentTS() const { return currentTS; }
    main::ClientContext* getClientContext() const { return clientContext; }

    void setForceCheckpoint() { forceCheckpoint = true; }
    bool shouldAppendToUndoBuffer() const {
        return getID() > DUMMY_TRANSACTION_ID && !isReadOnly();
    }
    bool shouldLogToWAL() const;

    bool shouldForceCheckpoint() const;

    void commit(storage::WAL* wal);
    void rollback(storage::WAL* wal);

    uint64_t getEstimatedMemUsage() const;
    storage::LocalStorage* getLocalStorage() const { return localStorage.get(); }
    LocalCacheManager& getLocalCacheManager() { return localCacheManager; }
    bool isUnCommitted(common::table_id_t tableID, common::offset_t nodeOffset) const {
        return nodeOffset >= getMinUncommittedNodeOffset(tableID);
    }
    common::row_idx_t getLocalRowIdx(common::table_id_t tableID,
        common::offset_t nodeOffset) const {
        KU_ASSERT(isUnCommitted(tableID, nodeOffset));
        return nodeOffset - getMinUncommittedNodeOffset(tableID);
    }
    common::offset_t getUncommittedOffset(common::table_id_t tableID,
        common::row_idx_t localRowIdx) const {
        return getMinUncommittedNodeOffset(tableID) + localRowIdx;
    }
    common::offset_t getMinUncommittedNodeOffset(common::table_id_t tableID) const {
        // The only case that minUncommittedNodeOffsets doesn't track the given tableID is when the
        // table is newly created within the same transaction, thus the minUncommittedNodeOffsets
        // should be 0.
        return minUncommittedNodeOffsets.contains(tableID) ? minUncommittedNodeOffsets.at(tableID) :
                                                             0;
    }
    void pushCreateDropCatalogEntry(catalog::CatalogSet& catalogSet,
        catalog::CatalogEntry& catalogEntry, bool isInternal, bool skipLoggingToWAL = false);
    void pushAlterCatalogEntry(catalog::CatalogSet& catalogSet, catalog::CatalogEntry& catalogEntry,
        const binder::BoundAlterInfo& alterInfo);
    void pushSequenceChange(catalog::SequenceCatalogEntry* sequenceEntry, int64_t kCount,
        const catalog::SequenceRollbackData& data);
    void pushInsertInfo(common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
        common::row_idx_t numRows, const storage::VersionRecordHandler* versionRecordHandler) const;
    void pushDeleteInfo(common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
        common::row_idx_t numRows, const storage::VersionRecordHandler* versionRecordHandler) const;
    void pushVectorUpdateInfo(storage::UpdateInfo& updateInfo, common::idx_t vectorIdx,
        storage::VectorUpdateInfo& vectorUpdateInfo) const;

    static Transaction getDummyTransactionFromExistingOne(const Transaction& other);

private:
    Transaction(TransactionType transactionType, common::transaction_t ID,
        common::transaction_t startTS,
        std::unordered_map<common::table_id_t, common::offset_t> minUncommittedNodeOffsets);

private:
    TransactionType type;
    common::transaction_t ID;
    common::transaction_t startTS;
    common::transaction_t commitTS;
    int64_t currentTS;
    main::ClientContext* clientContext;
    std::unique_ptr<storage::LocalStorage> localStorage;
    std::unique_ptr<storage::UndoBuffer> undoBuffer;
    LocalCacheManager localCacheManager;
    bool forceCheckpoint;
    bool hasCatalogChanges;

    // For each node table, we keep track of the minimum uncommitted node offset when the
    // transaction starts. This is mainly used to assign offsets to local nodes and determine if a
    // given node is transaction local or not.
    std::unordered_map<common::table_id_t, common::offset_t> minUncommittedNodeOffsets;
};

// TODO(bmwinger): These shouldn't need to be exported
extern KUZU_API Transaction DUMMY_TRANSACTION;
extern KUZU_API Transaction DUMMY_CHECKPOINT_TRANSACTION;

} // namespace transaction
} // namespace kuzu
