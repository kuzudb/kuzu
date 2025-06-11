#pragma once

#include <functional>

#include "common/serializer/buffered_serializer.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "in_mem_hash_index.h"
#include <span>

namespace kuzu::storage {
class StorageManager;
}
namespace kuzu {
namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {

enum class KUZU_API IndexConstraintType : uint8_t {
    PRIMARY = 0,              // Primary key index
    SECONDARY_NON_UNIQUE = 1, // Secondary index that is not unique
};

enum class KUZU_API IndexDefinitionType : uint8_t {
    BUILTIN = 0,
    EXTENSION = 1,
};

class Index;
struct IndexInfo;
using index_load_func_t = std::function<std::unique_ptr<Index>(main::ClientContext* context,
    StorageManager* storageManager, IndexInfo, std::span<uint8_t>)>;

struct KUZU_API IndexType {
    std::string typeName;
    IndexConstraintType constraintType;
    IndexDefinitionType definitionType;
    index_load_func_t loadFunc;

    IndexType(std::string typeName, IndexConstraintType constraintType,
        IndexDefinitionType definitionType, index_load_func_t loadFunc)
        : typeName{std::move(typeName)}, constraintType{constraintType},
          definitionType{definitionType}, loadFunc{std::move(loadFunc)} {}
};

struct KUZU_API IndexInfo {
    std::string name;
    std::string indexType;
    common::table_id_t tableID;
    std::vector<common::column_id_t> columnIDs;
    std::vector<common::PhysicalTypeID> keyDataTypes;
    bool isPrimary;
    bool isBuiltin;

    IndexInfo(std::string name, std::string indexType, common::table_id_t tableID,
        std::vector<common::column_id_t> columnIDs,
        std::vector<common::PhysicalTypeID> keyDataTypes, bool isPrimary, bool isBuiltin)
        : name{std::move(name)}, indexType{std::move(indexType)}, tableID{tableID},
          columnIDs{std::move(columnIDs)}, keyDataTypes{std::move(keyDataTypes)},
          isPrimary{isPrimary}, isBuiltin{isBuiltin} {}

    void serialize(common::Serializer& ser) const;
    static IndexInfo deserialize(common::Deserializer& deSer);
};

struct KUZU_API IndexStorageInfo {
    IndexStorageInfo() {}
    virtual ~IndexStorageInfo();
    DELETE_COPY_DEFAULT_MOVE(IndexStorageInfo);

    virtual std::shared_ptr<common::BufferedSerializer> serialize() const;

    template<typename TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }

    template<typename TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }
};

class KUZU_API Index {
public:
    struct InsertState {
        virtual ~InsertState();
        template<typename TARGET>
        TARGET& cast() {
            return common::ku_dynamic_cast<TARGET&>(*this);
        }
    };

    struct DeleteState {
        virtual ~DeleteState();
        template<typename TARGET>
        TARGET& cast() {
            return common::ku_dynamic_cast<TARGET&>(*this);
        }
    };

    Index(IndexInfo indexInfo, std::unique_ptr<IndexStorageInfo> storageInfo)
        : indexInfo{std::move(indexInfo)}, storageInfo{std::move(storageInfo)},
          storageInfoBuffer{nullptr}, storageInfoBufferSize{0}, loaded{true} {}
    Index(IndexInfo indexInfo, std::unique_ptr<uint8_t[]> storageBuffer, uint32_t storageBufferSize)
        : indexInfo{std::move(indexInfo)}, storageInfo{nullptr},
          storageInfoBuffer{std::move(storageBuffer)}, storageInfoBufferSize{storageBufferSize},
          loaded{false} {}
    virtual ~Index();

    DELETE_COPY_AND_MOVE(Index);

    bool isPrimary() const { return indexInfo.isPrimary; }
    bool isExtension() const { return indexInfo.isBuiltin; }
    bool isLoaded() const { return loaded; }
    std::string getName() const { return indexInfo.name; }
    IndexInfo getIndexInfo() const { return indexInfo; }

    virtual std::unique_ptr<InsertState> initInsertState(transaction::Transaction* transaction,
        MemoryManager* mm, visible_func isVisible) = 0;
    virtual void insert(transaction::Transaction* transaction,
        const common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& indexVectors, InsertState& insertState) = 0;
    virtual std::unique_ptr<DeleteState> initDeleteState(
        const transaction::Transaction* transaction, MemoryManager* mm, visible_func isVisible) = 0;
    virtual void delete_(transaction::Transaction* transaction,
        const common::ValueVector& nodeIDVector, DeleteState& deleteState) = 0;
    virtual void commitInsert(transaction::Transaction* transaction,
        const common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& indexVectors, InsertState& insertState) = 0;

    virtual void checkpointInMemory() {
        // DO NOTHING.
    };
    virtual void checkpoint(main::ClientContext*, bool forceCheckpointAll = false) {
        KU_UNUSED(forceCheckpointAll);
    }
    virtual void rollbackCheckpoint() {
        // DO NOTHING.
    }

    std::span<uint8_t> getStorageBuffer() const {
        KU_ASSERT(!loaded);
        return std::span(storageInfoBuffer.get(), storageInfoBufferSize);
    }
    const IndexStorageInfo& getStorageInfo() const { return *storageInfo; }
    virtual void serialize(common::Serializer& ser) const;

    template<typename TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }

protected:
    IndexInfo indexInfo;
    std::unique_ptr<IndexStorageInfo> storageInfo;
    std::unique_ptr<uint8_t[]> storageInfoBuffer;
    uint64_t storageInfoBufferSize;
    bool loaded;
};

class IndexHolder {
public:
    explicit IndexHolder(std::unique_ptr<Index> loadedIndex);
    IndexHolder(IndexInfo indexInfo, std::unique_ptr<uint8_t[]> storageInfoBuffer,
        uint32_t storageInfoBufferSize);

    std::string getName() const { return indexInfo.name; }
    bool isLoaded() const { return loaded; }

    void serialize(common::Serializer& ser) const;
    KUZU_API void load(main::ClientContext* context, StorageManager* storageManager);
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    void checkpoint(main::ClientContext* context) {
        if (loaded) {
            KU_ASSERT(index);
            index->checkpoint(context);
        }
    }
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    void rollbackCheckpoint() {
        if (loaded) {
            KU_ASSERT(index);
            index->rollbackCheckpoint();
        }
    }

    Index* getIndex() const {
        KU_ASSERT(index);
        return index.get();
    }

private:
    IndexInfo indexInfo;
    std::unique_ptr<uint8_t[]> storageInfoBuffer;
    uint64_t storageInfoBufferSize;
    bool loaded;

    // Loaded index structure.
    std::unique_ptr<Index> index;
};

} // namespace storage
} // namespace kuzu
