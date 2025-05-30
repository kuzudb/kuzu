#pragma once

#include "common/serializer/buffered_serializer.h"
#include "common/types/types.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

enum class IndexConstraintType : uint8_t {
    PRIMARY = 0, // Primary key index
    SECONDARY_NON_UNIQUE = 1,
};

enum class IndexDefinitionType : uint8_t {
    BUILTIN = 0,
    EXTENSION = 1,
};

struct IndexType {
    std::string typeName;
    IndexConstraintType constraintType;
    IndexDefinitionType definitionType;

    void serialize(common::Serializer& ser) const;
    static IndexType deserialize(common::Deserializer& deSer);
};

static constexpr IndexType HASH_INDEX_TYPE{"DEFAULT", IndexConstraintType::PRIMARY,
    IndexDefinitionType::BUILTIN};

struct IndexInfo {
    std::string name;
    IndexType indexType;
    common::column_id_t columnID;
    common::PhysicalTypeID keyDataType;

    void serialize(common::Serializer& ser) const;
    static IndexInfo deserialize(common::Deserializer& deSer);
};

struct IndexStorageInfo {
    IndexStorageInfo() {}
    virtual ~IndexStorageInfo() = default;
    DELETE_COPY_DEFAULT_MOVE(IndexStorageInfo);

    virtual std::shared_ptr<common::BufferedSerializer> serialize() const;

    template<typename TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
};

class Index {
public:
    Index(IndexInfo indexInfo, std::unique_ptr<IndexStorageInfo> storageInfo)
        : indexInfo{std::move(indexInfo)}, storageInfo{std::move(storageInfo)},
          storageInfoBuffer{nullptr}, storageInfoBufferSize{0}, loaded{true} {}
    Index(IndexInfo indexInfo, std::unique_ptr<uint8_t[]> storageBuffer, uint32_t storageBufferSize)
        : indexInfo{std::move(indexInfo)}, storageInfo{nullptr},
          storageInfoBuffer{std::move(storageBuffer)}, storageInfoBufferSize{storageBufferSize},
          loaded{false} {}
    virtual ~Index() = default;

    DELETE_COPY_AND_MOVE(Index);

    bool isPrimary() const {
        return indexInfo.indexType.constraintType == IndexConstraintType::PRIMARY;
    }
    bool isExtension() const {
        return indexInfo.indexType.definitionType == IndexDefinitionType::EXTENSION;
    }
    bool isLoaded() const { return loaded; }

    virtual void checkpointInMemory() {};
    virtual void checkpoint(bool forceCheckpointAll = false) { KU_UNUSED(forceCheckpointAll); }
    virtual void rollbackCheckpoint() {}

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

} // namespace storage
} // namespace kuzu
