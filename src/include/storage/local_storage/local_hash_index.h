#pragma once

#include "common/string_utils.h"
#include "common/type_utils.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/index/in_mem_hash_index.h"

namespace kuzu {
namespace storage {

// NOTE: This is a dummy base class to hide the templation from LocalHashIndex, while allows casting
// to templated HashIndexLocalStorage. Same for OnDiskHashIndex.
class BaseHashIndexLocalStorage {
public:
    virtual ~BaseHashIndexLocalStorage() = default;
    virtual uint64_t getEstimatedMemUsage() = 0;
};

enum class HashIndexLocalLookupState : uint8_t { KEY_FOUND, KEY_DELETED, KEY_NOT_EXIST };

// Local storage consists of two in memory indexes. One (localInsertionIndex) is to keep track of
// all newly inserted entries, and the other (localDeletionIndex) is to keep track of newly deleted
// entries (not available in localInsertionIndex). We assume that in a transaction, the insertions
// and deletions are very small, thus they can be kept in memory.
template<typename T>
class HashIndexLocalStorage final : public BaseHashIndexLocalStorage {
public:
    explicit HashIndexLocalStorage(MemoryManager& memoryManager, OverflowFileHandle* handle)
        : localDeletions{}, localInsertions{memoryManager, handle} {}
    using OwnedKeyType = std::conditional_t<std::same_as<T, common::ku_string_t>, std::string, T>;
    using Key = std::conditional_t<std::same_as<T, common::ku_string_t>, std::string_view, T>;
    HashIndexLocalLookupState lookup(Key key, common::offset_t& result, visible_func isVisible) {
        if (localDeletions.contains(key)) {
            return HashIndexLocalLookupState::KEY_DELETED;
        }
        if (localInsertions.lookup(key, result, isVisible)) {
            return HashIndexLocalLookupState::KEY_FOUND;
        }
        return HashIndexLocalLookupState::KEY_NOT_EXIST;
    }

    void deleteKey(Key key) {
        if (!localInsertions.deleteKey(key)) {
            localDeletions.insert(static_cast<OwnedKeyType>(key));
        }
    }

    bool insert(Key key, common::offset_t value, visible_func isVisible) {
        auto iter = localDeletions.find(key);
        if (iter != localDeletions.end()) {
            localDeletions.erase(iter);
        }
        return localInsertions.append(key, value, isVisible);
    }

    void reserveSpaceForAppend(uint32_t numNewEntries) {
        localInsertions.reserveSpaceForAppend(numNewEntries);
    }

    bool append(Key key, common::offset_t value, visible_func isVisible) {
        return localInsertions.append(key, value, isVisible);
    }

    size_t append(const IndexBuffer<OwnedKeyType>& buffer, uint64_t bufferOffset,
        visible_func isVisible) {
        return localInsertions.append(buffer, bufferOffset, isVisible);
    }

    bool hasUpdates() const { return !(localInsertions.empty() && localDeletions.empty()); }

    int64_t getNetInserts() const {
        return static_cast<int64_t>(localInsertions.size()) - localDeletions.size();
    }

    void clear() {
        localInsertions.clear();
        localDeletions.clear();
    }

    void applyLocalChanges(const std::function<void(Key)>& deleteOp,
        const std::function<void(const InMemHashIndex<T>&)>& insertOp) {
        for (auto& key : localDeletions) {
            deleteOp(key);
        }
        insertOp(localInsertions);
    }

    void reserveInserts(uint64_t newEntries) { localInsertions.reserve(newEntries); }

    const InMemHashIndex<T>& getInsertions() { return localInsertions; }

    uint64_t getEstimatedMemUsage() override {
        return localInsertions.getEstimatedMemUsage() +
               localDeletions.size() * sizeof(OwnedKeyType);
    }

private:
    // When the storage type is string, allow the key type to be string_view with a custom hash
    // function
    using hash_function = std::conditional_t<std::is_same_v<OwnedKeyType, std::string>,
        common::StringUtils::string_hash, std::hash<T>>;
    std::unordered_set<OwnedKeyType, hash_function, std::equal_to<>> localDeletions;
    InMemHashIndex<T> localInsertions;
};

class LocalHashIndex {
public:
    explicit LocalHashIndex(MemoryManager& memoryManager, common::PhysicalTypeID keyDataTypeID,
        OverflowFileHandle* overflowFileHandle)
        : keyDataTypeID{keyDataTypeID} {
        common::TypeUtils::visit(
            keyDataTypeID,
            [&](common::ku_string_t) {
                localIndex = std::make_unique<HashIndexLocalStorage<common::ku_string_t>>(
                    memoryManager, overflowFileHandle);
            },
            [&]<common::HashablePrimitive T>(T) {
                localIndex = std::make_unique<HashIndexLocalStorage<T>>(memoryManager, nullptr);
            },
            [&](auto) { KU_UNREACHABLE; });
    }

    common::offset_t lookup(const common::ValueVector& keyVector, common::sel_t pos,
        visible_func isVisible) {
        common::offset_t result = common::INVALID_OFFSET;
        common::TypeUtils::visit(
            keyDataTypeID,
            [&]<common::IndexHashable T>(
                T) { result = lookup(keyVector.getValue<T>(pos), isVisible); },
            [](auto) { KU_UNREACHABLE; });
        return result;
    }

    common::offset_t lookup(const common::ValueVector& keyVector, visible_func isVisible) {
        KU_ASSERT(keyVector.state->getSelVector().getSelSize() == 1);
        auto pos = keyVector.state->getSelVector().getSelectedPositions()[0];
        return lookup(keyVector, pos, isVisible);
    }

    common::offset_t lookup(const common::ku_string_t key, visible_func isVisible) {
        return lookup(key.getAsStringView(), isVisible);
    }
    template<common::IndexHashable T>
    common::offset_t lookup(T key, visible_func isVisible) {
        common::offset_t result = common::INVALID_OFFSET;
        common::ku_dynamic_cast<HashIndexLocalStorage<HashIndexType<T>>*>(localIndex.get())
            ->lookup(key, result, isVisible);
        return result;
    }

    bool insert(const common::ValueVector& keyVector, common::offset_t startNodeOffset,
        visible_func isVisible) {
        common::length_t numInserted = 0;
        common::TypeUtils::visit(
            keyDataTypeID,
            [&]<common::IndexHashable T>(T) {
                for (auto i = 0u; i < keyVector.state->getSelVector().getSelSize(); i++) {
                    const auto pos = keyVector.state->getSelVector().getSelectedPositions()[i];
                    numInserted +=
                        insert(keyVector.getValue<T>(pos), startNodeOffset + i, isVisible);
                }
            },
            [](auto) { KU_UNREACHABLE; });
        return numInserted == keyVector.state->getSelVector().getSelSize();
    }

    bool insert(const common::ku_string_t key, common::offset_t value, visible_func isVisible) {
        return insert(key.getAsStringView(), value, isVisible);
    }
    template<common::IndexHashable T>
    bool insert(T key, common::offset_t value, visible_func isVisible) {
        KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
        return common::ku_dynamic_cast<HashIndexLocalStorage<HashIndexType<T>>*>(localIndex.get())
            ->insert(key, value, isVisible);
    }

    void delete_(const common::ValueVector& keyVector) {
        common::TypeUtils::visit(
            keyDataTypeID,
            [&]<common::IndexHashable T>(T) {
                for (auto i = 0u; i < keyVector.state->getSelVector().getSelSize(); i++) {
                    const auto pos = keyVector.state->getSelVector().getSelectedPositions()[i];
                    delete_(keyVector.getValue<T>(pos));
                }
            },
            [](auto) { KU_UNREACHABLE; });
    }

    void delete_(const common::ku_string_t key) { delete_(key.getAsStringView()); }
    template<common::IndexHashable T>
    void delete_(T key) {
        KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
        common::ku_dynamic_cast<HashIndexLocalStorage<HashIndexType<T>>*>(localIndex.get())
            ->deleteKey(key);
    }

    uint64_t getEstimatedMemUsage() { return localIndex->getEstimatedMemUsage(); }

private:
    common::PhysicalTypeID keyDataTypeID;
    std::unique_ptr<BaseHashIndexLocalStorage> localIndex;
};

} // namespace storage
} // namespace kuzu
