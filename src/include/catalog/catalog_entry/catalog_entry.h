#pragma once

#include <string>

#include "catalog_entry_type.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/types/types.h"

namespace kuzu {
namespace main {
class ClientContext;
} // namespace main

namespace catalog {

class KUZU_API CatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructor & destructor
    //===--------------------------------------------------------------------===//
    CatalogEntry() = default;
    CatalogEntry(CatalogEntryType type, std::string name)
        : type{type}, name{std::move(name)}, timestamp{common::INVALID_TRANSACTION} {}
    DELETE_COPY_DEFAULT_MOVE(CatalogEntry);
    virtual ~CatalogEntry() = default;

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    CatalogEntryType getType() const { return type; }
    void rename(std::string name_) { this->name = std::move(name_); }
    std::string getName() const { return name; }
    common::transaction_t getTimestamp() const { return timestamp; }
    void setTimestamp(common::transaction_t timestamp_) { this->timestamp = timestamp_; }
    bool isDeleted() const { return deleted; }
    void setDeleted(bool deleted_) { this->deleted = deleted_; }
    CatalogEntry* getPrev() const {
        KU_ASSERT(prev);
        return prev.get();
    }
    std::unique_ptr<CatalogEntry> movePrev() {
        if (this->prev) {
            this->prev->setNext(nullptr);
        }
        return std::move(prev);
    }
    void setPrev(std::unique_ptr<CatalogEntry> prev_) {
        this->prev = std::move(prev_);
        if (this->prev) {
            this->prev->setNext(this);
        }
    }
    CatalogEntry* getNext() const { return next; }
    void setNext(CatalogEntry* next_) { this->next = next_; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    virtual void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<CatalogEntry> deserialize(common::Deserializer& deserializer);

    virtual std::string toCypher(main::ClientContext* /*clientContext*/) const { KU_UNREACHABLE; }

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const CatalogEntry&, const TARGET&>(*this);
    }
    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const CatalogEntry*, const TARGET*>(this);
    }
    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<CatalogEntry*, TARGET*>(this);
    }

protected:
    virtual void copyFrom(const CatalogEntry& other);

protected:
    CatalogEntryType type;
    std::string name;
    common::transaction_t timestamp;
    bool deleted = false;
    // Older versions.
    std::unique_ptr<CatalogEntry> prev;
    // Newer versions.
    CatalogEntry* next = nullptr;
};

} // namespace catalog
} // namespace kuzu
