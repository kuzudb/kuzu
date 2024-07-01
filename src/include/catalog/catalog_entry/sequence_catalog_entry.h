#pragma once

#include <mutex>

#include "binder/ddl/bound_create_sequence_info.h"
#include "catalog/property.h"
#include "catalog_entry.h"

namespace kuzu {
namespace binder {
struct BoundExtraCreateCatalogEntryInfo;
struct BoundAlterInfo;
} // namespace binder

namespace transaction {
class Transaction;
} // namespace transaction

namespace catalog {

struct SequenceChangeData {
    uint64_t usageCount;
    int64_t currVal;
    int64_t nextVal;
};

struct SequenceData {
    SequenceData() = default;
    explicit SequenceData(const binder::BoundCreateSequenceInfo& info)
        : usageCount{0}, nextVal{info.startWith}, currVal{info.startWith},
          increment{info.increment}, startValue{info.startWith}, minValue{info.minValue},
          maxValue{info.maxValue}, cycle{info.cycle} {}

    uint64_t usageCount;
    int64_t nextVal;
    int64_t currVal;
    int64_t increment;
    int64_t startValue;
    int64_t minValue;
    int64_t maxValue;
    bool cycle;
};

class CatalogSet;
class KUZU_API SequenceCatalogEntry : public CatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    SequenceCatalogEntry() = default;
    SequenceCatalogEntry(CatalogSet* set, common::sequence_id_t sequenceID,
        const binder::BoundCreateSequenceInfo& sequenceInfo)
        : CatalogEntry{CatalogEntryType::SEQUENCE_ENTRY, std::move(sequenceInfo.sequenceName)},
          set{set}, sequenceID{sequenceID}, sequenceData{SequenceData(sequenceInfo)} {}

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    common::sequence_id_t getSequenceID() const { return sequenceID; }
    SequenceData getSequenceData();

    //===--------------------------------------------------------------------===//
    // sequence functions
    //===--------------------------------------------------------------------===//
    int64_t currVal();
    void nextKVal(transaction::Transaction* transaction, const uint64_t& count,
        common::ValueVector& resultVector);
    void replayVal(const uint64_t& usageCount, const int64_t& currVal, const int64_t& nextVal);

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<SequenceCatalogEntry> deserialize(common::Deserializer& deserializer);
    std::string toCypher(main::ClientContext* /*clientContext*/) const override;

    binder::BoundCreateSequenceInfo getBoundCreateSequenceInfo() const;

protected:
    void copyFrom(const CatalogEntry& other) override;

protected:
    CatalogSet* set;
    common::sequence_id_t sequenceID;

private:
    std::mutex mtx;
    SequenceData sequenceData;
};

} // namespace catalog
} // namespace kuzu
