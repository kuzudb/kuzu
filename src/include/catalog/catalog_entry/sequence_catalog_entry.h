#pragma once

#include <mutex>

#include "binder/ddl/bound_create_sequence_info.h"
#include "catalog_entry.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace common {
class ValueVector;
}

namespace binder {
struct BoundExtraCreateCatalogEntryInfo;
struct BoundAlterInfo;
} // namespace binder

namespace transaction {
class Transaction;
} // namespace transaction

namespace catalog {

struct SequenceRollbackData {
    uint64_t usageCount;
    int64_t currVal;
};

struct SequenceData {
    SequenceData() = default;
    explicit SequenceData(const binder::BoundCreateSequenceInfo& info)
        : usageCount{0}, currVal{info.startWith}, increment{info.increment},
          startValue{info.startWith}, minValue{info.minValue}, maxValue{info.maxValue},
          cycle{info.cycle} {}

    uint64_t usageCount;
    int64_t currVal;
    int64_t increment;
    int64_t startValue;
    int64_t minValue;
    int64_t maxValue;
    bool cycle;
};

class CatalogSet;
class KUZU_API SequenceCatalogEntry final : public CatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    SequenceCatalogEntry() : sequenceData{} {}
    explicit SequenceCatalogEntry(const binder::BoundCreateSequenceInfo& sequenceInfo)
        : CatalogEntry{CatalogEntryType::SEQUENCE_ENTRY, std::move(sequenceInfo.sequenceName)},
          sequenceData{SequenceData(sequenceInfo)} {}

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    SequenceData getSequenceData();

    //===--------------------------------------------------------------------===//
    // sequence functions
    //===--------------------------------------------------------------------===//
    int64_t currVal();
    void nextKVal(const transaction::Transaction* transaction, const uint64_t& count);
    void nextKVal(const transaction::Transaction* transaction, const uint64_t& count,
        common::ValueVector& resultVector);
    void rollbackVal(const uint64_t& usageCount, const int64_t& currVal);

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<SequenceCatalogEntry> deserialize(common::Deserializer& deserializer);
    std::string toCypher(main::ClientContext* /*clientContext*/) const override;

    binder::BoundCreateSequenceInfo getBoundCreateSequenceInfo() const;

private:
    void nextValNoLock();

private:
    std::mutex mtx;
    SequenceData sequenceData;
};

} // namespace catalog
} // namespace kuzu
