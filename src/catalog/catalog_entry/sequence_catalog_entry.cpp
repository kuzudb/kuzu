#include "catalog/catalog_entry/sequence_catalog_entry.h"

#include "binder/ddl/bound_create_sequence_info.h"
#include "common/exception/catalog.h"
#include "common/exception/overflow.h"
#include "common/serializer/deserializer.h"
#include "common/vector/value_vector.h"
#include "function/arithmetic/add.h"
#include "transaction/transaction.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace catalog {

SequenceData SequenceCatalogEntry::getSequenceData() {
    std::lock_guard<std::mutex> lck(mtx);
    return sequenceData;
}

int64_t SequenceCatalogEntry::currVal() {
    std::lock_guard<std::mutex> lck(mtx);
    int64_t result;
    if (sequenceData.usageCount == 0) {
        throw CatalogException(
            "currval: sequence \"" + name +
            "\" is not yet defined. To define the sequence, call nextval first.");
    }
    result = sequenceData.currVal;
    return result;
}

void SequenceCatalogEntry::nextValNoLock() {
    if (sequenceData.usageCount == 0) {
        // initialization of sequence
        sequenceData.usageCount++;
        return;
    }
    bool overflow = false;
    auto next = sequenceData.currVal;
    try {
        function::Add::operation(next, sequenceData.increment, next);
    } catch (const OverflowException& e) {
        overflow = true;
    }
    if (sequenceData.cycle) {
        if (overflow) {
            next = sequenceData.increment < 0 ? sequenceData.maxValue : sequenceData.minValue;
        } else if (next < sequenceData.minValue) {
            next = sequenceData.maxValue;
        } else if (next > sequenceData.maxValue) {
            next = sequenceData.minValue;
        }
    } else {
        bool minError = overflow ? sequenceData.increment < 0 : next < sequenceData.minValue;
        bool maxError = overflow ? sequenceData.increment > 0 : next > sequenceData.maxValue;
        if (minError) {
            throw CatalogException("nextval: reached minimum value of sequence \"" + name + "\" " +
                                   std::to_string(sequenceData.minValue));
        }
        if (maxError) {
            throw CatalogException("nextval: reached maximum value of sequence \"" + name + "\" " +
                                   std::to_string(sequenceData.maxValue));
        }
    }
    sequenceData.currVal = next;
    sequenceData.usageCount++;
}

// referenced from DuckDB
void SequenceCatalogEntry::nextKVal(transaction::Transaction* transaction, const uint64_t& count) {
    KU_ASSERT(count > 0);
    SequenceRollbackData rollbackData;
    {
        std::lock_guard<std::mutex> lck(mtx);
        rollbackData = SequenceRollbackData{sequenceData.usageCount, sequenceData.currVal};
        for (auto i = 0ul; i < count; i++) {
            nextValNoLock();
        }
    }
    transaction->pushSequenceChange(this, count, rollbackData);
}

void SequenceCatalogEntry::nextKVal(transaction::Transaction* transaction, const uint64_t& count,
    common::ValueVector& resultVector) {
    KU_ASSERT(count > 0);
    SequenceRollbackData rollbackData;
    {
        std::lock_guard<std::mutex> lck(mtx);
        rollbackData = SequenceRollbackData{sequenceData.usageCount, sequenceData.currVal};
        for (auto i = 0ul; i < count; i++) {
            nextValNoLock();
            resultVector.setValue(i, sequenceData.currVal);
        }
    }
    transaction->pushSequenceChange(this, count, rollbackData);
}

void SequenceCatalogEntry::rollbackVal(const uint64_t& usageCount, const int64_t& currVal) {
    std::lock_guard<std::mutex> lck(mtx);
    sequenceData.usageCount = usageCount;
    sequenceData.currVal = currVal;
}

void SequenceCatalogEntry::serialize(common::Serializer& serializer) const {
    CatalogEntry::serialize(serializer);
    serializer.write(sequenceID);
    serializer.write(sequenceData.usageCount);
    serializer.write(sequenceData.currVal);
    serializer.write(sequenceData.increment);
    serializer.write(sequenceData.startValue);
    serializer.write(sequenceData.minValue);
    serializer.write(sequenceData.maxValue);
    serializer.write(sequenceData.cycle);
}

std::unique_ptr<SequenceCatalogEntry> SequenceCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
    common::sequence_id_t sequenceID;
    uint64_t usageCount;
    int64_t currVal;
    int64_t increment;
    int64_t startValue;
    int64_t minValue;
    int64_t maxValue;
    bool cycle;
    deserializer.deserializeValue(sequenceID);
    deserializer.deserializeValue(usageCount);
    deserializer.deserializeValue(currVal);
    deserializer.deserializeValue(increment);
    deserializer.deserializeValue(startValue);
    deserializer.deserializeValue(minValue);
    deserializer.deserializeValue(maxValue);
    deserializer.deserializeValue(cycle);
    auto result = std::make_unique<SequenceCatalogEntry>();
    result->sequenceID = sequenceID;
    result->sequenceData.usageCount = usageCount;
    result->sequenceData.currVal = currVal;
    result->sequenceData.increment = increment;
    result->sequenceData.startValue = startValue;
    result->sequenceData.minValue = minValue;
    result->sequenceData.maxValue = maxValue;
    result->sequenceData.cycle = cycle;
    return result;
}

std::string SequenceCatalogEntry::toCypher(main::ClientContext* /*clientContext*/) const {
    return common::stringFormat(
        "CREATE SEQUENCE IF NOT EXISTS {} START {} INCREMENT {} MINVALUE {} MAXVALUE {} {} CYCLE;\n"
        "RETURN nextval('{}');",
        getName(), sequenceData.currVal, sequenceData.increment, sequenceData.minValue,
        sequenceData.maxValue, sequenceData.cycle ? "" : "NO", getName());
}

void SequenceCatalogEntry::copyFrom(const CatalogEntry& other) {
    CatalogEntry::copyFrom(other);
    auto& otherSequence = ku_dynamic_cast<const CatalogEntry&, const SequenceCatalogEntry&>(other);
    sequenceID = otherSequence.sequenceID;
}

binder::BoundCreateSequenceInfo SequenceCatalogEntry::getBoundCreateSequenceInfo() const {
    return BoundCreateSequenceInfo(name, sequenceData.startValue, sequenceData.increment,
        sequenceData.minValue, sequenceData.maxValue, sequenceData.cycle,
        ConflictAction::ON_CONFLICT_THROW);
}

} // namespace catalog
} // namespace kuzu
