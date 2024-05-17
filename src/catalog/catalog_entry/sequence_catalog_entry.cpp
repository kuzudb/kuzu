#include "catalog/catalog_entry/sequence_catalog_entry.h"

#include "binder/ddl/bound_create_sequence_info.h"
#include "common/exception/catalog.h"
#include "common/exception/overflow.h"
#include "function/arithmetic/add.h"

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

// referenced from DuckDB
int64_t SequenceCatalogEntry::nextVal() {
    std::lock_guard<std::mutex> lck(mtx);
    int64_t result;
    result = sequenceData.nextVal;
    bool overflow = false;
    try {
        function::Add::operation(sequenceData.nextVal, sequenceData.increment,
            sequenceData.nextVal);
    } catch (const OverflowException& e) {
        overflow = true;
    }
    if (sequenceData.cycle) {
        if (overflow) {
            sequenceData.nextVal =
                sequenceData.increment < 0 ? sequenceData.maxValue : sequenceData.minValue;
        } else if (sequenceData.nextVal < sequenceData.minValue) {
            sequenceData.nextVal = sequenceData.maxValue;
        } else if (sequenceData.nextVal > sequenceData.maxValue) {
            sequenceData.nextVal = sequenceData.minValue;
        }
    } else {
        if (result < sequenceData.minValue || (overflow && sequenceData.increment < 0)) {
            throw CatalogException("nextval: reached minimum value of sequence \"" + name + "\" " +
                                   std::to_string(sequenceData.minValue));
        }
        if (result > sequenceData.maxValue || overflow) {
            throw CatalogException("nextval: reached maximum value of sequence \"" + name + "\" " +
                                   std::to_string(sequenceData.maxValue));
        }
    }
    sequenceData.currVal = result;
    sequenceData.usageCount++;
    return result;
}

void SequenceCatalogEntry::replayVal(uint64_t usageCount, int64_t currVal, int64_t nextVal) {
    std::lock_guard<std::mutex> lck(mtx);
    if (usageCount > sequenceData.usageCount) {
        sequenceData.usageCount = usageCount;
        sequenceData.currVal = currVal;
        sequenceData.nextVal = nextVal;
    }
}

void SequenceCatalogEntry::serialize(common::Serializer& serializer) const {
    CatalogEntry::serialize(serializer);
    serializer.write(sequenceID);
    serializer.write(sequenceData.usageCount);
    serializer.write(sequenceData.nextVal);
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
    int64_t nextVal;
    int64_t currVal;
    int64_t increment;
    int64_t startValue;
    int64_t minValue;
    int64_t maxValue;
    bool cycle;
    deserializer.deserializeValue(sequenceID);
    deserializer.deserializeValue(usageCount);
    deserializer.deserializeValue(nextVal);
    deserializer.deserializeValue(currVal);
    deserializer.deserializeValue(increment);
    deserializer.deserializeValue(startValue);
    deserializer.deserializeValue(minValue);
    deserializer.deserializeValue(maxValue);
    deserializer.deserializeValue(cycle);
    auto result = std::make_unique<SequenceCatalogEntry>();
    result->sequenceID = sequenceID;
    result->sequenceData.usageCount = usageCount;
    result->sequenceData.nextVal = nextVal;
    result->sequenceData.currVal = currVal;
    result->sequenceData.increment = increment;
    result->sequenceData.startValue = startValue;
    result->sequenceData.minValue = minValue;
    result->sequenceData.maxValue = maxValue;
    result->sequenceData.cycle = cycle;
    return result;
}

void SequenceCatalogEntry::copyFrom(const CatalogEntry& other) {
    CatalogEntry::copyFrom(other);
    auto& otherSequence = ku_dynamic_cast<const CatalogEntry&, const SequenceCatalogEntry&>(other);
    sequenceID = otherSequence.sequenceID;
}

binder::BoundCreateSequenceInfo SequenceCatalogEntry::getBoundCreateSequenceInfo() const {
    return BoundCreateSequenceInfo(name, sequenceData.startValue, sequenceData.increment,
        sequenceData.minValue, sequenceData.maxValue, sequenceData.cycle);
}

} // namespace catalog
} // namespace kuzu
