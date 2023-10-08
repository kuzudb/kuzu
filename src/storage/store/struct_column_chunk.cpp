#include "storage/store/struct_column_chunk.h"

#include "common/exception/not_implemented.h"
#include "common/exception/parser.h"
#include "common/string_utils.h"
#include "common/types/value/nested.h"
#include "storage/store/string_column_chunk.h"
#include "storage/store/table_copy_utils.h"
#include "storage/store/var_list_column_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

StructColumnChunk::StructColumnChunk(LogicalType dataType,
    std::unique_ptr<common::CSVReaderConfig> csvReaderConfig, bool enableCompression)
    : ColumnChunk{std::move(dataType), std::move(csvReaderConfig)} {
    auto fieldTypes = StructType::getFieldTypes(&this->dataType);
    childrenChunks.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        childrenChunks[i] = ColumnChunkFactory::createColumnChunk(
            *fieldTypes[i], enableCompression, this->csvReaderConfig.get());
    }
}

void StructColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto otherStructChunk = dynamic_cast<StructColumnChunk*>(other);
    assert(other->getNumChildren() == getNumChildren());
    nullChunk->append(
        other->getNullChunk(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    for (auto i = 0u; i < getNumChildren(); i++) {
        childrenChunks[i]->append(otherStructChunk->childrenChunks[i].get(), startPosInOtherChunk,
            startPosInChunk, numValuesToAppend);
    }
    numValues += numValuesToAppend;
}

void StructColumnChunk::append(common::ValueVector* vector, common::offset_t startPosInChunk) {
    auto numFields = StructType::getNumFields(&dataType);
    for (auto i = 0u; i < numFields; i++) {
        childrenChunks[i]->append(StructVector::getFieldVector(vector, i).get(), startPosInChunk);
    }
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        nullChunk->setNull(
            startPosInChunk + i, vector->isNull(vector->state->selVector->selectedPositions[i]));
    }
    numValues += vector->state->selVector->selectedSize;
}

void StructColumnChunk::write(const Value& val, uint64_t posToWrite) {
    assert(val.getDataType()->getPhysicalType() == PhysicalTypeID::STRUCT);
    auto numElements = NestedVal::getChildrenSize(&val);
    for (auto i = 0u; i < numElements; i++) {
        childrenChunks[i]->write(*NestedVal::getChildVal(&val, i), posToWrite);
    }
}

} // namespace storage
} // namespace kuzu
