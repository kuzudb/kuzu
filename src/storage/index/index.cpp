#include "storage/index/index.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "storage/index/hash_index.h"

namespace kuzu {
namespace storage {

void IndexType::serialize(common::Serializer& ser) const {
    ser.write<std::string>(typeName);
    ser.write<IndexConstraintType>(constraintType);
    ser.write<IndexDefinitionType>(definitionType);
}

IndexType IndexType::deserialize(common::Deserializer& deSer) {
    std::string typeName;
    auto constraintType = IndexConstraintType::PRIMARY;
    auto definitionType = IndexDefinitionType::BUILTIN;
    deSer.deserializeValue(typeName);
    deSer.deserializeValue<IndexConstraintType>(constraintType);
    deSer.deserializeValue<IndexDefinitionType>(definitionType);
    return IndexType{std::move(typeName), constraintType, definitionType};
}

void IndexInfo::serialize(common::Serializer& ser) const {
    ser.write<std::string>(name);
    indexType.serialize(ser);
    ser.write<common::column_id_t>(columnID);
    ser.write<common::PhysicalTypeID>(keyDataType);
}

IndexInfo IndexInfo::deserialize(common::Deserializer& deSer) {
    std::string name;
    common::column_id_t columnID = common::INVALID_COLUMN_ID;
    auto keyDataType = common::PhysicalTypeID::ANY;
    deSer.deserializeValue(name);
    IndexType indexType = IndexType::deserialize(deSer);
    deSer.deserializeValue<common::column_id_t>(columnID);
    deSer.deserializeValue<common::PhysicalTypeID>(keyDataType);
    return IndexInfo{std::move(name), std::move(indexType), columnID, keyDataType};
}

std::shared_ptr<common::BufferedSerializer> IndexStorageInfo::serialize() const {
    return std::make_shared<common::BufferedSerializer>(0 /*maximumSize*/);
}

void Index::serialize(common::Serializer& ser) const {
    indexInfo.serialize(ser);
    auto bufferedWriter = storageInfo->serialize();
    ser.write<uint64_t>(bufferedWriter->getSize());
    ser.write(bufferedWriter->getData().data.get(), bufferedWriter->getSize());
}

} // namespace storage
} // namespace kuzu
