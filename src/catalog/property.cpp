#include "catalog/property.h"

#include "common/ser_deser.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void MetadataDAHInfo::serialize(FileInfo* fileInfo, uint64_t& offset) const {
    SerDeser::serializeValue(dataDAHPageIdx, fileInfo, offset);
    SerDeser::serializeValue(nullDAHPageIdx, fileInfo, offset);
    SerDeser::serializeVectorOfPtrs(childrenInfos, fileInfo, offset);
}

std::unique_ptr<MetadataDAHInfo> MetadataDAHInfo::deserialize(
    FileInfo* fileInfo, uint64_t& offset) {
    auto metadataDAHInfo = std::make_unique<MetadataDAHInfo>();
    SerDeser::deserializeValue(metadataDAHInfo->dataDAHPageIdx, fileInfo, offset);
    SerDeser::deserializeValue(metadataDAHInfo->nullDAHPageIdx, fileInfo, offset);
    SerDeser::deserializeVectorOfPtrs(metadataDAHInfo->childrenInfos, fileInfo, offset);
    return metadataDAHInfo;
}

void Property::serialize(FileInfo* fileInfo, uint64_t& offset) const {
    SerDeser::serializeValue(name, fileInfo, offset);
    dataType->serialize(fileInfo, offset);
    SerDeser::serializeValue(propertyID, fileInfo, offset);
    SerDeser::serializeValue(tableID, fileInfo, offset);
    metadataDAHInfo->serialize(fileInfo, offset);
}

std::unique_ptr<Property> Property::deserialize(FileInfo* fileInfo, uint64_t& offset) {
    std::string name;
    property_id_t propertyID;
    table_id_t tableID;
    SerDeser::deserializeValue(name, fileInfo, offset);
    auto dataType = LogicalType::deserialize(fileInfo, offset);
    SerDeser::deserializeValue(propertyID, fileInfo, offset);
    SerDeser::deserializeValue(tableID, fileInfo, offset);
    auto metadataDAHInfo = MetadataDAHInfo::deserialize(fileInfo, offset);
    return std::make_unique<Property>(
        name, std::move(dataType), propertyID, tableID, std::move(metadataDAHInfo));
}

std::vector<std::unique_ptr<catalog::Property>> Property::copyProperties(
    const std::vector<std::unique_ptr<catalog::Property>>& propertiesToCopy) {
    std::vector<std::unique_ptr<catalog::Property>> propertiesToReturn;
    propertiesToReturn.reserve(propertiesToCopy.size());
    for (const auto& property : propertiesToCopy) {
        propertiesToReturn.push_back(property->copy());
    }
    return propertiesToReturn;
}

} // namespace catalog
} // namespace kuzu
