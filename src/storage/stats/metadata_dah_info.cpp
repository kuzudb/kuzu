#include "storage/stats/metadata_dah_info.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::unique_ptr<MetadataDAHInfo> MetadataDAHInfo::copy() {
    auto result = std::make_unique<MetadataDAHInfo>(dataDAHIdx, nullDAHIdx);
    result->childrenInfos.resize(childrenInfos.size());
    for (size_t i = 0; i < childrenInfos.size(); ++i) {
        result->childrenInfos[i] = childrenInfos[i]->copy();
    }
    return result;
}

void MetadataDAHInfo::serialize(Serializer& serializer) const {
    serializer.serializeValue(dataDAHIdx);
    serializer.serializeValue(nullDAHIdx);
    serializer.serializeVectorOfPtrs(childrenInfos);
}

std::unique_ptr<MetadataDAHInfo> MetadataDAHInfo::deserialize(Deserializer& deserializer) {
    auto metadataDAHInfo = std::make_unique<MetadataDAHInfo>();
    deserializer.deserializeValue(metadataDAHInfo->dataDAHIdx);
    deserializer.deserializeValue(metadataDAHInfo->nullDAHIdx);
    deserializer.deserializeVectorOfPtrs(metadataDAHInfo->childrenInfos);
    return metadataDAHInfo;
}

} // namespace storage
} // namespace kuzu
