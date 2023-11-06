#include "storage/stats/metadata_dah_info.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::unique_ptr<MetadataDAHInfo> MetadataDAHInfo::copy() {
    auto result = std::make_unique<MetadataDAHInfo>(dataDAHPageIdx, nullDAHPageIdx);
    result->childrenInfos.resize(childrenInfos.size());
    for (size_t i = 0; i < childrenInfos.size(); ++i) {
        result->childrenInfos[i] = childrenInfos[i]->copy();
    }
    return result;
}

void MetadataDAHInfo::serialize(Serializer& serializer) const {
    serializer.serializeValue(dataDAHPageIdx);
    serializer.serializeValue(nullDAHPageIdx);
    serializer.serializeVectorOfPtrs(childrenInfos);
}

std::unique_ptr<MetadataDAHInfo> MetadataDAHInfo::deserialize(Deserializer& deserializer) {
    auto metadataDAHInfo = std::make_unique<MetadataDAHInfo>();
    deserializer.deserializeValue(metadataDAHInfo->dataDAHPageIdx);
    deserializer.deserializeValue(metadataDAHInfo->nullDAHPageIdx);
    deserializer.deserializeVectorOfPtrs(metadataDAHInfo->childrenInfos);
    return metadataDAHInfo;
}

} // namespace storage
} // namespace kuzu
