#include "index/fts_storage_info.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::storage;

std::shared_ptr<common::BufferedSerializer> FTSStorageInfo::serialize() const {
    auto bufferWriter = std::make_shared<common::BufferedSerializer>();
    auto serializer = common::Serializer(bufferWriter);
    serializer.write<common::idx_t>(numDocs);
    serializer.write<double>(avgDocLen);
    return bufferWriter;
}

std::unique_ptr<IndexStorageInfo> FTSStorageInfo::deserialize(
    std::unique_ptr<common::BufferReader> reader) {
    common::Deserializer deSer{std::move(reader)};
    common::idx_t numDocs = 0;
    double avgDocLen = 0;
    deSer.deserializeValue<common::idx_t>(numDocs);
    deSer.deserializeValue<double>(avgDocLen);
    return std::make_unique<FTSStorageInfo>(numDocs, avgDocLen);
}

} // namespace fts_extension
} // namespace kuzu
