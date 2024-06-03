#pragma once

#include "common/types/types.h"

namespace kuzu {
namespace storage {

// DAH is the abbreviation for Disk Array Header.
struct MetadataDAHInfo {
    common::page_idx_t dataDAHIdx = common::INVALID_PAGE_IDX;
    common::page_idx_t nullDAHIdx = common::INVALID_PAGE_IDX;
    std::vector<std::unique_ptr<MetadataDAHInfo>> childrenInfos;

    MetadataDAHInfo() : MetadataDAHInfo{common::INVALID_PAGE_IDX, common::INVALID_PAGE_IDX} {}
    explicit MetadataDAHInfo(common::page_idx_t dataDAHPageIdx)
        : MetadataDAHInfo{dataDAHPageIdx, common::INVALID_PAGE_IDX} {}
    MetadataDAHInfo(common::page_idx_t dataDAHIdx, common::page_idx_t nullDAHIdx)
        : dataDAHIdx{dataDAHIdx}, nullDAHIdx{nullDAHIdx} {}

    std::unique_ptr<MetadataDAHInfo> copy();

    void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<MetadataDAHInfo> deserialize(common::Deserializer& deserializer);
};

} // namespace storage
} // namespace kuzu
