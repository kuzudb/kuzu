#pragma once

#include "common/file_utils.h"
#include "common/types/types.h"

namespace kuzu {
namespace catalog {

// DAH is the abbreviation for Disk Array Header.
class MetadataDAHInfo {
public:
    MetadataDAHInfo() : MetadataDAHInfo{common::INVALID_PAGE_IDX, common::INVALID_PAGE_IDX} {}
    MetadataDAHInfo(common::page_idx_t dataDAHPageIdx)
        : MetadataDAHInfo{dataDAHPageIdx, common::INVALID_PAGE_IDX} {}
    MetadataDAHInfo(common::page_idx_t dataDAHPageIdx, common::page_idx_t nullDAHPageIdx)
        : dataDAHPageIdx{dataDAHPageIdx}, nullDAHPageIdx{nullDAHPageIdx} {}

    inline std::unique_ptr<MetadataDAHInfo> copy() {
        auto result = std::make_unique<MetadataDAHInfo>(dataDAHPageIdx, nullDAHPageIdx);
        result->childrenInfos.resize(childrenInfos.size());
        for (size_t i = 0; i < childrenInfos.size(); ++i) {
            result->childrenInfos[i] = childrenInfos[i]->copy();
        }
        return result;
    }

    void serialize(common::FileInfo* fileInfo, uint64_t& offset) const;
    static std::unique_ptr<MetadataDAHInfo> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);

    common::page_idx_t dataDAHPageIdx = common::INVALID_PAGE_IDX;
    common::page_idx_t nullDAHPageIdx = common::INVALID_PAGE_IDX;
    std::vector<std::unique_ptr<MetadataDAHInfo>> childrenInfos;
};

class Property {
public:
    static constexpr std::string_view REL_FROM_PROPERTY_NAME = "_FROM_";
    static constexpr std::string_view REL_TO_PROPERTY_NAME = "_TO_";

    Property(std::string name, std::unique_ptr<common::LogicalType> dataType)
        : Property{std::move(name), std::move(dataType), common::INVALID_PROPERTY_ID,
              common::INVALID_TABLE_ID} {}

    Property(std::string name, std::unique_ptr<common::LogicalType> dataType,
        common::property_id_t propertyID, common::table_id_t tableID,
        std::unique_ptr<MetadataDAHInfo> metadataDAHInfo = nullptr)
        : name{std::move(name)}, dataType{std::move(dataType)},
          propertyID{propertyID}, tableID{tableID}, metadataDAHInfo{std::move(metadataDAHInfo)} {
        if (this->metadataDAHInfo == nullptr) {
            this->metadataDAHInfo = std::make_unique<MetadataDAHInfo>();
        }
    }

    inline std::string getName() const { return name; }

    inline common::LogicalType* getDataType() const { return dataType.get(); }

    inline common::property_id_t getPropertyID() const { return propertyID; }

    inline common::table_id_t getTableID() const { return tableID; }

    inline MetadataDAHInfo* getMetadataDAHInfo() const { return metadataDAHInfo.get(); }

    inline void setPropertyID(common::property_id_t propertyID_) { this->propertyID = propertyID_; }

    inline void setTableID(common::table_id_t tableID_) { this->tableID = tableID_; }

    inline void setMetadataDAHInfo(std::unique_ptr<MetadataDAHInfo> metadataDAHInfo_) {
        this->metadataDAHInfo = std::move(metadataDAHInfo_);
    }

    inline void rename(std::string newName) { this->name = std::move(newName); }

    void serialize(common::FileInfo* fileInfo, uint64_t& offset) const;
    static std::unique_ptr<Property> deserialize(common::FileInfo* fileInfo, uint64_t& offset);

    static std::vector<std::unique_ptr<catalog::Property>> copyProperties(
        const std::vector<std::unique_ptr<catalog::Property>>& propertiesToCopy);

    inline std::unique_ptr<Property> copy() const {
        return std::make_unique<Property>(
            name, dataType->copy(), propertyID, tableID, metadataDAHInfo->copy());
    }

private:
    std::string name;
    std::unique_ptr<common::LogicalType> dataType;
    common::property_id_t propertyID;
    common::table_id_t tableID;
    std::unique_ptr<MetadataDAHInfo> metadataDAHInfo;
};

} // namespace catalog
} // namespace kuzu
