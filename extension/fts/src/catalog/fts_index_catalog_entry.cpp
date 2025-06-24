#include "catalog/fts_index_catalog_entry.h"

#include "catalog/catalog.h"
#include "common/serializer/buffer_reader.h"
#include "common/serializer/buffer_writer.h"
#include "common/string_utils.h"
#include "main/client_context.h"
#include "utils/fts_utils.h"

namespace kuzu {
namespace fts_extension {

std::shared_ptr<common::BufferWriter> FTSIndexAuxInfo::serialize() const {
    auto bufferWriter = std::make_shared<common::BufferWriter>();
    auto serializer = common::Serializer(bufferWriter);
    config.serialize(serializer);
    return bufferWriter;
}

std::unique_ptr<FTSIndexAuxInfo> FTSIndexAuxInfo::deserialize(
    std::unique_ptr<common::BufferReader> reader) {
    common::Deserializer deserializer{std::move(reader)};
    auto config = FTSConfig::deserialize(deserializer);
    return std::make_unique<FTSIndexAuxInfo>(std::move(config));
}

std::string FTSIndexAuxInfo::getStopWordsName(const common::FileScanInfo& exportFileInfo) const {
    std::string stopWordsName = "default";
    switch (exportFileInfo.fileTypeInfo.fileType) {
    case common::FileType::UNKNOWN: {
        stopWordsName = config.stopWordsSource;
    } break;
    case common::FileType::CSV:
    case common::FileType::PARQUET: {
        if (config.stopWordsTableName != FTSUtils::getDefaultStopWordsTableName()) {
            stopWordsName = common::stringFormat("{}/{}.{}", exportFileInfo.filePaths[0],
                config.stopWordsTableName,
                common::StringUtils::getLower(exportFileInfo.fileTypeInfo.fileTypeStr));
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    return stopWordsName;
}

std::string FTSIndexAuxInfo::toCypher(const catalog::IndexCatalogEntry& indexEntry,
    const catalog::ToCypherInfo& info) const {
    auto& indexToCypherInfo = info.constCast<catalog::IndexToCypherInfo>();
    std::string cypher;
    auto catalog = indexToCypherInfo.context->getCatalog();
    auto tableCatalogEntry = catalog->getTableCatalogEntry(
        indexToCypherInfo.context->getTransaction(), indexEntry.getTableID());
    auto tableName = tableCatalogEntry->getName();
    std::string propertyStr;
    auto propertyIDs = indexEntry.getPropertyIDs();
    for (auto i = 0u; i < propertyIDs.size(); i++) {
        propertyStr +=
            common::stringFormat("'{}'{}", tableCatalogEntry->getProperty(propertyIDs[i]).getName(),
                i == propertyIDs.size() - 1 ? "" : ", ");
    }

    cypher += common::stringFormat("CALL CREATE_FTS_INDEX('{}', '{}', [{}], stemmer := '{}', "
                                   "stopWords := '{}');",
        tableName, indexEntry.getIndexName(), std::move(propertyStr), config.stemmer,
        getStopWordsName(indexToCypherInfo.exportFileInfo));
    return cypher;
}

catalog::TableCatalogEntry* FTSIndexAuxInfo::getTableEntryToExport(
    const main::ClientContext* context) const {
    if (config.stopWordsTableName == FTSUtils::getDefaultStopWordsTableName()) {
        return nullptr;
    }
    return context->getCatalog()->getTableCatalogEntry(context->getTransaction(),
        config.stopWordsTableName);
}

} // namespace fts_extension
} // namespace kuzu
