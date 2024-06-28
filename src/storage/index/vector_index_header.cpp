#include "storage/index/vector_index_header.h"

#include <fcntl.h>

#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

VectorIndexHeader::VectorIndexHeader(int dim, const VectorIndexConfig config, table_id_t tableId,
    property_id_t embeddingPropertyId, table_id_t csrRelTableId)
    : dim(dim), numVectors(0), config(std::move(config)), entrypoint(INVALID_VECTOR_ID),
      entrypointLevel(0), nodeTableId(tableId), embeddingPropertyId(embeddingPropertyId),
      csrRelTableIds(csrRelTableId), re(RandomEngine()) {}

VectorIndexHeader::VectorIndexHeader(const VectorIndexHeader& other)
    : dim(other.dim), numVectors(other.numVectors), config(other.config),
      entrypoint(other.entrypoint), entrypointLevel(other.entrypointLevel),
      nodeTableId(other.nodeTableId), embeddingPropertyId(other.embeddingPropertyId),
      csrRelTableIds(other.csrRelTableIds), re(RandomEngine()) {
    actualIds = std::vector<vector_id_t>(other.actualIds);
    neighbors = std::vector<vector_id_t>(other.neighbors);
}

VectorIndexHeader::VectorIndexHeader(int dim, uint64_t numVectors, const VectorIndexConfig config,
    vector_id_t entrypoint, uint8_t entrypointLevel, std::vector<vector_id_t> actualIds,
    std::vector<vector_id_t> neighbors, table_id_t nodeTableId, property_id_t embeddingPropertyId,
    table_id_t csrRelTableIds)
    : dim(dim), numVectors(numVectors), config(config), entrypoint(entrypoint),
      entrypointLevel(entrypointLevel), actualIds(std::move(actualIds)),
      neighbors(std::move(neighbors)), nodeTableId(nodeTableId),
      embeddingPropertyId(embeddingPropertyId), csrRelTableIds(csrRelTableIds), re(RandomEngine()) {
}

bool VectorIndexHeader::includeInUpperLevel() {
    float f = re.randomFloat();
    if (f <= config.samplingProbability) {
        return true;
    }
    return false;
}

void VectorIndexHeader::update(const vector_id_t* vectorIds, int numVectors,
    std::vector<vector_id_t>& upperLevelVectorIds) {
    std::vector<vector_id_t> vectorsInUpperLevel;
    for (int i = 0; i < numVectors; i++) {
        if (includeInUpperLevel()) {
            vectorsInUpperLevel.push_back(vectorIds[i]);
        }
    }
    {
        std::lock_guard<std::mutex> lock(mtx);
        this->numVectors += numVectors;
        if (vectorsInUpperLevel.empty() && entrypoint == INVALID_VECTOR_ID) {
            entrypoint = vectorIds[0];
        }
        if (!vectorsInUpperLevel.empty()) {
            auto numVectorsInUpperLayer = actualIds.size();
            actualIds.resize(numVectorsInUpperLayer + vectorsInUpperLevel.size(),
                INVALID_VECTOR_ID);

            auto numNeighbors = neighbors.size();
            neighbors.resize(numNeighbors + vectorsInUpperLevel.size() * config.maxNbrsAtUpperLevel,
                INVALID_VECTOR_ID);

            for (size_t i = 0; i < vectorsInUpperLevel.size(); i++) {
                auto upperLayerVectorId = i + numVectorsInUpperLayer;
                actualIds[upperLayerVectorId] = vectorsInUpperLevel[i];
                upperLevelVectorIds.push_back(upperLayerVectorId);
            }

            if (entrypoint == INVALID_VECTOR_ID || entrypointLevel == 0) {
                entrypoint = upperLevelVectorIds[0];
                entrypointLevel = 1;
            }
        }
    }
}

void VectorIndexHeader::serialize(Serializer& serializer) const {
    serializer.serializeValue(dim);
    serializer.serializeValue(numVectors);
    config.serialize(serializer);
    serializer.serializeValue(entrypoint);
    serializer.serializeValue(entrypointLevel);
    serializer.serializeVector(actualIds);
    serializer.serializeVector(neighbors);
    serializer.serializeValue(nodeTableId);
    serializer.serializeValue(embeddingPropertyId);
    serializer.serializeValue(csrRelTableIds);
}

std::unique_ptr<VectorIndexHeader> VectorIndexHeader::deserialize(Deserializer& deserializer) {
    int dim;
    deserializer.deserializeValue(dim);
    uint64_t numVectors;
    deserializer.deserializeValue(numVectors);
    auto config = VectorIndexConfig::deserialize(deserializer);
    vector_id_t entrypoint;
    deserializer.deserializeValue(entrypoint);
    uint8_t entrypointLevel;
    deserializer.deserializeValue(entrypointLevel);
    std::vector<vector_id_t> actualIds;
    deserializer.deserializeVector(actualIds);
    std::vector<vector_id_t> neighbors;
    deserializer.deserializeVector(neighbors);
    table_id_t nodeTableId;
    deserializer.deserializeValue(nodeTableId);
    property_id_t embeddingPropertyId;
    deserializer.deserializeValue(embeddingPropertyId);
    table_id_t csrRelTableIds;
    deserializer.deserializeValue(csrRelTableIds);
    return std::make_unique<VectorIndexHeader>(dim, numVectors, config, entrypoint, entrypointLevel,
        std::move(actualIds), std::move(neighbors), nodeTableId, embeddingPropertyId,
        csrRelTableIds);
}

void VectorIndexKey::serialize(common::Serializer& serializer) const {
    serializer.serializeValue(tableID);
    serializer.serializeValue(propertyId);
}

VectorIndexKey VectorIndexKey::deserialize(common::Deserializer& deserializer) {
    table_id_t tableID;
    deserializer.deserializeValue(tableID);
    property_id_t propertyId;
    deserializer.deserializeValue(propertyId);
    return VectorIndexKey(tableID, propertyId);
}

VectorIndexHeaders::VectorIndexHeaders(const std::string& databasePath,
    common::VirtualFileSystem* fs, main::ClientContext* context) : isUpdated{false} {
    readOnlyVersion = std::make_unique<VectorIndexHeaderCollection>();
    if (fs->fileOrPathExists(StorageUtils::getVectorIndexHeadersFilePath(fs, databasePath,
                                 FileVersionType::ORIGINAL),
            context)) {
        readFromFile(databasePath, FileVersionType::ORIGINAL, fs, context);
    } else {
        saveToFile(databasePath, FileVersionType::ORIGINAL, TransactionType::READ_ONLY, fs);
    }
}

void VectorIndexHeaders::saveToFile(const std::string& directory,
    common::FileVersionType dbFileType, transaction::TransactionType transactionType,
    common::VirtualFileSystem* fs) {
    std::string filePath = getFilePath(directory, dbFileType, fs);
    auto ser = Serializer(
        std::make_unique<BufferedFileWriter>(fs->openFile(filePath, O_WRONLY | O_CREAT)));
    auto& vectorIndexHeaders = (transactionType == transaction::TransactionType::READ_ONLY ||
                                   readWriteVersion == nullptr) ?
                                   readOnlyVersion :
                                   readWriteVersion;
    ser.serializeUnorderedMap(vectorIndexHeaders->vectorIndexPerTable);
}

std::string VectorIndexHeaders::getFilePath(const std::string& directory,
    common::FileVersionType dbFileType, common::VirtualFileSystem* fs) {
    return StorageUtils::getVectorIndexHeadersFilePath(fs, directory, dbFileType);
}

void VectorIndexHeaders::readFromFile(const std::string& dbPath, common::FileVersionType dbFileType,
    common::VirtualFileSystem* fs, main::ClientContext* context) {
    auto filePath = getFilePath(dbPath, dbFileType, fs);
    auto deser = Deserializer(
        std::make_unique<BufferedFileReader>(fs->openFile(filePath, O_RDONLY, context)));
    deser.deserializeUnorderedMap(readOnlyVersion->vectorIndexPerTable);
}

} // namespace storage
} // namespace kuzu
