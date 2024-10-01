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
    property_id_t embeddingPropertyId, property_id_t compressedPropertyId, table_id_t csrRelTableId)
    : dim(dim), numVectors(0), config(std::move(config)), entrypoint(INVALID_VECTOR_ID),
      entrypointLevel(0), numVectorsInUpperLevel(0), nodeTableId(tableId),
      embeddingPropertyId(embeddingPropertyId), compressedPropertyId(compressedPropertyId),
      csrRelTableIds(csrRelTableId), quantizer(std::make_unique<SQ8Bit>(dim)), re(RandomEngine()) {}

VectorIndexHeader::VectorIndexHeader(const VectorIndexHeader& other)
    : dim(other.dim), numVectors(other.numVectors), config(other.config),
      entrypoint(other.entrypoint), entrypointLevel(other.entrypointLevel),
      numVectorsInUpperLevel(other.numVectorsInUpperLevel), nodeTableId(other.nodeTableId),
      embeddingPropertyId(other.embeddingPropertyId),
      compressedPropertyId(other.compressedPropertyId), csrRelTableIds(other.csrRelTableIds),
      quantizer(other.quantizer->copy()), re(RandomEngine()) {
    actualIds = std::vector<vector_id_t>(other.actualIds);
    neighbors = std::vector<vector_id_t>(other.neighbors);
}

VectorIndexHeader::VectorIndexHeader(int dim, uint64_t numVectors, const VectorIndexConfig config,
    vector_id_t entrypoint, uint8_t entrypointLevel, std::vector<vector_id_t> actualIds,
    std::vector<vector_id_t> neighbors, uint64_t numVectorsInUpperLevel, table_id_t nodeTableId,
    property_id_t embeddingPropertyId, property_id_t compressedPropertyId,
    table_id_t csrRelTableIds, std::unique_ptr<SQ8Bit> quantizer)
    : dim(dim), numVectors(numVectors), config(config), entrypoint(entrypoint),
      entrypointLevel(entrypointLevel), actualIds(std::move(actualIds)),
      neighbors(std::move(neighbors)), numVectorsInUpperLevel(numVectorsInUpperLevel),
      nodeTableId(nodeTableId), embeddingPropertyId(embeddingPropertyId),
      compressedPropertyId(compressedPropertyId), csrRelTableIds(csrRelTableIds),
      quantizer(std::move(quantizer)), re(RandomEngine()) {}

bool VectorIndexHeader::includeInUpperLevel() {
    float f = re.randomFloat();
    if (f <= config.samplingProbability) {
        return true;
    }
    return false;
}

void VectorIndexHeader::initSampleGraph(uint64_t numVectors) {
    std::lock_guard<std::mutex> lock(mtx);
    auto currentSize = actualIds.size();
    auto newSize = currentSize;
    for (uint64_t i = 0; i < numVectors; i++) {
        if (includeInUpperLevel()) {
            newSize++;
        }
    }
    // Add some buffer space for now. Will fix it!!
    newSize += 5000;
    if (newSize > currentSize) {
        actualIds.resize(newSize, INVALID_VECTOR_ID);
        neighbors.resize(newSize * config.maxNbrsAtUpperLevel, INVALID_VECTOR_ID);
    }
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
        // Problem, while increasing size of actualIds, neighbors, upperLevelVectorIds, we need to
        // lock the mutex. and also while reading the neighbors, actualIds, we need to lock the
        // mutex.
        std::lock_guard<std::mutex> lock(mtx);
        this->numVectors += numVectors;
        if (!vectorsInUpperLevel.empty()) {
            for (size_t i = 0; i < vectorsInUpperLevel.size(); i++) {
                auto upperLayerVectorId = numVectorsInUpperLevel++;
                actualIds[upperLayerVectorId] = vectorsInUpperLevel[i];
                upperLevelVectorIds.push_back(upperLayerVectorId);
            }
        }
    }
}

void VectorIndexHeader::updateEntrypoint(vector_id_t entrypoint, uint8_t entrypointLevel) {
    if (this->entrypoint != INVALID_VECTOR_ID && this->entrypointLevel >= entrypointLevel) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (this->entrypoint == INVALID_VECTOR_ID || this->entrypointLevel < entrypointLevel) {
            this->entrypoint = entrypoint;
            this->entrypointLevel = entrypointLevel;
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
    serializer.serializeValue(numVectorsInUpperLevel);
    serializer.serializeValue(nodeTableId);
    serializer.serializeValue(embeddingPropertyId);
    serializer.serializeValue(compressedPropertyId);
    serializer.serializeValue(csrRelTableIds);
    quantizer->serialize(serializer);
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
    uint64_t numVectorsInUpperLevel;
    deserializer.deserializeValue(numVectorsInUpperLevel);
    table_id_t nodeTableId;
    deserializer.deserializeValue(nodeTableId);
    property_id_t embeddingPropertyId;
    deserializer.deserializeValue(embeddingPropertyId);
    property_id_t compressedPropertyId;
    deserializer.deserializeValue(compressedPropertyId);
    table_id_t csrRelTableIds;
    deserializer.deserializeValue(csrRelTableIds);
    std::unique_ptr<SQ8Bit> quantizer = SQ8Bit::deserialize(deserializer);
    return std::make_unique<VectorIndexHeader>(dim, numVectors, config, entrypoint, entrypointLevel,
        std::move(actualIds), std::move(neighbors), numVectorsInUpperLevel, nodeTableId,
        embeddingPropertyId, compressedPropertyId, csrRelTableIds, std::move(quantizer));
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
    common::VirtualFileSystem* fs, main::ClientContext* context)
    : isUpdated{false} {
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
