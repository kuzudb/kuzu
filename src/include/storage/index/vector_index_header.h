#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <vector>

#include "common/random_engine.h"
#include "common/types/types.h"
#include "common/vector_index/vector_index_config.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/index/quantization.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

class DiskArrayCollection;

// TODO: Change the name to VectorIndexMetadata
class VectorIndexHeader {
public:
    explicit VectorIndexHeader(int dim, const VectorIndexConfig config, table_id_t tableId,
        property_id_t embeddingPropertyId, property_id_t compressedPropertyId,
        table_id_t csrRelTableId);

    VectorIndexHeader(const VectorIndexHeader& other);

    explicit VectorIndexHeader(int dim, uint64_t numVectors, const VectorIndexConfig config,
        vector_id_t entrypoint, uint8_t entrypointLevel, std::vector<vector_id_t> actualIds,
        std::vector<vector_id_t> neighbors, uint64_t numVectorsInUpperLevel, table_id_t nodeTableId,
        property_id_t embeddingPropertyId, property_id_t compressedPropertyId,
        table_id_t csrRelTableIds, std::unique_ptr<SQ8Bit> quantizer);

    void initSampleGraph(uint64_t numVectors);

    // This method is thread-safe, however it doesn't resize the actualIds and neighbors vectors.
    void update(const vector_id_t* vectorIds, int numVectors,
        std::vector<vector_id_t>& upperLevelVectorIds);

    void updateEntrypoint(vector_id_t entrypoint, uint8_t entrypointLevel);

    void getEntrypoint(vector_id_t& ep, uint8_t& epLevel) {
        std::lock_guard<std::mutex> lock(mtx);
        ep = entrypoint;
        epLevel = entrypointLevel;
    }

    inline uint64_t getActualId(uint64_t id) const { return actualIds[id]; }

    inline vector_id_t* getNeighbors(vector_id_t id, size_t& begin, size_t& end) {
        begin = id * config.maxNbrsAtUpperLevel;
        end = begin + config.maxNbrsAtUpperLevel;
        return neighbors.data();
    }

    inline VectorIndexConfig getConfig() const { return config; }

    inline int getDim() const { return dim; }

    inline table_id_t getNodeTableId() const { return nodeTableId; }

    inline property_id_t getEmbeddingPropertyId() const { return embeddingPropertyId; }

    inline property_id_t getCompressedPropertyId() const { return compressedPropertyId; }

    inline table_id_t getCSRRelTableId() const { return csrRelTableIds; }

    static std::string getIndexRelTableName(table_id_t nodeTableId,
        property_id_t embeddingPropertyId) {
        return stringFormat("{}_{}_vector_index", nodeTableId, embeddingPropertyId);
    }

    static std::string getCompressedVectorPropertyName(property_id_t embeddingPropertyId) {
        return stringFormat("{}_compressed_vectors", embeddingPropertyId);
    }

    inline uint64_t getNumVectors() const { return numVectors; }

    inline SQ8Bit* getQuantizer() { return quantizer.get(); }

    void serialize(Serializer& serializer) const;

    static std::unique_ptr<VectorIndexHeader> deserialize(Deserializer& deserializer);

    inline std::unique_ptr<VectorIndexHeader> copy() const {
        return std::make_unique<VectorIndexHeader>(*this);
    }

private:
    bool includeInUpperLevel();

private:
    // General configuration
    int dim;
    uint64_t numVectors;
    const VectorIndexConfig config;

    // entrypoint information
    vector_id_t entrypoint;
    uint8_t entrypointLevel;
    std::mutex mtx;

    // In-memory CSR for the sample graph
    std::vector<vector_id_t> actualIds;
    std::vector<vector_id_t> neighbors;
    uint64_t numVectorsInUpperLevel;

    // Node table info
    table_id_t nodeTableId;
    property_id_t embeddingPropertyId;
    property_id_t compressedPropertyId;

    // CSR Rel Table ID
    table_id_t csrRelTableIds;

    // Quantizer
    std::unique_ptr<SQ8Bit> quantizer;

    // This is used to keep track of the number of vectors in each level
    RandomEngine re;
};

struct VectorIndexKey {
    table_id_t tableID;
    property_id_t propertyId;

    VectorIndexKey(table_id_t tableID_, property_id_t propertyId_)
        : tableID(tableID_), propertyId(propertyId_) {}
    bool operator==(const VectorIndexKey& other) const {
        return tableID == other.tableID && propertyId == other.propertyId;
    }

    void serialize(Serializer& serializer) const;

    static VectorIndexKey deserialize(Deserializer& deserializer);
};

struct VectorIndexKeyHasher {
    size_t operator()(const VectorIndexKey& key) const {
        return std::hash<table_id_t>{}(key.tableID) ^ std::hash<property_id_t>{}(key.propertyId);
    }
};

struct VectorIndexHeaderCollection {
    // We maintain the header for a table property
    std::unordered_map<VectorIndexKey, std::unique_ptr<VectorIndexHeader>, VectorIndexKeyHasher>
        vectorIndexPerTable;

    VectorIndexHeader* getVectorIndexHeader(table_id_t tableID, property_id_t propertyId) const {
        auto key = VectorIndexKey(tableID, propertyId);
        KU_ASSERT(vectorIndexPerTable.contains(key));
        return vectorIndexPerTable.at(key).get();
    }
};

class WAL;
class VectorIndexHeaders {
public:
    explicit VectorIndexHeaders(const std::string& databasePath, VirtualFileSystem* fs,
        main::ClientContext* context);

    ~VectorIndexHeaders() = default;

    void setUpdated() { isUpdated = true; }
    bool hasUpdates() const { return isUpdated; }

    void writeFileForWALRecord(const std::string& directory, common::VirtualFileSystem* fs) {
        saveToFile(directory, common::FileVersionType::WAL_VERSION,
            transaction::TransactionType::WRITE, fs);
    }

    void checkpointInMemoryIfNecessary() {
        std::unique_lock lck{mtx};
        KU_ASSERT(readWriteVersion);
        readOnlyVersion = std::move(readWriteVersion);
        resetUpdated();
    }

    void rollbackInMemoryIfNecessary() {
        std::unique_lock lck{mtx};
        readWriteVersion.reset();
        resetUpdated();
    }

    void addHeader(std::unique_ptr<VectorIndexHeader> indexHeader) {
        std::unique_lock lck{mtx};
        setUpdated();
        initForWriteTrxNoLock();
        readWriteVersion->vectorIndexPerTable[VectorIndexKey(indexHeader->getNodeTableId(),
            indexHeader->getEmbeddingPropertyId())] = std::move(indexHeader);
    }

    VectorIndexHeader* getHeaderWriteVersion(table_id_t nodeTableId,
        property_id_t embeddingPropertyId) {
        std::unique_lock lck{mtx};
        setUpdated();
        initForWriteTrxNoLock();
        return readWriteVersion
            ->vectorIndexPerTable[VectorIndexKey(nodeTableId, embeddingPropertyId)]
            .get();
    }

    VectorIndexHeader* getHeaderReadOnlyVersion(table_id_t nodeTableId,
        property_id_t embeddingPropertyId) {
        std::unique_lock lck{mtx};
        return readOnlyVersion->getVectorIndexHeader(nodeTableId, embeddingPropertyId);
    }

    void saveToFile(const std::string& directory, common::FileVersionType dbFileType,
        TransactionType transactionType, common::VirtualFileSystem* fs);

private:
    inline void initForWriteTrxNoLock() {
        if (readWriteVersion == nullptr) {
            readWriteVersion = std::make_unique<VectorIndexHeaderCollection>();
            KU_ASSERT(readOnlyVersion);
            for (auto& [indexKey, indexHeader] : readOnlyVersion->vectorIndexPerTable) {
                readWriteVersion->vectorIndexPerTable[indexKey] = indexHeader->copy();
            }
        }
    }

    std::string getFilePath(const std::string& directory, common::FileVersionType dbFileType,
        common::VirtualFileSystem* fs);

    const VectorIndexHeaderCollection* getVersion(TransactionType type) const {
        return type == TransactionType::READ_ONLY ? readOnlyVersion.get() : readWriteVersion.get();
    }

    void readFromFile(const std::string& dbPath, common::FileVersionType dbFileType,
        common::VirtualFileSystem* fs, main::ClientContext* context);

    void resetUpdated() { isUpdated = false; }

protected:
    bool isUpdated;
    std::unique_ptr<VectorIndexHeaderCollection> readOnlyVersion;
    std::unique_ptr<VectorIndexHeaderCollection> readWriteVersion;
    std::mutex mtx;
};

} // namespace storage
} // namespace kuzu
