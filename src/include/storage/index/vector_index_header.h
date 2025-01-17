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
        class VectorIndexHeaderPerPartition {
        public:
            explicit VectorIndexHeaderPerPartition(const int dim, const uint64_t partitionId,
                                                   node_group_idx_t startNodeGroupId,
                                                   node_group_idx_t endNodeGroupId, table_id_t csrRelTableId,
                                                   const uint64_t maxNbrsAtUpperLevel,
                                                   const double samplingProbability);

            VectorIndexHeaderPerPartition(const VectorIndexHeaderPerPartition &other);

            explicit VectorIndexHeaderPerPartition(const uint64_t partitionId, uint64_t numVectors,
                                                   vector_id_t entrypoint, uint8_t entrypointLevel,
                                                   const uint64_t maxNbrsAtUpperLevel, const double samplingProbability,
                                                   std::vector<vector_id_t> actualIds,
                                                   std::vector<vector_id_t> neighbors, uint64_t numVectorsInUpperLevel,
                                                   node_group_idx_t startNodeGroupId,
                                                   node_group_idx_t endNodeGroupId,
                                                   table_id_t csrRelTableIds, std::unique_ptr<SQ8Bit> quantizer,
                                                   std::vector<uint8_t> quantizedVectors);

            void initSampleGraph(uint64_t numVectors);

            // This method is thread-safe, however it doesn't resize the actualIds and neighbors vectors.
            void update(const vector_id_t *vectorIds, int numVectors, std::vector<vector_id_t> &upperLevelVectorIds);

            void updateEntrypoint(vector_id_t entrypoint, uint8_t entrypointLevel);

            void getEntrypoint(vector_id_t &ep, uint8_t &epLevel) {
                std::lock_guard<std::mutex> lock(mtx);
                ep = entrypoint;
                epLevel = entrypointLevel;
            }

            // TODO: Add bound check for the actualIds.
            inline uint64_t getActualId(uint64_t id) const { return actualIds[id]; }

            inline vector_id_t *getNeighbors(vector_id_t id, size_t &begin, size_t &end) {
                begin = id * maxNbrsAtUpperLevel;
                end = begin + maxNbrsAtUpperLevel;
                return neighbors.data();
            }

            inline table_id_t getCSRRelTableId() const { return csrRelTableId; }

            inline uint64_t getNumVectors() const { return numVectors; }

            inline SQ8Bit *getQuantizer() { return quantizer.get(); }

            inline uint8_t *getQuantizedVectors() { return quantizedVectors.data(); }

            inline node_group_idx_t getStartNodeGroupId() const { return startNodeGroupId; }

            inline node_group_idx_t getEndNodeGroupId() const { return endNodeGroupId; }

            inline offset_t getMaxOffset(offset_t maxNodeTableOffset) const {
                auto startOffset = startNodeGroupId * StorageConstants::NODE_GROUP_SIZE;
                auto endOffset = (endNodeGroupId + 1) * StorageConstants::NODE_GROUP_SIZE;
                return std::min(maxNodeTableOffset, (endOffset - 1)) - startOffset;
            }

            void serialize(Serializer &serializer) const;

            static std::unique_ptr<VectorIndexHeaderPerPartition> deserialize(Deserializer &deserializer);

            inline std::unique_ptr<VectorIndexHeaderPerPartition> copy() const {
                return std::make_unique<VectorIndexHeaderPerPartition>(*this);
            }

        private:
            bool includeInUpperLevel();

        private:
            // TODO: Ideally we should have a Partitioner class that is responsible for partitioning the vectors
            //      for now let's skip this as it'll make things more complicated.
            // General configuration
            const uint64_t partitionId;
            uint64_t numVectors;

            // entrypoint information
            vector_id_t entrypoint;
            uint8_t entrypointLevel;
            std::mutex mtx;

            // In-memory CSR for the sample graph
            const uint64_t maxNbrsAtUpperLevel;
            const double samplingProbability;
            std::vector<vector_id_t> actualIds;
            std::vector<vector_id_t> neighbors;
            uint64_t numVectorsInUpperLevel;

            // Partition Node table info
            node_group_idx_t startNodeGroupId;
            node_group_idx_t endNodeGroupId;

            // CSR Rel Table ID
            table_id_t csrRelTableId;

            // Quantizer
            std::unique_ptr<SQ8Bit> quantizer;
            // TODO: Maybe remove this!
            std::vector<uint8_t> quantizedVectors;

            // This is used to keep track of the number of vectors in each level
            RandomEngine re;
        };

        class VectorIndexHeader {
        public:
            VectorIndexHeader(const int dim, const VectorIndexConfig config,
                              const table_id_t nodeTableId, const property_id_t embeddingPropertyId,
                              const property_id_t compressedPropertyId);

            VectorIndexHeader(const VectorIndexHeader &other);

            VectorIndexHeader(const int dim, const VectorIndexConfig config, const table_id_t nodeTableId,
                              const property_id_t embeddingPropertyId,
                              const property_id_t compressedPropertyId,
                              std::vector<std::unique_ptr<VectorIndexHeaderPerPartition>> headerPerPartition);

            static std::string getCompressedVectorPropertyName(property_id_t embeddingPropertyId) {
                return stringFormat("{}_compressed_vectors", embeddingPropertyId);
            }

            static std::string getIndexRelTableName(table_id_t nodeTableId,
                                                    property_id_t embeddingPropertyId,
                                                    // TODO: Maybe it's better to include partitionId here,
                                                    //  instead of startNodeGroupId and endNodeGroupId
                                                    node_group_idx_t startNodeGroupId,
                                                    node_group_idx_t endNodeGroupId) {
                return stringFormat("{}_{}_{}_{}_vector_index", nodeTableId, embeddingPropertyId, startNodeGroupId,
                                    endNodeGroupId);
            }

            inline uint64_t getNumVectors() const {
                uint64_t numVectors = 0;
                for (const auto &header: headerPerPartition) {
                    numVectors += header->getNumVectors();
                }
                return numVectors;
            }

            // This method is not thread safe
            inline int createPartition(node_group_idx_t startNodeGroupId, node_group_idx_t endNodeGroupId,
                                       table_id_t csrRelTableId) {
                auto partitionId = headerPerPartition.size();
                headerPerPartition.push_back(std::make_unique<VectorIndexHeaderPerPartition>(
                        dim,
                        partitionId,
                        startNodeGroupId,
                        endNodeGroupId,
                        csrRelTableId,
                        config.maxNbrs * config.gamma,
                        config.samplingProbability));
                return partitionId;
            }

            inline int getDim() const { return dim; }

            inline VectorIndexConfig getConfig() const { return config; }

            inline table_id_t getNodeTableId() const { return nodeTableId; }

            inline property_id_t getEmbeddingPropertyId() const { return embeddingPropertyId; }

            inline property_id_t getCompressedPropertyId() const { return compressedPropertyId; }

            inline uint64_t getNumPartitions() const { return headerPerPartition.size(); }

            inline VectorIndexHeaderPerPartition *getPartitionHeader(uint64_t partitionId) {
                KU_ASSERT(partitionId < headerPerPartition.size());
                return headerPerPartition[partitionId].get();
            }

            void serialize(Serializer &serializer) const;

            static std::unique_ptr<VectorIndexHeader> deserialize(Deserializer &deserializer);

            inline std::unique_ptr<VectorIndexHeader> copy() const {
                return std::make_unique<VectorIndexHeader>(*this);
            }

        private:
            // General configuration
            const int dim;
            const VectorIndexConfig config;

            // Node table info
            const table_id_t nodeTableId;
            const property_id_t embeddingPropertyId;
            const property_id_t compressedPropertyId;

            // header per partition
            std::vector<std::unique_ptr<VectorIndexHeaderPerPartition>> headerPerPartition;
        };

        struct VectorIndexKey {
            table_id_t tableID;
            property_id_t propertyId;

            VectorIndexKey(table_id_t tableID_, property_id_t propertyId_)
                    : tableID(tableID_), propertyId(propertyId_) {}

            bool operator==(const VectorIndexKey &other) const {
                return tableID == other.tableID && propertyId == other.propertyId;
            }

            void serialize(Serializer &serializer) const;

            static VectorIndexKey deserialize(Deserializer &deserializer);
        };

        struct VectorIndexKeyHasher {
            size_t operator()(const VectorIndexKey &key) const {
                return std::hash<table_id_t>{}(key.tableID) ^ std::hash<property_id_t>{}(key.propertyId);
            }
        };

        struct VectorIndexHeaderCollection {
            // We maintain the header for a table property
            std::unordered_map<VectorIndexKey, std::unique_ptr<VectorIndexHeader>, VectorIndexKeyHasher>
                    vectorIndexPerTable;

            VectorIndexHeader *getVectorIndexHeader(table_id_t tableID, property_id_t propertyId) const {
                auto key = VectorIndexKey(tableID, propertyId);
                KU_ASSERT(vectorIndexPerTable.contains(key));
                return vectorIndexPerTable.at(key).get();
            }
        };

        class WAL;

        class VectorIndexHeaders {
        public:
            explicit VectorIndexHeaders(const std::string &databasePath, VirtualFileSystem *fs,
                                        main::ClientContext *context);

            ~VectorIndexHeaders() = default;

            void setUpdated() { isUpdated = true; }

            bool hasUpdates() const { return isUpdated; }

            void writeFileForWALRecord(const std::string &directory, common::VirtualFileSystem *fs) {
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
                                                                     indexHeader->getEmbeddingPropertyId())] = std::move(
                        indexHeader);
            }

            VectorIndexHeader *getHeaderWriteVersion(table_id_t nodeTableId,
                                                     property_id_t embeddingPropertyId) {
                std::unique_lock lck{mtx};
                setUpdated();
                initForWriteTrxNoLock();
                return readWriteVersion
                        ->vectorIndexPerTable[VectorIndexKey(nodeTableId, embeddingPropertyId)]
                        .get();
            }

            VectorIndexHeader *getHeaderReadOnlyVersion(table_id_t nodeTableId,
                                                        property_id_t embeddingPropertyId) {
                std::unique_lock lck{mtx};
                return readOnlyVersion->getVectorIndexHeader(nodeTableId, embeddingPropertyId);
            }

            void saveToFile(const std::string &directory, common::FileVersionType dbFileType,
                            TransactionType transactionType, common::VirtualFileSystem *fs);

        private:
            inline void initForWriteTrxNoLock() {
                if (readWriteVersion == nullptr) {
                    readWriteVersion = std::make_unique<VectorIndexHeaderCollection>();
                    KU_ASSERT(readOnlyVersion);
                    for (auto &[indexKey, indexHeader]: readOnlyVersion->vectorIndexPerTable) {
                        readWriteVersion->vectorIndexPerTable[indexKey] = indexHeader->copy();
                    }
                }
            }

            std::string getFilePath(const std::string &directory, common::FileVersionType dbFileType,
                                    common::VirtualFileSystem *fs);

            const VectorIndexHeaderCollection *getVersion(TransactionType type) const {
                return type == TransactionType::READ_ONLY ? readOnlyVersion.get() : readWriteVersion.get();
            }

            void readFromFile(const std::string &dbPath, common::FileVersionType dbFileType,
                              common::VirtualFileSystem *fs, main::ClientContext *context);

            void resetUpdated() { isUpdated = false; }

        protected:
            bool isUpdated;
            std::unique_ptr<VectorIndexHeaderCollection> readOnlyVersion;
            std::unique_ptr<VectorIndexHeaderCollection> readWriteVersion;
            std::mutex mtx;
        };

    } // namespace storage
} // namespace kuzu
