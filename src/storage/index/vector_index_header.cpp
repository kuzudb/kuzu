#include "storage/index/vector_index_header.h"

#include <fcntl.h>

#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
    namespace storage {
        VectorIndexHeaderPerPartition::VectorIndexHeaderPerPartition(const int dim, const uint64_t partitionId,
                                                                     node_group_idx_t startNodeGroupId,
                                                                     node_group_idx_t endNodeGroupId,
                                                                     table_id_t csrRelTableId,
                                                                     const uint64_t maxNbrsAtUpperLevel,
                                                                     const double samplingProbability)
                : partitionId(partitionId), numVectors(0), entrypoint(INVALID_VECTOR_ID),
                  entrypointLevel(0), maxNbrsAtUpperLevel(maxNbrsAtUpperLevel),
                  samplingProbability(samplingProbability),
                  numVectorsInUpperLevel(0), startNodeGroupId(startNodeGroupId), endNodeGroupId(endNodeGroupId),
                  csrRelTableId(csrRelTableId), quantizer(std::make_unique<SQ8Bit>(dim)), re(RandomEngine()) {}

        VectorIndexHeaderPerPartition::VectorIndexHeaderPerPartition(const VectorIndexHeaderPerPartition &other)
                : partitionId(other.partitionId), numVectors(other.numVectors), entrypoint(other.entrypoint),
                  entrypointLevel(other.entrypointLevel), maxNbrsAtUpperLevel(other.maxNbrsAtUpperLevel),
                  samplingProbability(other.samplingProbability), numVectorsInUpperLevel(other.numVectorsInUpperLevel),
                  startNodeGroupId(other.startNodeGroupId), endNodeGroupId(other.endNodeGroupId),
                  csrRelTableId(other.csrRelTableId), quantizer(std::make_unique<SQ8Bit>(*other.quantizer)),
                  re(RandomEngine()) {
            actualIds = std::vector<vector_id_t>(other.actualIds);
            neighbors = std::vector<vector_id_t>(other.neighbors);
            quantizedVectors = std::vector<uint8_t>(other.quantizedVectors);
        }

        VectorIndexHeaderPerPartition::VectorIndexHeaderPerPartition(
                const uint64_t partitionId, uint64_t numVectors,
                vector_id_t entrypoint, uint8_t entrypointLevel,
                const uint64_t maxNbrsAtUpperLevel,
                const double samplingProbability,
                std::vector<vector_id_t> actualIds,
                std::vector<vector_id_t> neighbors,
                uint64_t numVectorsInUpperLevel,
                node_group_idx_t startNodeGroupId,
                node_group_idx_t endNodeGroupId,
                table_id_t csrRelTableIds,
                std::unique_ptr<SQ8Bit> quantizer,
                std::vector<uint8_t> quantizedVectors) :
                partitionId(partitionId), numVectors(numVectors), entrypoint(entrypoint),
                entrypointLevel(entrypointLevel),
                maxNbrsAtUpperLevel(maxNbrsAtUpperLevel), samplingProbability(samplingProbability),
                actualIds(std::move(actualIds)), neighbors(std::move(neighbors)),
                numVectorsInUpperLevel(numVectorsInUpperLevel), startNodeGroupId(startNodeGroupId),
                endNodeGroupId(endNodeGroupId), csrRelTableId(csrRelTableIds), quantizer(std::move(quantizer)),
                quantizedVectors(std::move(quantizedVectors)), re(RandomEngine()) {}


        bool VectorIndexHeaderPerPartition::includeInUpperLevel() {
            float f = re.randomFloat();
            if (f <= samplingProbability) {
                return true;
            }
            return false;
        }

        void VectorIndexHeaderPerPartition::initSampleGraph(uint64_t numVectors) {
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
                neighbors.resize(newSize * maxNbrsAtUpperLevel, INVALID_VECTOR_ID);
            }

            // TODO: Maybe remove this!!
//            quantizedVectors.resize(quantizer->codeSize * numVectors, 0);
        }

        void VectorIndexHeaderPerPartition::update(const vector_id_t *vectorIds, int numVectors,
                                                   std::vector<vector_id_t> &upperLevelVectorIds) {
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

        void VectorIndexHeaderPerPartition::updateEntrypoint(vector_id_t entrypoint, uint8_t entrypointLevel) {
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

        void VectorIndexHeaderPerPartition::serialize(Serializer &serializer) const {
            serializer.serializeValue(partitionId);
            serializer.serializeValue(numVectors);
            serializer.serializeValue(entrypoint);
            serializer.serializeValue(entrypointLevel);
            serializer.serializeValue(maxNbrsAtUpperLevel);
            serializer.serializeValue(samplingProbability);
            serializer.serializeVector(actualIds);
            serializer.serializeVector(neighbors);
            serializer.serializeValue(numVectorsInUpperLevel);
            serializer.serializeValue(startNodeGroupId);
            serializer.serializeValue(endNodeGroupId);
            serializer.serializeValue(csrRelTableId);
            quantizer->serialize(serializer);
            serializer.serializeVector(quantizedVectors);
        }

        std::unique_ptr<VectorIndexHeaderPerPartition>
        VectorIndexHeaderPerPartition::deserialize(Deserializer &deserializer) {
            uint64_t partitionId;
            deserializer.deserializeValue(partitionId);
            uint64_t numVectors;
            deserializer.deserializeValue(numVectors);
            vector_id_t entrypoint;
            deserializer.deserializeValue(entrypoint);
            uint8_t entrypointLevel;
            deserializer.deserializeValue(entrypointLevel);
            uint64_t maxNbrsAtUpperLevel;
            deserializer.deserializeValue(maxNbrsAtUpperLevel);
            double samplingProbability;
            deserializer.deserializeValue(samplingProbability);
            std::vector<vector_id_t> actualIds;
            deserializer.deserializeVector(actualIds);
            std::vector<vector_id_t> neighbors;
            deserializer.deserializeVector(neighbors);
            uint64_t numVectorsInUpperLevel;
            deserializer.deserializeValue(numVectorsInUpperLevel);
            node_group_idx_t startNodeGroupId;
            deserializer.deserializeValue(startNodeGroupId);
            node_group_idx_t endNodeGroupId;
            deserializer.deserializeValue(endNodeGroupId);
            table_id_t csrRelTableId;
            deserializer.deserializeValue(csrRelTableId);
            std::unique_ptr<SQ8Bit> quantizer = SQ8Bit::deserialize(deserializer);
            std::vector<uint8_t> quantizedVectors;
            deserializer.deserializeVector(quantizedVectors);
            return std::make_unique<VectorIndexHeaderPerPartition>(partitionId, numVectors, entrypoint, entrypointLevel,
                                                                   maxNbrsAtUpperLevel, samplingProbability,
                                                                   std::move(actualIds), std::move(neighbors),
                                                                   numVectorsInUpperLevel, startNodeGroupId,
                                                                   endNodeGroupId,
                                                                   csrRelTableId, std::move(quantizer),
                                                                   std::move(quantizedVectors));
        }

        VectorIndexHeader::VectorIndexHeader(const int dim, const kuzu::common::VectorIndexConfig config,
                                             const kuzu::common::table_id_t nodeTableId,
                                             const kuzu::common::property_id_t embeddingPropertyId,
                                             const kuzu::common::property_id_t compressedPropertyId)
                : dim(dim), config(config),
                  nodeTableId(nodeTableId),
                  embeddingPropertyId(embeddingPropertyId),
                  compressedPropertyId(compressedPropertyId) {};

        VectorIndexHeader::VectorIndexHeader(const kuzu::storage::VectorIndexHeader &other)
                : dim(other.dim), config(other.config), nodeTableId(other.nodeTableId),
                  embeddingPropertyId(other.embeddingPropertyId), compressedPropertyId(other.compressedPropertyId) {
            for (const auto &partition: other.headerPerPartition) {
                headerPerPartition.push_back(partition->copy());
            }
        }

        VectorIndexHeader::VectorIndexHeader(const int dim, const kuzu::common::VectorIndexConfig config,
                                             const kuzu::common::table_id_t nodeTableId,
                                             const kuzu::common::property_id_t embeddingPropertyId,
                                             const kuzu::common::property_id_t compressedPropertyId,
                                             std::vector<std::unique_ptr<VectorIndexHeaderPerPartition>> headerPerPartition)
                : dim(dim), config(config),
                  nodeTableId(nodeTableId),
                  embeddingPropertyId(embeddingPropertyId),
                  compressedPropertyId(compressedPropertyId),
                  headerPerPartition(std::move(headerPerPartition)) {};

        void VectorIndexHeader::serialize(kuzu::common::Serializer &serializer) const {
            serializer.serializeValue(dim);
            config.serialize(serializer);
            serializer.serializeValue(nodeTableId);
            serializer.serializeValue(embeddingPropertyId);
            serializer.serializeValue(compressedPropertyId);
            serializer.serializeValue(headerPerPartition.size());
            for (const auto &partition: headerPerPartition) {
                partition->serialize(serializer);
            }
        }

        std::unique_ptr<VectorIndexHeader>
        VectorIndexHeader::deserialize(kuzu::common::Deserializer &deserializer) {
            int dim;
            deserializer.deserializeValue(dim);
            VectorIndexConfig config = VectorIndexConfig::deserialize(deserializer);
            table_id_t nodeTableId;
            deserializer.deserializeValue(nodeTableId);
            property_id_t embeddingPropertyId;
            deserializer.deserializeValue(embeddingPropertyId);
            property_id_t compressedPropertyId;
            deserializer.deserializeValue(compressedPropertyId);
            uint64_t numPartitions;
            deserializer.deserializeValue(numPartitions);
            std::vector<std::unique_ptr<VectorIndexHeaderPerPartition>> headerPerPartition;
            for (uint64_t i = 0; i < numPartitions; i++) {
                headerPerPartition.push_back(VectorIndexHeaderPerPartition::deserialize(deserializer));
            }
            return std::make_unique<VectorIndexHeader>(dim, config, nodeTableId, embeddingPropertyId,
                                                       compressedPropertyId, std::move(headerPerPartition));
        }

        void VectorIndexKey::serialize(common::Serializer &serializer) const {
            serializer.serializeValue(tableID);
            serializer.serializeValue(propertyId);
        }

        VectorIndexKey VectorIndexKey::deserialize(common::Deserializer &deserializer) {
            table_id_t tableID;
            deserializer.deserializeValue(tableID);
            property_id_t propertyId;
            deserializer.deserializeValue(propertyId);
            return VectorIndexKey(tableID, propertyId);
        }

        VectorIndexHeaders::VectorIndexHeaders(const std::string &databasePath,
                                               common::VirtualFileSystem *fs, main::ClientContext *context)
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

        void VectorIndexHeaders::saveToFile(const std::string &directory,
                                            common::FileVersionType dbFileType,
                                            transaction::TransactionType transactionType,
                                            common::VirtualFileSystem *fs) {
            std::string filePath = getFilePath(directory, dbFileType, fs);
            auto ser = Serializer(
                    std::make_unique<BufferedFileWriter>(fs->openFile(filePath, O_WRONLY | O_CREAT)));
            auto &vectorIndexHeaders = (transactionType == transaction::TransactionType::READ_ONLY ||
                                        readWriteVersion == nullptr) ?
                                       readOnlyVersion :
                                       readWriteVersion;
            ser.serializeUnorderedMap(vectorIndexHeaders->vectorIndexPerTable);
        }

        std::string VectorIndexHeaders::getFilePath(const std::string &directory,
                                                    common::FileVersionType dbFileType, common::VirtualFileSystem *fs) {
            return StorageUtils::getVectorIndexHeadersFilePath(fs, directory, dbFileType);
        }

        void VectorIndexHeaders::readFromFile(const std::string &dbPath, common::FileVersionType dbFileType,
                                              common::VirtualFileSystem *fs, main::ClientContext *context) {
            auto filePath = getFilePath(dbPath, dbFileType, fs);
            auto deser = Deserializer(
                    std::make_unique<BufferedFileReader>(fs->openFile(filePath, O_RDONLY, context)));
            deser.deserializeUnorderedMap(readOnlyVersion->vectorIndexPerTable);
        }

    } // namespace storage
} // namespace kuzu
