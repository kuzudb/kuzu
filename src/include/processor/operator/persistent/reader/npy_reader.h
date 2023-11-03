#pragma once

#include <string>
#include <vector>

#include "common/data_chunk/data_chunk.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"

namespace kuzu {
namespace processor {

class NpyReader {
public:
    explicit NpyReader(const std::string& filePath);

    ~NpyReader();

    size_t getNumElementsPerRow() const;

    uint8_t* getPointerToRow(size_t row) const;

    inline size_t getNumRows() const { return shape[0]; }

    void readBlock(common::block_idx_t blockIdx, common::ValueVector* vectorToRead) const;

    // Used in tests only.
    inline common::LogicalTypeID getType() const { return type; }
    inline std::vector<size_t> getShape() const { return shape; }

    void validate(const common::LogicalType& type_, common::offset_t numRows);

private:
    void parseHeader();
    void parseType(std::string descr);

private:
    std::string filePath;
    int fd;
    size_t fileSize;
    void* mmapRegion;
    size_t dataOffset;
    std::vector<size_t> shape;
    common::LogicalTypeID type;
    static inline const std::string defaultFieldName = "NPY_FIELD";
};

class NpyMultiFileReader {
public:
    explicit NpyMultiFileReader(const std::vector<std::string>& filePaths);

    void readBlock(common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead) const;

private:
    std::vector<std::unique_ptr<NpyReader>> fileReaders;
};

} // namespace processor
} // namespace kuzu
