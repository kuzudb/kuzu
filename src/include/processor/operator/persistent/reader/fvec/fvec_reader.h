#pragma once

#include <vector>

#include "common/data_chunk/data_chunk.h"
#include "common/types/types.h"
#include "function/function.h"
#include "function/table/scan_functions.h"

namespace kuzu {
namespace processor {

class FvecReader {

public:
    explicit FvecReader(const std::string& filePath);

    ~FvecReader();

    void readBlock(common::block_idx_t blockIdx, common::ValueVector* vectorToRead);

    inline size_t getNumVectors() const {
        return numVectors;
    };

    inline void reset() {
        dataOffset = 0;
    }

    inline size_t getDimension() const {
        return dimension;
    };

private:
    void parseHeader();

private:
    std::string filePath;
    int fd;
    size_t fileSize;
    void* mmapRegion;
    size_t dataOffset;
    size_t numVectors;
    size_t dimension;
};

struct FvecScanSharedState final : public function::ScanSharedState {
    explicit FvecScanSharedState(common::ReaderConfig readerConfig, uint64_t numRows);
    std::unique_ptr<FvecReader> reader;
};

struct FvecScanFunction {
    static constexpr const char* name = "READ_FVEC";
    static function::function_set getFunctionSet();
};

} // namespace processor
} // namespace kuzu
