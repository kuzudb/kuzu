#include "processor/operator/persistent/reader/fvec/fvec_reader.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>

#include "common/exception/copy.h"
#include "common/utils.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "storage/storage_utils.h"
#include <sys/fcntl.h>

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

FvecReader::FvecReader(const std::string& filePath) : filePath(filePath), dataOffset(0) {
    fd = open(filePath.c_str(), O_RDONLY);
    if (fd == -1) {
        throw CopyException("Failed to open fvecs file.");
    }
    struct stat fileStatus {};
    fstat(fd, &fileStatus);
    fileSize = fileStatus.st_size;
    // TODO: Add support for windows
    mmapRegion = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
    if (mmapRegion == MAP_FAILED) {
        throw CopyException("Failed to mmap NPY file.");
    }
    parseHeader();
}

FvecReader::~FvecReader() {
    munmap(mmapRegion, fileSize);
    close(fd);
}

void FvecReader::parseHeader() {
    int d;
    memcpy(&d, mmapRegion, sizeof(int));
    if (d <= 0 || d >= 1000000) {
        throw CopyException("Failed to read fvec file. Unreasonable dimension.");
    }
    dimension = d;
    if (fileSize % ((d + 1) * 4) != 0) {
        throw CopyException("Failed to read fvec file. Weird file size.");
    }
    numVectors = (fileSize / ((d + 1) * 4));
    dataOffset = sizeof(int);
}

void FvecReader::readBlock(common::block_idx_t blockIdx, common::ValueVector* vectorToRead) {
    uint64_t rowNumber = blockIdx * DEFAULT_VECTOR_CAPACITY;
    if (rowNumber >= numVectors) {
//        vectorToRead->state->selVector->selectedSize = 0;
        return;
    }
    uint64_t numRows = std::min(DEFAULT_VECTOR_CAPACITY, numVectors - rowNumber);
    for (auto i = 0; i < numRows; i++) {
        auto listEntry = ListVector::addList(vectorToRead, dimension);
        vectorToRead->setValue(i, listEntry);
    }
    auto dataVector = ListVector::getDataVector(vectorToRead);
    uint8_t* dest = dataVector->getData();
    uint8_t* src = (uint8_t*)mmapRegion + dataOffset;
    for (auto i = 0; i < numRows; i++) {
        memcpy(dest + i * dimension, src + 1 + i * (dimension + 1), dimension * sizeof(float));
    }
//    vectorToRead->state->selVector->selectedSize = numRows;
    dataOffset += numRows * (dimension + 1) * sizeof(float);
}

FvecScanSharedState::FvecScanSharedState(common::ReaderConfig readerConfig, uint64_t numRows)
    : ScanSharedState(std::move(readerConfig), numRows) {
    reader = std::make_unique<FvecReader>(this->readerConfig.filePaths[0]);
}

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState = reinterpret_cast<FvecScanSharedState*>(input.sharedState);
    auto [_, blockIdx] = sharedState->getNext();
    sharedState->reader->readBlock(blockIdx, output.dataChunk.getValueVector(0).get());
//    return output.dataChunk.state->selVector->selectedSize;
    return 0;
}

static std::unique_ptr<function::TableFuncBindData> bindFunc(
    main::ClientContext* /*context*/, function::TableFuncBindInput* input) {
    auto scanInput = reinterpret_cast<function::ScanTableFuncBindInput*>(input);
    KU_ASSERT(scanInput->config.filePaths.size() == 1);
    std::vector<std::string> resultColumnNames;
    std::vector<common::LogicalType> resultColumnTypes;
    resultColumnNames.push_back("column");
    auto reader = make_unique<FvecReader>(scanInput->config.filePaths[0]);
    resultColumnTypes.push_back(LogicalType::ARRAY(LogicalType::FLOAT(), reader->getDimension()));
    return std::make_unique<function::ScanBindData>(std::move(resultColumnTypes),
        std::move(resultColumnNames), scanInput->config.copy(), scanInput->context);
}

static std::unique_ptr<function::TableFuncSharedState> initSharedState(
    function::TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<function::ScanBindData*>(input.bindData);
    auto reader = make_unique<FvecReader>(bindData->config.filePaths[0]);
    return std::make_unique<FvecScanSharedState>(bindData->config.copy(), reader->getNumVectors());
}

static std::unique_ptr<function::TableFuncLocalState> initLocalState(
    function::TableFunctionInitInput& /*input*/, function::TableFuncSharedState* /*state*/,
    storage::MemoryManager* /*mm*/) {
    return std::make_unique<function::TableFuncLocalState>();
}

function::function_set FvecScanFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(name, tableFunc, bindFunc,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

} // namespace processor
} // namespace kuzu
