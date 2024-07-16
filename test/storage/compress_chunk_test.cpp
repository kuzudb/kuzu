#include "alp/decode.hpp"
#include "alp/encode.hpp"
#include "graph_test/graph_test.h"
#include "gtest/gtest.h"
#include "storage/storage_manager.h"
#include "storage/store/column_chunk_flush.h"
#include "storage/store/column_chunk_metadata.h"
#include "storage/store/column_read.h"
#include <ranges>

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace testing {

using check_func_t = std::function<void(ColumnReader*, transaction::Transaction*,
    const ColumnChunkMetadata&, uint64_t, const LogicalType&)>;

class CompressChunkTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }

    template<std::floating_point T>
    void testCompressChunk(std::vector<T> bufferToCompress, check_func_t checkFunc);
};

template<std::floating_point T>
std::unique_ptr<CompressionMetadata> getFloatMetadata(std::vector<T> bufferToCompress,
    const std::shared_ptr<FloatCompression<T>>& alg) {
    alp::state alpMetadata;
    std::vector<double> samples(bufferToCompress.size());
    alp::AlpEncode<double>::init(bufferToCompress.data(), 0, bufferToCompress.size(),
        samples.data(), alpMetadata);
    EXPECT_TRUE(alpMetadata.best_k_combinations.size() >= 1);
    if (alpMetadata.best_k_combinations.size() >
        1) { // Only if more than 1 found top combinations we sample and search
        alp::AlpEncode<double>::find_best_exponent_factor_from_combinations(
            alpMetadata.best_k_combinations, alpMetadata.k_combinations, bufferToCompress.data(),
            alpMetadata.vector_size, alpMetadata.fac, alpMetadata.exp);
    } else {
        alpMetadata.exp = alpMetadata.best_k_combinations[0].first;
        alpMetadata.fac = alpMetadata.best_k_combinations[0].second;
    }

    const auto& [min, max] = std::minmax_element(bufferToCompress.begin(), bufferToCompress.end());

    const auto floatEncodedValues =
        std::views::all(bufferToCompress) | std::views::transform([alpMetadata](double val) {
            const auto encoded_value =
                alp::AlpEncode<double>::encode_value(val, alpMetadata.fac, alpMetadata.exp);
            const auto decoded_value = alp::AlpDecode<double>::decode_value(encoded_value,
                alpMetadata.fac, alpMetadata.exp);
            return val == decoded_value ? encoded_value : 0;
        });
    const auto& [minEncoded, maxEncoded] =
        std::minmax_element(floatEncodedValues.begin(), floatEncodedValues.end());
    return std::make_unique<CompressionMetadata>(StorageValue(*min), StorageValue(*max),
        alg->getCompressionType(), alpMetadata, StorageValue(*minEncoded),
        StorageValue(*maxEncoded), 10);
}

template<std::floating_point T>
ColumnChunkMetadata compressBuffer(std::vector<T> bufferToCompress,
    const std::shared_ptr<FloatCompression<T>>& alg, const CompressionMetadata* metadata,
    BMFileHandle* dataFH, const LogicalType& dataType) {

    auto preScanMetadata = GetFloatCompressionMetadata<double>{alg, dataType}.operator()(
        (uint8_t*)bufferToCompress.data(), bufferToCompress.size() * sizeof(double),
        bufferToCompress.size(), bufferToCompress.size(), metadata->min, metadata->max);
    auto startPageIdx = dataFH->addNewPages(preScanMetadata.numPages);
    return CompressedFloatFlushBuffer<double>{alg, dataType}.operator()(
        (uint8_t*)bufferToCompress.data(), bufferToCompress.size(), dataFH, startPageIdx,
        preScanMetadata);
}

template<std::floating_point T>
void CompressChunkTest::testCompressChunk(std::vector<T> bufferToCompress, check_func_t checkFunc) {
    auto* bm = getBufferManager(*database);
    auto* storageManager = getStorageManager(*database);
    auto* dataFH = storageManager->getDataFH();

    const auto dataType = std::is_same_v<float, T> ? LogicalType::FLOAT() : LogicalType::DOUBLE();
    const auto alg = std::make_shared<FloatCompression<double>>();

    const auto preScanMetadata = getFloatMetadata(bufferToCompress, alg);
    auto chunkMetadata =
        compressBuffer(bufferToCompress, alg, preScanMetadata.get(), dataFH, dataType);

    auto columnReader = ColumnReaderFactory::createColumnReader(common::PhysicalTypeID::DOUBLE,
        dataFH, bm, &storageManager->getWAL());
    transaction::Transaction transaction{transaction::TransactionType::READ_ONLY};

    checkFunc(columnReader.get(), &transaction, chunkMetadata,
        chunkMetadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType), dataType);
}

TEST_F(CompressChunkTest, TestDouble) {

    std::vector<double> bufferToCompress(128, 5.5);
    bufferToCompress[5] = 0;

    auto checkFunc = [&bufferToCompress](ColumnReader* reader,
                         transaction::Transaction* transaction,
                         const ColumnChunkMetadata& chunkMeta, uint64_t numValuesPerPage,
                         const LogicalType& dataType) {
        double val;
        reader->readCompressedValuesToPage(transaction, chunkMeta, numValuesPerPage, (uint8_t*)&val,
            0, 1, 2, ReadCompressedValuesFromPage(dataType),
            [](offset_t, offset_t) { return true; });
        EXPECT_EQ(bufferToCompress[1], val);

        reader->readCompressedValuesToPage(transaction, chunkMeta, numValuesPerPage, (uint8_t*)&val,
            0, 5, 6, ReadCompressedValuesFromPage(dataType),
            [](offset_t, offset_t) { return true; });
        EXPECT_EQ(0, val);
    };
    testCompressChunk(bufferToCompress, checkFunc);
}

} // namespace testing
} // namespace kuzu
