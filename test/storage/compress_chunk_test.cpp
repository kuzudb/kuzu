#include "alp/decode.hpp"
#include "alp/encode.hpp"
#include "gmock/gmock-matchers.h"
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
    void SetUp() override {
        BaseGraphTest::SetUp(); // NOLINT
        createDBAndConn();
    }

    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }

    template<std::floating_point T>
    void testCompressChunk(std::vector<T> bufferToCompress, check_func_t checkFunc);

    template<std::floating_point T>
    void testCheckWholeOutput(std::vector<T> bufferToCompress);
};

template<std::floating_point T>
std::unique_ptr<CompressionMetadata> getFloatMetadata(std::vector<T> bufferToCompress,
    const std::shared_ptr<FloatCompression<T>>& alg) {
    alp::state alpMetadata;
    std::vector<T> samples(bufferToCompress.size());
    alp::AlpEncode<T>::init(bufferToCompress.data(), 0, bufferToCompress.size(), samples.data(),
        alpMetadata);
    if (alpMetadata.best_k_combinations.size() >
        1) { // Only if more than 1 found top combinations we sample and search
        alp::AlpEncode<T>::find_best_exponent_factor_from_combinations(
            alpMetadata.best_k_combinations, alpMetadata.k_combinations, bufferToCompress.data(),
            alpMetadata.vector_size, alpMetadata.fac, alpMetadata.exp);
    } else if (alpMetadata.best_k_combinations.size() > 0) {
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

    auto preScanMetadata = GetFloatCompressionMetadata<T>{alg, dataType}.operator()(
        (uint8_t*)bufferToCompress.data(), bufferToCompress.size() * sizeof(T),
        bufferToCompress.size(), bufferToCompress.size(), metadata->min, metadata->max);
    auto startPageIdx = dataFH->addNewPages(preScanMetadata.numPages);

    if (preScanMetadata.compMeta.compression == CompressionType::CONSTANT) {
        return preScanMetadata;
    }

    return CompressedFloatFlushBuffer<T>{alg, dataType}.operator()(
        (uint8_t*)bufferToCompress.data(), bufferToCompress.size(), dataFH, startPageIdx,
        preScanMetadata);
}

template<std::floating_point T>
void CompressChunkTest::testCompressChunk(std::vector<T> bufferToCompress, check_func_t checkFunc) {
    auto* bm = getBufferManager(*database);
    auto* storageManager = getStorageManager(*database);
    auto* dataFH = storageManager->getDataFH();

    const auto dataType = std::is_same_v<float, T> ? LogicalType::FLOAT() : LogicalType::DOUBLE();
    const auto alg = std::make_shared<FloatCompression<T>>();

    const auto preScanMetadata = getFloatMetadata(bufferToCompress, alg);
    auto chunkMetadata =
        compressBuffer(bufferToCompress, alg, preScanMetadata.get(), dataFH, dataType);

    auto columnReader = ColumnReaderFactory::createColumnReader(dataType.getPhysicalType(),
        DBFileID::newDataFileID(), dataFH, bm, &storageManager->getWAL());
    transaction::Transaction transaction{transaction::TransactionType::READ_ONLY};

    checkFunc(columnReader.get(), &transaction, chunkMetadata,
        chunkMetadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType), dataType);
}

template<std::floating_point T>
void CompressChunkTest::testCheckWholeOutput(std::vector<T> bufferToCompress) {
    testCompressChunk<T>(bufferToCompress,
        [&bufferToCompress](ColumnReader* reader, transaction::Transaction* transaction,
            const ColumnChunkMetadata& chunkMeta, uint64_t numValuesPerPage,
            const LogicalType& dataType) {
            std::vector<T> out(bufferToCompress.size());
            reader->readCompressedValuesToPage(transaction, chunkMeta, numValuesPerPage,
                (uint8_t*)out.data(), 0, 0, out.size(), ReadCompressedValuesFromPage(dataType),
                [](offset_t, offset_t) { return true; });
            EXPECT_THAT(out, ::testing::ContainerEq(bufferToCompress));
        });
}

TEST_F(CompressChunkTest, TestDoubleSimpleSingleRead) {

    std::vector<double> bufferToCompress(128, 5.5);
    bufferToCompress[5] = 0;

    auto checkFunc = [&bufferToCompress](ColumnReader* reader,
                         transaction::Transaction* transaction,
                         const ColumnChunkMetadata& chunkMeta, uint64_t numValuesPerPage,
                         const LogicalType& dataType) {
        std::vector<double> val(2, 1.0);
        reader->readCompressedValueToPage(transaction, chunkMeta, numValuesPerPage, 1,
            (uint8_t*)val.data(), 0, ReadCompressedValuesFromPage(dataType));
        EXPECT_EQ(bufferToCompress[1], val[0]);

        reader->readCompressedValueToPage(transaction, chunkMeta, numValuesPerPage, 5,
            (uint8_t*)val.data(), 1, ReadCompressedValuesFromPage(dataType));
        EXPECT_EQ(0, val[1]);
    };
    testCompressChunk(bufferToCompress, checkFunc);
}

TEST_F(CompressChunkTest, TestDoubleWithExceptions) {
    std::vector<double> src(256, 5.6);
    src[2] = 54387589437957.834;
    for (size_t i = 102; i < src.size(); i += 100) {
        src[i] = src[i - 100] + 4385498.234;
    }

    testCheckWholeOutput(src);
}

TEST_F(CompressChunkTest, TestFloatWithExceptions) {
    std::vector<float> src(256, 5.6);
    src[2] = 54387589.8341;
    for (size_t i = 102; i < src.size(); i += 100) {
        src[i] = src[i - 100] + 4385.2348;
    }

    testCheckWholeOutput(src);
}

TEST_F(CompressChunkTest, TestFloatFilter) {
    std::vector<float> src(10 * 1024, 5.6);
    src[2] = -54387589.8341;
    for (size_t i = 8; i < src.size(); i += 6) {
        src[i] = src[i - 6] + -4385.2348;
    }

    testCompressChunk(src, [&src](ColumnReader* reader, transaction::Transaction* transaction,
                               const ColumnChunkMetadata& chunkMeta, uint64_t numValuesPerPage,
                               const LogicalType& dataType) {
        common::ValueVector out{LogicalType::FLOAT()};

        static constexpr size_t startOffset = 5 * 1024 + 7;
        static constexpr size_t numValuesToRead = 32;

        reader->readCompressedValuesToVector(transaction, chunkMeta, numValuesPerPage, &out, 0,
            startOffset, startOffset + numValuesToRead,
            ReadCompressedValuesFromPageToVector(dataType), [](offset_t startIdx, offset_t endIdx) {
                for (size_t i = startIdx; i < endIdx; ++i) {
                    if (((i - 2) % 6) == 0)
                        return true;
                }
                return false;
            });

        for (size_t i = 0; i < numValuesToRead; ++i) {
            if (((i - 2) % 6) == 0) {
                EXPECT_EQ(src[i + startOffset], out.getValue<float>(i));
            }
        }
    });
}

TEST_F(CompressChunkTest, TestDoubleMultiPage) {
    std::vector<double> src(10 * 1024, 5.6);
    src[1] = 12345678901234.56;
    for (size_t i = 98; i < src.size(); i += 23) {
        src[i] = src[i - 23] + 78901234.567;
    }

    testCheckWholeOutput(src);
}

TEST_F(CompressChunkTest, TestConstant) {
    std::vector<double> src(256, 0.54);
    testCheckWholeOutput(src);
}

TEST_F(CompressChunkTest, TestFloatManyExceptionsNoCompress) {
    std::vector<float> src(10 * 1024, 1.23);
    src[0] = 54387589.8341;
    for (size_t i = 1; i < src.size(); i += 1) {
        src[i] = (src[i - 1] + 4385.2348) * 0.9788;
    }

    testCheckWholeOutput(src);
}

TEST_F(CompressChunkTest, TestDoubleWithCompress) {
    std::vector<double> src(10 * 1024, 5.6);
    src[2] = 54387589437957.834;
    for (size_t i = 102; i < src.size(); i += 6) {
        src[i] = src[i - 6] + 4385498.234;
    }

    testCheckWholeOutput(src);
}

TEST_F(CompressChunkTest, TestDoubleReadPartialAtOffsets) {
    std::vector<double> src(10 * 1024, 5.6);
    src[2] = 54387589437957.834;
    for (size_t i = 102; i < src.size(); i += 6) {
        src[i] = src[i - 6] + 4385498.234;
    }

    testCompressChunk(src, [&src](ColumnReader* reader, transaction::Transaction* transaction,
                               const ColumnChunkMetadata& chunkMeta, uint64_t numValuesPerPage,
                               const LogicalType& dataType) {
        std::vector<double> out(src.size());

        static constexpr size_t offsetInResult = 150;
        static constexpr size_t numValuesToRead = 200;
        const size_t offsetInSrc = src.size() - numValuesToRead;

        reader->readCompressedValuesToPage(transaction, chunkMeta, numValuesPerPage,
            (uint8_t*)out.data(), offsetInResult, offsetInSrc, offsetInSrc + numValuesToRead,
            ReadCompressedValuesFromPage(dataType), [](offset_t, offset_t) { return true; });
        EXPECT_THAT(std::vector<double>(out.begin() + offsetInResult,
                        out.begin() + offsetInResult + numValuesToRead),
            ::testing::ContainerEq(std::vector<double>(src.begin() + offsetInSrc,
                src.begin() + offsetInSrc + numValuesToRead)));
    });
}

TEST_F(CompressChunkTest, TestDoubleReadPartialMultiPage) {
    std::vector<double> src(10 * 1024, 5.6);
    src[2] = 54387538437957.834;
    for (size_t i = 102; i < src.size(); i += 6) {
        src[i] = src[i - 6] + 9285498.231;
    }

    testCompressChunk(src, [&src](ColumnReader* reader, transaction::Transaction* transaction,
                               const ColumnChunkMetadata& chunkMeta, uint64_t numValuesPerPage,
                               const LogicalType& dataType) {
        std::vector<double> out(src.size());

        static constexpr size_t offsetInSrc = 1485;
        static constexpr size_t numValuesToRead = 3 * 1024 + 5;

        reader->readCompressedValuesToPage(transaction, chunkMeta, numValuesPerPage,
            (uint8_t*)out.data(), 0, offsetInSrc, offsetInSrc + numValuesToRead,
            ReadCompressedValuesFromPage(dataType), [](offset_t, offset_t) { return true; });
        EXPECT_THAT(std::vector<double>(out.begin(), out.begin() + numValuesToRead),
            ::testing::ContainerEq(std::vector<double>(src.begin() + offsetInSrc,
                src.begin() + offsetInSrc + numValuesToRead)));
    });
}

TEST_F(CompressChunkTest, TestDoubleReadPartialAtOffsetsIntoValueVector) {
    std::vector<double> src(512, 5.6);
    src[4] = -54387589437957.834;
    for (size_t i = 12; i < src.size(); i += 8) {
        src[i] = src[i - 8] - 4385498.234;
    }

    testCompressChunk(src, [&src](ColumnReader* reader, transaction::Transaction* transaction,
                               const ColumnChunkMetadata& chunkMeta, uint64_t numValuesPerPage,
                               const LogicalType& dataType) {
        common::ValueVector out{LogicalType::DOUBLE()};

        static constexpr size_t offsetInResult = 150;
        static constexpr size_t numValuesToRead = 200;
        const size_t offsetInSrc = src.size() - numValuesToRead;

        reader->readCompressedValuesToVector(transaction, chunkMeta, numValuesPerPage, &out,
            offsetInResult, offsetInSrc, offsetInSrc + numValuesToRead,
            ReadCompressedValuesFromPageToVector(dataType),
            [](offset_t, offset_t) { return true; });

        for (size_t i = 0; i < numValuesToRead; ++i) {
            EXPECT_EQ(src[offsetInSrc + i], out.getValue<double>(offsetInResult + i));
        }
    });
}

} // namespace testing
} // namespace kuzu
