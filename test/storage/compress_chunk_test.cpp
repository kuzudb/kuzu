#include <cstdint>

#include "alp/decode.hpp"
#include "alp/encode.hpp"
#include "gmock/gmock-matchers.h"
#include "graph_test/graph_test.h"
#include "gtest/gtest.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_manager.h"
#include "storage/store/column_chunk_data.h"
#include "storage/store/column_chunk_metadata.h"
#include "storage/store/column_reader_writer.h"
#include "storage/store/compression_flush_buffer.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"
#include <ranges>

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace testing {

using check_func_t = std::function<void(ColumnReadWriter*, transaction::Transaction*, ChunkState&,
    const LogicalType&)>;

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
    void commitUpdate(transaction::Transaction* transaction, ChunkState& state, FileHandle* dataFH,
        MemoryManager* memoryManager, ShadowFile* shadowFile);

    template<std::floating_point T>
    void testCompressChunk(const std::vector<T>& bufferToCompress, check_func_t checkFunc);

    template<std::floating_point T>
    void testUpdateChunk(std::vector<T>& bufferToCompress, check_func_t updateFunc);

    template<std::floating_point T>
    void testCheckWholeOutput(const std::vector<T>& bufferToCompress);
};

template<typename T>
std::span<const uint8_t> byteSpan(const std::vector<T>& buffer) {
    auto bufferSpan = std::span(buffer);
    return std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(bufferSpan.data()),
        bufferSpan.size_bytes());
}

template<std::floating_point T>
std::unique_ptr<CompressionMetadata> getFloatMetadata(const std::vector<T>& bufferToCompress,
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
    std::vector<T> vec(floatEncodedValues.begin(), floatEncodedValues.end());
    const auto& [minEncoded, maxEncoded] = std::minmax_element(vec.begin(), vec.end());
    const auto physicalType =
        std::is_same_v<T, double> ? PhysicalTypeID::DOUBLE : PhysicalTypeID::FLOAT;
    return std::make_unique<CompressionMetadata>(StorageValue(*min), StorageValue(*max),
        alg->getCompressionType(), alpMetadata, StorageValue(*minEncoded),
        StorageValue(*maxEncoded), physicalType);
}

template<std::floating_point T>
ColumnChunkMetadata compressBuffer(const std::vector<T>& bufferToCompress,
    const std::shared_ptr<FloatCompression<T>>& alg, const CompressionMetadata* metadata,
    FileHandle* dataFH, const LogicalType& dataType) {

    auto preScanMetadata =
        GetFloatCompressionMetadata<T>{alg, dataType}.operator()(byteSpan(bufferToCompress),
            bufferToCompress.size(), bufferToCompress.size(), metadata->min, metadata->max);
    auto startPageIdx = dataFH->addNewPages(preScanMetadata.numPages);

    if (preScanMetadata.compMeta.compression == CompressionType::CONSTANT) {
        return preScanMetadata;
    }

    return CompressedFloatFlushBuffer<T>{alg, dataType}.operator()(byteSpan(bufferToCompress),
        dataFH, startPageIdx, preScanMetadata);
}

template<std::floating_point T>
void CompressChunkTest::commitUpdate(transaction::Transaction* transaction, ChunkState& state,
    FileHandle* dataFH, MemoryManager* memoryManager, ShadowFile* shadowFile) {
    if (state.metadata.compMeta.compression == storage::CompressionType::ALP) {
        state.getExceptionChunk<T>()->finalizeAndFlushToDisk(state);
    }
    auto* clientContext = getClientContext(*conn);
    clientContext->getTransactionManagerUnsafe()->commit(*clientContext);
    clientContext->getTransactionManagerUnsafe()->checkpoint(*clientContext);
    if (state.metadata.compMeta.compression == storage::CompressionType::ALP) {
        state.alpExceptionChunk = std::make_unique<InMemoryExceptionChunk<T>>(transaction, state,
            dataFH, memoryManager, shadowFile);
    }
}

template<std::floating_point T>
void CompressChunkTest::testCompressChunk(const std::vector<T>& bufferToCompress,
    check_func_t checkFunc) {
    auto* mm = getMemoryManager(*database);
    auto* bm = getBufferManager(*database);
    auto* storageManager = getStorageManager(*database);
    auto* dataFH = storageManager->getDataFH();

    const auto dataType = std::is_same_v<float, T> ? LogicalType::FLOAT() : LogicalType::DOUBLE();
    const auto alg = std::make_shared<FloatCompression<T>>();

    const auto preScanMetadata = getFloatMetadata(bufferToCompress, alg);
    auto chunkMetadata =
        compressBuffer(bufferToCompress, alg, preScanMetadata.get(), dataFH, dataType);

    auto columnReader = ColumnReadWriterFactory::createColumnReadWriter(dataType.getPhysicalType(),
        DBFileID::newDataFileID(), dataFH, bm, &storageManager->getShadowFile());

    auto* clientContext = getClientContext(*conn);
    clientContext->getTransactionContext()->beginWriteTransaction();

    ChunkState state;
    state.metadata = chunkMetadata;
    state.numValuesPerPage = state.metadata.compMeta.numValues(KUZU_PAGE_SIZE, dataType);
    if (chunkMetadata.compMeta.compression == CompressionType::ALP) {
        state.alpExceptionChunk = std::make_unique<InMemoryExceptionChunk<T>>(
            clientContext->getTransaction(), state, dataFH, mm, &storageManager->getShadowFile());
    }

    checkFunc(columnReader.get(), clientContext->getTransaction(), state, dataType);
}

template<std::floating_point T>
void CompressChunkTest::testUpdateChunk(std::vector<T>& bufferToCompress, check_func_t updateFunc) {
    if (!inMemMode) {
        testCompressChunk(bufferToCompress,
            [&bufferToCompress, &updateFunc, this](ColumnReadWriter* reader,
                transaction::Transaction* transaction, ChunkState& state,
                const LogicalType& dataType) {
                auto* mm = getMemoryManager(*database);
                auto* storageManager = getStorageManager(*database);
                auto* dataFH = storageManager->getDataFH();

                updateFunc(reader, transaction, state, dataType);

                commitUpdate<T>(transaction, state, dataFH, mm, &storageManager->getShadowFile());

                std::vector<T> out(bufferToCompress.size());
                reader->readCompressedValuesToPage(transaction, state, (uint8_t*)out.data(), 0, 0,
                    out.size(), ReadCompressedValuesFromPage(dataType));
                EXPECT_THAT(out, ::testing::ContainerEq(bufferToCompress));
            });
    }
}

template<std::floating_point T>
void CompressChunkTest::testCheckWholeOutput(const std::vector<T>& bufferToCompress) {
    testCompressChunk(bufferToCompress,
        [&bufferToCompress](ColumnReadWriter* reader, transaction::Transaction* transaction,
            ChunkState& state, const LogicalType& dataType) {
            std::vector<T> out(bufferToCompress.size());
            reader->readCompressedValuesToPage(transaction, state, (uint8_t*)out.data(), 0, 0,
                out.size(), ReadCompressedValuesFromPage(dataType));
            EXPECT_THAT(out, ::testing::ContainerEq(bufferToCompress));
        });
}

TEST_F(CompressChunkTest, TestDoubleSimpleSingleRead) {
    std::vector<double> bufferToCompress(128, -5.5);
    bufferToCompress[5] = -1;

    auto checkFunc = [&bufferToCompress](ColumnReadWriter* reader,
                         transaction::Transaction* transaction, ChunkState& state,
                         const LogicalType& dataType) {
        std::vector<double> val(2, 1.0);
        reader->readCompressedValueToPage(transaction, state, 1, (uint8_t*)val.data(), 0,
            ReadCompressedValuesFromPage(dataType));
        EXPECT_EQ(bufferToCompress[1], val[0]);

        reader->readCompressedValueToPage(transaction, state, 5, (uint8_t*)val.data(), 1,
            ReadCompressedValuesFromPage(dataType));
        EXPECT_EQ(-1, val[1]);
    };
    testCompressChunk(bufferToCompress, checkFunc);
}

TEST_F(CompressChunkTest, TestDoubleWithExceptions) {
    std::vector<double> src(256, 5.6);
    src[0] = 0;
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
    src[4] = 0;
    src[2] = -54387589.8341;
    for (size_t i = 8; i < src.size(); i += 6) {
        src[i] = src[i - 6] + -4385.2348;
    }

    testCompressChunk(src, [&src](ColumnReadWriter* reader, transaction::Transaction* transaction,
                               ChunkState& state, const LogicalType& dataType) {
        common::ValueVector out{LogicalType::FLOAT()};

        static constexpr size_t startOffset = 2 * 1024 + 7;
        static constexpr size_t numValuesToRead = DEFAULT_VECTOR_CAPACITY;
        const size_t startPageIdx = startOffset / state.numValuesPerPage;

        reader->readCompressedValuesToVector(transaction, state, &out, 0, startOffset,
            startOffset + numValuesToRead, ReadCompressedValuesFromPageToVector(dataType),
            [&state, startPageIdx](offset_t startIdx, offset_t) {
                const auto pageIdx = (startOffset + startIdx) / state.numValuesPerPage;
                return (pageIdx == startPageIdx);
            });

        for (size_t i = 0; i < numValuesToRead; ++i) {
            const size_t pageIdx = (startOffset + i) / state.numValuesPerPage;
            if (startPageIdx == pageIdx) {
                EXPECT_EQ(src[i + startOffset], out.getValue<float>(i));
            } else {
                EXPECT_EQ(0, out.getValue<float>(i));
            }
        }
    });
}

TEST_F(CompressChunkTest, TestFloatFilterStateful) {
    static constexpr size_t startOffset = 2 * 1024 + 7;
    static constexpr size_t numValuesToRead = DEFAULT_VECTOR_CAPACITY;
    std::vector<float> src(10 * 1024, 5.6);
    src[4] = 0;
    src[startOffset + 5] = 1234.5678 * 2345.6789 * 3456.7891;
    src[startOffset + 10] = 1234.5678 * 2345.6789 / 3456.7891;

    testCompressChunk(src, [](ColumnReadWriter* reader, transaction::Transaction* transaction,
                               ChunkState& state, const LogicalType& dataType) {
        common::ValueVector out{LogicalType::FLOAT()};

        // the filter will pass:
        // if the value is an exception, the 1st exception read
        // if the value is not an exception if it is in the 1st page that is read from
        auto filterFunc = [j = 0](offset_t, offset_t) mutable {
            ++j;
            return (j == 1);
        };
        reader->readCompressedValuesToVector(transaction, state, &out, 0, startOffset,
            startOffset + numValuesToRead, ReadCompressedValuesFromPageToVector(dataType),
            filterFunc);

        // the read call should not modify the functor
        EXPECT_TRUE(filterFunc(0, 0));
    });
}

TEST_F(CompressChunkTest, TestDoubleMultiPage) {
    std::vector<double> src(10 * 1024, 5.6);
    src[5] = 0;
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

TEST_F(CompressChunkTest, TestConstantExcludingExceptions) {
    std::vector<double> src(256, -0.54);
    src[1] = 1234567.890123456;
    testCheckWholeOutput(src);
}

TEST_F(CompressChunkTest, TestUncompressed) {
    std::vector<double> src({123456789.123456789, 987654321.987654321});
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
    src[5] = 0;
    src[2] = 54387589437957.834;
    for (size_t i = 102; i < src.size(); i += 6) {
        src[i] = src[i - 6] + 4385498.234;
    }

    testCheckWholeOutput(src);
}

TEST_F(CompressChunkTest, TestDoubleReadPartialAtOffsets) {
    std::vector<double> src(10 * 1024, 5.6);
    src[5] = 0;
    src[2] = 54387589437957.834;
    for (size_t i = 102; i < src.size(); i += 6) {
        src[i] = src[i - 6] + 4385498.234;
    }

    testCompressChunk(src, [&src](ColumnReadWriter* reader, transaction::Transaction* transaction,
                               ChunkState& state, const LogicalType& dataType) {
        std::vector<double> out(src.size());

        static constexpr size_t offsetInResult = 150;
        static constexpr size_t numValuesToRead = 200;
        const size_t offsetInSrc = src.size() - numValuesToRead;

        reader->readCompressedValuesToPage(transaction, state, (uint8_t*)out.data(), offsetInResult,
            offsetInSrc, offsetInSrc + numValuesToRead, ReadCompressedValuesFromPage(dataType));
        EXPECT_THAT(std::vector<double>(out.begin() + offsetInResult,
                        out.begin() + offsetInResult + numValuesToRead),
            ::testing::ContainerEq(std::vector<double>(src.begin() + offsetInSrc,
                src.begin() + offsetInSrc + numValuesToRead)));
    });
}

TEST_F(CompressChunkTest, TestDoubleReadPartialMultiPage) {
    std::vector<double> src(10 * 1024, 5.6);
    src[5] = 0;
    src[2] = 54387538437957.834;
    for (size_t i = 102; i < src.size(); i += 6) {
        src[i] = src[i - 6] + 9285498.231;
    }

    testCompressChunk(src, [&src](ColumnReadWriter* reader, transaction::Transaction* transaction,
                               ChunkState& state, const LogicalType& dataType) {
        std::vector<double> out(src.size());

        static constexpr size_t offsetInSrc = 1485;
        static constexpr size_t numValuesToRead = 3 * 1024 + 5;

        reader->readCompressedValuesToPage(transaction, state, (uint8_t*)out.data(), 0, offsetInSrc,
            offsetInSrc + numValuesToRead, ReadCompressedValuesFromPage(dataType),
            [](offset_t, offset_t) { return true; });
        EXPECT_THAT(std::vector<double>(out.begin(), out.begin() + numValuesToRead),
            ::testing::ContainerEq(std::vector<double>(src.begin() + offsetInSrc,
                src.begin() + offsetInSrc + numValuesToRead)));
    });
}

TEST_F(CompressChunkTest, TestDoubleReadPartialAtOffsetsIntoValueVector) {
    static constexpr size_t offsetInResult = 150;
    static constexpr size_t numValuesToRead = 200;
    if (DEFAULT_VECTOR_CAPACITY < offsetInResult + numValuesToRead) {
        GTEST_SKIP();
    }

    std::vector<double> src(512, -5.6);
    src[3] = -1;
    src[4] = -54387589437957.834;
    for (size_t i = 12; i < src.size(); i += 8) {
        src[i] = src[i - 8] - 4385498.234;
    }

    testCompressChunk(src, [&src](ColumnReadWriter* reader, transaction::Transaction* transaction,
                               ChunkState& state, const LogicalType& dataType) {
        common::ValueVector out{LogicalType::DOUBLE()};

        const size_t offsetInSrc = src.size() - numValuesToRead;

        reader->readCompressedValuesToVector(transaction, state, &out, offsetInResult, offsetInSrc,
            offsetInSrc + numValuesToRead, ReadCompressedValuesFromPageToVector(dataType),
            [](offset_t, offset_t) { return true; });

        for (size_t i = 0; i < numValuesToRead; ++i) {
            EXPECT_EQ(src[offsetInSrc + i], out.getValue<double>(offsetInResult + i));
        }
    });
}

TEST_F(CompressChunkTest, TestDoubleInPlaceUpdateNoExceptions) {
    std::vector<double> src(256, 5.6);

    if (DEFAULT_VECTOR_CAPACITY < src.size()) {
        GTEST_SKIP();
    }

    src[0] = 0;
    src[1] = 123456789012.56;
    for (size_t i = 11; i < src.size(); i += 10) {
        src[i] = src[i - 10] + 7890123.567;
    }

    testUpdateChunk(src, [&src](ColumnReadWriter* reader, transaction::Transaction*,
                             ChunkState& state, const LogicalType& dataType) {
        static constexpr size_t cpyOffset = 0;
        static constexpr size_t numValuesToSet = 10;
        for (size_t i = 0; i < numValuesToSet; ++i) {
            src[cpyOffset + i] = 5.7;
        }

        common::ValueVector in{LogicalType::FLOAT()};
        for (size_t i = 0; i < src.size(); ++i) {
            in.setValue<double>(i, src[i]);
        }

        InPlaceUpdateLocalState localUpdateState{};
        for (size_t i = cpyOffset; i < cpyOffset + numValuesToSet; ++i) {
            ASSERT_TRUE(state.metadata.compMeta.canUpdateInPlace((uint8_t*)src.data(), i, 1,
                dataType.getPhysicalType(), localUpdateState));
            reader->writeValueToPageFromVector(state, i, &in, i,
                WriteCompressedValuesToPage(dataType));

            // finalize after each update so that we can still update in place
            if (state.metadata.compMeta.compression == storage::CompressionType::ALP) {
                KU_ASSERT(nullptr != state.getExceptionChunk<double>());
                state.getExceptionChunk<double>()->finalizeAndFlushToDisk(state);
            }
        }
    });
}

TEST_F(CompressChunkTest, TestDoubleInPlaceUpdateWithExceptions) {
    std::vector<double> src(256, 5.6);
    src[0] = 0;
    src[1] = 123456789012.56;
    for (size_t i = 11; i < src.size(); i += 10) {
        src[i] = src[i - 10] + 7890123.567;
    }

    testUpdateChunk(src, [&src](ColumnReadWriter* reader, transaction::Transaction*,
                             ChunkState& state, const LogicalType& dataType) {
        static constexpr size_t numValuesToSet = 5;
        const size_t cpyOffset = 0;
        src[cpyOffset] = 10101010100101;
        for (size_t i = 2; i < numValuesToSet; i += 2) {
            src[cpyOffset + i] = src[cpyOffset + i - 2] + 1;
        }

        InPlaceUpdateLocalState localUpdateState{};
        ASSERT_TRUE(state.metadata.compMeta.canUpdateInPlace((uint8_t*)src.data(), cpyOffset,
            numValuesToSet, dataType.getPhysicalType(), localUpdateState));
        reader->writeValuesToPageFromBuffer(state, cpyOffset, (uint8_t*)src.data(), nullptr,
            cpyOffset, numValuesToSet, WriteCompressedValuesToPage(dataType));
    });
}

TEST_F(CompressChunkTest, TestDoubleInPlaceUpdateWithExceptionsManyUpdates) {
    std::vector<double> src(StorageConstants::NODE_GROUP_SIZE, 5.6);
    src[1] = 123456789012.56;
    src[3] = 1;
    for (size_t i = 11; i < src.size(); i += 10) {
        src[i] = src[i - 10] + 7890123.567;
    }

    testUpdateChunk(src, [&src](ColumnReadWriter* reader, transaction::Transaction*,
                             ChunkState& state, const LogicalType& dataType) {
        const size_t cpyOffset = 1;
        src[cpyOffset] = 10101010100101;
        for (size_t i = 11; i < src.size(); i += 10) {
            src[i] = src[i - 10] + 1;
        }

        // in practice canUpdateInPlace() will return false so we will typically not in-place update
        // in this case however it is still worth testing if it works
        reader->writeValuesToPageFromBuffer(state, 0, (uint8_t*)src.data(), nullptr, 0, src.size(),
            WriteCompressedValuesToPage(dataType));
    });
}

TEST_F(CompressChunkTest, TestDoubleInPlaceUpdateNoExceptionsMultiPage) {
    std::vector<double> src(10 * 1024, 5.6);
    src[5] = 0;
    src[1] = 123456789012.56;
    src[3] = 1;
    for (size_t i = 11; i < src.size(); i += 10) {
        src[i] = src[i - 10] + 7890123.567;
    }

    testUpdateChunk(src, [&src](ColumnReadWriter* reader, transaction::Transaction*,
                             ChunkState& state, const LogicalType& dataType) {
        static constexpr size_t numValuesToSet = 1000;
        const size_t cpyOffset = src.size() - numValuesToSet;
        for (size_t i = 0; i < numValuesToSet; ++i) {
            src[cpyOffset + i] = 5.7;
        }

        InPlaceUpdateLocalState localUpdateState{};
        ASSERT_TRUE(state.metadata.compMeta.canUpdateInPlace((uint8_t*)src.data(), cpyOffset,
            numValuesToSet, dataType.getPhysicalType(), localUpdateState));
        reader->writeValuesToPageFromBuffer(state, cpyOffset, (uint8_t*)src.data(), nullptr,
            cpyOffset, numValuesToSet, WriteCompressedValuesToPage(dataType));
    });
}

TEST_F(CompressChunkTest, TestDoubleInPlaceUpdateWithExceptionsMultiPage) {
    // numValues not not fit in uint16
    std::vector<double> src(1 << 17, 5.6);
    src[1] = 123456789012.56;
    src[3] = 1;
    for (size_t i = 11; i < src.size(); i += 10) {
        src[i] = src[i - 10] + 7890123.567;
    }

    testUpdateChunk(src, [&src](ColumnReadWriter* reader, transaction::Transaction*,
                             ChunkState& state, const LogicalType& dataType) {
        static constexpr size_t numValuesToSet = 1000;
        const size_t cpyOffset = 2100;
        src[cpyOffset] = 10101010100101;
        for (size_t i = 2; i < numValuesToSet; i += 2) {
            src[cpyOffset + i] = src[cpyOffset + i - 2] + 1;
        }

        InPlaceUpdateLocalState localUpdateState{};
        ASSERT_TRUE(state.metadata.compMeta.canUpdateInPlace((uint8_t*)src.data(), cpyOffset,
            numValuesToSet, dataType.getPhysicalType(), localUpdateState));
        reader->writeValuesToPageFromBuffer(state, cpyOffset, (uint8_t*)src.data(), nullptr,
            cpyOffset, numValuesToSet, WriteCompressedValuesToPage(dataType));
    });
}

TEST_F(CompressChunkTest, TestDoubleInPlaceUpdateWithExceptionsMultiPageNullMask) {
    if (!inMemMode) {
        std::vector<double> src(4 * 1024, 5.6);
        src[1] = 123456789012.56;
        // ALP doesn't sample every value when tuning parameters
        // so we set multiple to 1 just to make sure that at least one is detected
        // this will make sure we aren't using constant compression for the bitpacked values
        std::fill(src.begin() + 2, src.begin() + 11, 0);
        for (size_t i = 11; i < src.size(); i += 10) {
            src[i] = src[i - 10] + 7890123.567;
        }

        testCompressChunk(src, [&src, this](ColumnReadWriter* reader,
                                   transaction::Transaction* transaction, ChunkState& state,
                                   const LogicalType& dataType) {
            auto* mm = getMemoryManager(*database);
            auto* storageManager = getStorageManager(*database);
            auto* dataFH = storageManager->getDataFH();

            static constexpr size_t numValuesToSet = 50;
            const size_t cpyOffset = 2100;
            src[cpyOffset] = 10101010;
            for (size_t i = 1; i < numValuesToSet; i++) {
                src[cpyOffset + i] = src[cpyOffset + i - 1] + 1;
            }

            std::optional<NullMask> nullMask(src.size());
            nullMask->setNullFromRange(0, cpyOffset, true);
            InPlaceUpdateLocalState localUpdateState{};
            ASSERT_TRUE(state.metadata.compMeta.canUpdateInPlace((uint8_t*)src.data(), 0,
                cpyOffset + numValuesToSet, dataType.getPhysicalType(), localUpdateState,
                nullMask));
            reader->writeValuesToPageFromBuffer(state, 0, (uint8_t*)src.data(), &nullMask.value(),
                0, cpyOffset + numValuesToSet, WriteCompressedValuesToPage(dataType));

            commitUpdate<double>(transaction, state, dataFH, mm, &storageManager->getShadowFile());

            // our compression algorithms don't actually respect the null mask but we can check if
            // the non-null values match
            std::vector<double> out(numValuesToSet);
            reader->readCompressedValuesToPage(transaction, state, (uint8_t*)out.data(), 0,
                cpyOffset, cpyOffset + numValuesToSet, ReadCompressedValuesFromPage(dataType));
            EXPECT_THAT(out, ::testing::ContainerEq(std::vector<double>(src.begin() + cpyOffset,
                                 src.begin() + cpyOffset + numValuesToSet)));
        });
    }
}

TEST_F(CompressChunkTest, TestInPlaceUpdateConstant) {
    std::vector<double> src(256, 0.54);
    testCompressChunk(src, [](ColumnReadWriter*, transaction::Transaction*, ChunkState& state,
                               const LogicalType& dataType) {
        double newVal = -1;
        InPlaceUpdateLocalState localUpdateState{};
        EXPECT_FALSE(state.metadata.compMeta.canUpdateInPlace((uint8_t*)&newVal, 0, 1,
            dataType.getPhysicalType(), localUpdateState));
    });
}

TEST_F(CompressChunkTest, TestInPlaceUpdateConstantExcludingExceptions) {
    std::vector<float> src(256, 1);
    src[1] = 1234567.7;
    testCompressChunk(src, [](ColumnReadWriter*, transaction::Transaction*, ChunkState& state,
                               const LogicalType& dataType) {
        float newVal = 1.2;
        InPlaceUpdateLocalState localUpdateState{};
        EXPECT_FALSE(state.metadata.compMeta.canUpdateInPlace((uint8_t*)&newVal, 0, 1,
            dataType.getPhysicalType(), localUpdateState));
    });
}

TEST_F(CompressChunkTest, TestFloatBeforeInPlaceUpdateManyExceptionsNoCompress) {
    std::vector<float> src(100);
    src[0] = 543875.8341;
    for (size_t i = 1; i < src.size(); i += 1) {
        src[i] = (src[i - 1] + 43.2348) * 0.9788;
    }

    testUpdateChunk(src, [&src](ColumnReadWriter* reader, transaction::Transaction*,
                             ChunkState& state, const LogicalType& dataType) {
        static constexpr size_t cpyOffset = 3;
        const size_t numValuesToSet = src.size() - cpyOffset;
        for (size_t i = cpyOffset; i < cpyOffset + numValuesToSet; ++i) {
            src[i] = i + 0.01;
        }

        InPlaceUpdateLocalState localUpdateState{};
        ASSERT_TRUE(state.metadata.compMeta.canUpdateInPlace((uint8_t*)src.data(), cpyOffset,
            numValuesToSet, dataType.getPhysicalType(), localUpdateState));
        reader->writeValuesToPageFromBuffer(state, cpyOffset, (uint8_t*)src.data(), nullptr,
            cpyOffset, numValuesToSet, WriteCompressedValuesToPage(dataType));
    });
}

} // namespace testing
} // namespace kuzu
