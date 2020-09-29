#include "src/main/graphflow/storage/include/Column.h"

#include <unordered_map>
#include <unordered_set>

#include "gtest/gtest.h"
#include "src/main/graphflow/utils/types.h"

using namespace graphflow::storage;
using namespace graphflow::utils;

TEST(ColumnTests, ColumnIntegerTest) {
	gfNodeOffset_t offsets[] = {0, 1, 32767, 32768, (1u << 20u) - 1};
	int numElements = sizeof(offsets)/sizeof(gfNodeOffset_t);
	auto *col = new ColumnInteger(0 /*nodeLabel*/, 1u << 20u /*numElements*/, NULL_GFINT);
	for (int i = 0; i < numElements; i++) {
		col->setVal(offsets[i], offsets[i]);
	}
	auto offsetsSet = new std::unordered_set<gfNodeOffset_t>(offsets, offsets + numElements);
	for (gfNodeOffset_t i = 0; i < 1u << 20u; i++) {
		if (offsetsSet->find(i) != offsetsSet->end()) {
			EXPECT_EQ(col->getVal(i), i);
		} else {
			EXPECT_EQ(col->getVal(i), NULL_GFINT);
		}
	}
}

TEST(ColumnTests, ColumnDoubleTest) {
	gfNodeOffset_t offsets[] = {0, 1, 32767, 32768, (1u << 20u) - 1};
	int numElements = sizeof(offsets)/sizeof(gfNodeOffset_t);
	auto *col = new ColumnDouble(0 /*nodeLabel*/, 1u << 20u /*numElements*/, NULL_GFDOUBLE);
	for (int i = 0; i < numElements; i++) {
		col->setVal(offsets[i], sqrt(offsets[i]));
	}
	auto offsetsSet = new std::unordered_set<gfNodeOffset_t>(offsets, offsets + numElements);
	for (gfNodeOffset_t i = 0; i < 1u << 20u; i++) {
		if (offsetsSet->find(i) != offsetsSet->end()) {
			EXPECT_EQ(col->getVal(i), sqrt(i));
		} else {
			EXPECT_EQ(col->getVal(i), NULL_GFDOUBLE);
		}
	}
}

TEST(ColumnTests, ColumnBooleanTest) {
	gfNodeOffset_t offsets[] = {0, 1, 32767, 32768, (1u << 20u) - 1};
	int numElements = sizeof(offsets)/sizeof(gfNodeOffset_t);
	auto *col = new ColumnBoolean(0 /*nodeLabel*/, 1u << 20u /*numElements*/, NULL_GFBOOL);
	auto offsetsMap = new std::unordered_map<gfNodeOffset_t, gfBool_t >();
	for (int i = 0; i < numElements; i++) {
		offsetsMap->insert({offsets[i], (offsets[i] % 2) + 1});
		col->setVal(offsets[i], (offsets[i] % 2) + 1);
	}
	for (gfNodeOffset_t i = 0; i < 1u << 20u; i++) {
		if (offsetsMap->find(i) != offsetsMap->end()) {
			EXPECT_EQ(col->getVal(i), offsetsMap->find(i)->second);
		} else {
			EXPECT_EQ(col->getVal(i), NULL_GFBOOL);
		}
	}
}

TEST(ColumnTests, ColumnStringTest) {
	gfNodeOffset_t offsets[] = {0, 1, 32767, 32768, (1u << 20u) - 1};
	gfString_t stringVals[] = {new std::string("Chomsky"), new std::string("Foucault"),
							new std::string("Derrida"), new std::string("Montaigne"),
							new std::string("Nietzsche")};
	int numElements = sizeof(offsets)/sizeof(gfNodeOffset_t);
	auto *col = new ColumnString(0 /*nodeLabel*/, 1u << 20u /*numElements*/, NULL_gfSTRING);
	auto offsetsMap = new std::unordered_map<gfNodeOffset_t, gfString_t>();
	for (int i = 0; i < numElements; i++) {
		offsetsMap->insert({offsets[i], stringVals[i]});
		col->setVal(offsets[i], stringVals[i]);
	}
	for (gfNodeOffset_t i = 0; i < 1u << 20u; i++) {
		if (offsetsMap->find(i) != offsetsMap->end()) {
			auto found = offsetsMap->find(i);
			EXPECT_EQ(col->getVal(i), found->second);
		} else {
			EXPECT_EQ(col->getVal(i), NULL_gfSTRING);
		}
	}
}
