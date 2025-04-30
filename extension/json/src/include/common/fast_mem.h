#pragma once

#include <cstring>

template<size_t SIZE>
inline void memCpyFixed(void* dest, const void* src) {
    memcpy(dest, src, SIZE);
}

template<size_t SIZE>
inline int memCmpFixed(const void* str1, const void* str2) {
    return memcmp(str1, str2, SIZE);
}

template<size_t SIZE>
inline void memSetFixed(void* ptr, int value) {
    memset(ptr, value, SIZE);
}

namespace kuzu {
namespace json_extension {

// This templated memcpy is significantly faster than std::memcpy, but only when you are calling
// memcpy with a const size in a loop. For instance `while (<cond>) { memcpy(<dest>, <src>,
// const_size); ... }`
inline void fastMemcpy(void* dest, const void* src, const size_t size) {
    // LCOV_EXCL_START
    switch (size) {
    case 0:
        return;
    case 1:
        return memCpyFixed<1>(dest, src);
    case 2:
        return memCpyFixed<2>(dest, src);
    case 3:
        return memCpyFixed<3>(dest, src);
    case 4:
        return memCpyFixed<4>(dest, src);
    case 5:
        return memCpyFixed<5>(dest, src);
    case 6:
        return memCpyFixed<6>(dest, src);
    case 7:
        return memCpyFixed<7>(dest, src);
    case 8:
        return memCpyFixed<8>(dest, src);
    case 9:
        return memCpyFixed<9>(dest, src);
    case 10:
        return memCpyFixed<10>(dest, src);
    case 11:
        return memCpyFixed<11>(dest, src);
    case 12:
        return memCpyFixed<12>(dest, src);
    case 13:
        return memCpyFixed<13>(dest, src);
    case 14:
        return memCpyFixed<14>(dest, src);
    case 15:
        return memCpyFixed<15>(dest, src);
    case 16:
        return memCpyFixed<16>(dest, src);
    case 17:
        return memCpyFixed<17>(dest, src);
    case 18:
        return memCpyFixed<18>(dest, src);
    case 19:
        return memCpyFixed<19>(dest, src);
    case 20:
        return memCpyFixed<20>(dest, src);
    case 21:
        return memCpyFixed<21>(dest, src);
    case 22:
        return memCpyFixed<22>(dest, src);
    case 23:
        return memCpyFixed<23>(dest, src);
    case 24:
        return memCpyFixed<24>(dest, src);
    case 25:
        return memCpyFixed<25>(dest, src);
    case 26:
        return memCpyFixed<26>(dest, src);
    case 27:
        return memCpyFixed<27>(dest, src);
    case 28:
        return memCpyFixed<28>(dest, src);
    case 29:
        return memCpyFixed<29>(dest, src);
    case 30:
        return memCpyFixed<30>(dest, src);
    case 31:
        return memCpyFixed<31>(dest, src);
    case 32:
        return memCpyFixed<32>(dest, src);
    case 33:
        return memCpyFixed<33>(dest, src);
    case 34:
        return memCpyFixed<34>(dest, src);
    case 35:
        return memCpyFixed<35>(dest, src);
    case 36:
        return memCpyFixed<36>(dest, src);
    case 37:
        return memCpyFixed<37>(dest, src);
    case 38:
        return memCpyFixed<38>(dest, src);
    case 39:
        return memCpyFixed<39>(dest, src);
    case 40:
        return memCpyFixed<40>(dest, src);
    case 41:
        return memCpyFixed<41>(dest, src);
    case 42:
        return memCpyFixed<42>(dest, src);
    case 43:
        return memCpyFixed<43>(dest, src);
    case 44:
        return memCpyFixed<44>(dest, src);
    case 45:
        return memCpyFixed<45>(dest, src);
    case 46:
        return memCpyFixed<46>(dest, src);
    case 47:
        return memCpyFixed<47>(dest, src);
    case 48:
        return memCpyFixed<48>(dest, src);
    case 49:
        return memCpyFixed<49>(dest, src);
    case 50:
        return memCpyFixed<50>(dest, src);
    case 51:
        return memCpyFixed<51>(dest, src);
    case 52:
        return memCpyFixed<52>(dest, src);
    case 53:
        return memCpyFixed<53>(dest, src);
    case 54:
        return memCpyFixed<54>(dest, src);
    case 55:
        return memCpyFixed<55>(dest, src);
    case 56:
        return memCpyFixed<56>(dest, src);
    case 57:
        return memCpyFixed<57>(dest, src);
    case 58:
        return memCpyFixed<58>(dest, src);
    case 59:
        return memCpyFixed<59>(dest, src);
    case 60:
        return memCpyFixed<60>(dest, src);
    case 61:
        return memCpyFixed<61>(dest, src);
    case 62:
        return memCpyFixed<62>(dest, src);
    case 63:
        return memCpyFixed<63>(dest, src);
    case 64:
        return memCpyFixed<64>(dest, src);
    case 65:
        return memCpyFixed<65>(dest, src);
    case 66:
        return memCpyFixed<66>(dest, src);
    case 67:
        return memCpyFixed<67>(dest, src);
    case 68:
        return memCpyFixed<68>(dest, src);
    case 69:
        return memCpyFixed<69>(dest, src);
    case 70:
        return memCpyFixed<70>(dest, src);
    case 71:
        return memCpyFixed<71>(dest, src);
    case 72:
        return memCpyFixed<72>(dest, src);
    case 73:
        return memCpyFixed<73>(dest, src);
    case 74:
        return memCpyFixed<74>(dest, src);
    case 75:
        return memCpyFixed<75>(dest, src);
    case 76:
        return memCpyFixed<76>(dest, src);
    case 77:
        return memCpyFixed<77>(dest, src);
    case 78:
        return memCpyFixed<78>(dest, src);
    case 79:
        return memCpyFixed<79>(dest, src);
    case 80:
        return memCpyFixed<80>(dest, src);
    case 81:
        return memCpyFixed<81>(dest, src);
    case 82:
        return memCpyFixed<82>(dest, src);
    case 83:
        return memCpyFixed<83>(dest, src);
    case 84:
        return memCpyFixed<84>(dest, src);
    case 85:
        return memCpyFixed<85>(dest, src);
    case 86:
        return memCpyFixed<86>(dest, src);
    case 87:
        return memCpyFixed<87>(dest, src);
    case 88:
        return memCpyFixed<88>(dest, src);
    case 89:
        return memCpyFixed<89>(dest, src);
    case 90:
        return memCpyFixed<90>(dest, src);
    case 91:
        return memCpyFixed<91>(dest, src);
    case 92:
        return memCpyFixed<92>(dest, src);
    case 93:
        return memCpyFixed<93>(dest, src);
    case 94:
        return memCpyFixed<94>(dest, src);
    case 95:
        return memCpyFixed<95>(dest, src);
    case 96:
        return memCpyFixed<96>(dest, src);
    case 97:
        return memCpyFixed<97>(dest, src);
    case 98:
        return memCpyFixed<98>(dest, src);
    case 99:
        return memCpyFixed<99>(dest, src);
    case 100:
        return memCpyFixed<100>(dest, src);
    case 101:
        return memCpyFixed<101>(dest, src);
    case 102:
        return memCpyFixed<102>(dest, src);
    case 103:
        return memCpyFixed<103>(dest, src);
    case 104:
        return memCpyFixed<104>(dest, src);
    case 105:
        return memCpyFixed<105>(dest, src);
    case 106:
        return memCpyFixed<106>(dest, src);
    case 107:
        return memCpyFixed<107>(dest, src);
    case 108:
        return memCpyFixed<108>(dest, src);
    case 109:
        return memCpyFixed<109>(dest, src);
    case 110:
        return memCpyFixed<110>(dest, src);
    case 111:
        return memCpyFixed<111>(dest, src);
    case 112:
        return memCpyFixed<112>(dest, src);
    case 113:
        return memCpyFixed<113>(dest, src);
    case 114:
        return memCpyFixed<114>(dest, src);
    case 115:
        return memCpyFixed<115>(dest, src);
    case 116:
        return memCpyFixed<116>(dest, src);
    case 117:
        return memCpyFixed<117>(dest, src);
    case 118:
        return memCpyFixed<118>(dest, src);
    case 119:
        return memCpyFixed<119>(dest, src);
    case 120:
        return memCpyFixed<120>(dest, src);
    case 121:
        return memCpyFixed<121>(dest, src);
    case 122:
        return memCpyFixed<122>(dest, src);
    case 123:
        return memCpyFixed<123>(dest, src);
    case 124:
        return memCpyFixed<124>(dest, src);
    case 125:
        return memCpyFixed<125>(dest, src);
    case 126:
        return memCpyFixed<126>(dest, src);
    case 127:
        return memCpyFixed<127>(dest, src);
    case 128:
        return memCpyFixed<128>(dest, src);
    case 129:
        return memCpyFixed<129>(dest, src);
    case 130:
        return memCpyFixed<130>(dest, src);
    case 131:
        return memCpyFixed<131>(dest, src);
    case 132:
        return memCpyFixed<132>(dest, src);
    case 133:
        return memCpyFixed<133>(dest, src);
    case 134:
        return memCpyFixed<134>(dest, src);
    case 135:
        return memCpyFixed<135>(dest, src);
    case 136:
        return memCpyFixed<136>(dest, src);
    case 137:
        return memCpyFixed<137>(dest, src);
    case 138:
        return memCpyFixed<138>(dest, src);
    case 139:
        return memCpyFixed<139>(dest, src);
    case 140:
        return memCpyFixed<140>(dest, src);
    case 141:
        return memCpyFixed<141>(dest, src);
    case 142:
        return memCpyFixed<142>(dest, src);
    case 143:
        return memCpyFixed<143>(dest, src);
    case 144:
        return memCpyFixed<144>(dest, src);
    case 145:
        return memCpyFixed<145>(dest, src);
    case 146:
        return memCpyFixed<146>(dest, src);
    case 147:
        return memCpyFixed<147>(dest, src);
    case 148:
        return memCpyFixed<148>(dest, src);
    case 149:
        return memCpyFixed<149>(dest, src);
    case 150:
        return memCpyFixed<150>(dest, src);
    case 151:
        return memCpyFixed<151>(dest, src);
    case 152:
        return memCpyFixed<152>(dest, src);
    case 153:
        return memCpyFixed<153>(dest, src);
    case 154:
        return memCpyFixed<154>(dest, src);
    case 155:
        return memCpyFixed<155>(dest, src);
    case 156:
        return memCpyFixed<156>(dest, src);
    case 157:
        return memCpyFixed<157>(dest, src);
    case 158:
        return memCpyFixed<158>(dest, src);
    case 159:
        return memCpyFixed<159>(dest, src);
    case 160:
        return memCpyFixed<160>(dest, src);
    case 161:
        return memCpyFixed<161>(dest, src);
    case 162:
        return memCpyFixed<162>(dest, src);
    case 163:
        return memCpyFixed<163>(dest, src);
    case 164:
        return memCpyFixed<164>(dest, src);
    case 165:
        return memCpyFixed<165>(dest, src);
    case 166:
        return memCpyFixed<166>(dest, src);
    case 167:
        return memCpyFixed<167>(dest, src);
    case 168:
        return memCpyFixed<168>(dest, src);
    case 169:
        return memCpyFixed<169>(dest, src);
    case 170:
        return memCpyFixed<170>(dest, src);
    case 171:
        return memCpyFixed<171>(dest, src);
    case 172:
        return memCpyFixed<172>(dest, src);
    case 173:
        return memCpyFixed<173>(dest, src);
    case 174:
        return memCpyFixed<174>(dest, src);
    case 175:
        return memCpyFixed<175>(dest, src);
    case 176:
        return memCpyFixed<176>(dest, src);
    case 177:
        return memCpyFixed<177>(dest, src);
    case 178:
        return memCpyFixed<178>(dest, src);
    case 179:
        return memCpyFixed<179>(dest, src);
    case 180:
        return memCpyFixed<180>(dest, src);
    case 181:
        return memCpyFixed<181>(dest, src);
    case 182:
        return memCpyFixed<182>(dest, src);
    case 183:
        return memCpyFixed<183>(dest, src);
    case 184:
        return memCpyFixed<184>(dest, src);
    case 185:
        return memCpyFixed<185>(dest, src);
    case 186:
        return memCpyFixed<186>(dest, src);
    case 187:
        return memCpyFixed<187>(dest, src);
    case 188:
        return memCpyFixed<188>(dest, src);
    case 189:
        return memCpyFixed<189>(dest, src);
    case 190:
        return memCpyFixed<190>(dest, src);
    case 191:
        return memCpyFixed<191>(dest, src);
    case 192:
        return memCpyFixed<192>(dest, src);
    case 193:
        return memCpyFixed<193>(dest, src);
    case 194:
        return memCpyFixed<194>(dest, src);
    case 195:
        return memCpyFixed<195>(dest, src);
    case 196:
        return memCpyFixed<196>(dest, src);
    case 197:
        return memCpyFixed<197>(dest, src);
    case 198:
        return memCpyFixed<198>(dest, src);
    case 199:
        return memCpyFixed<199>(dest, src);
    case 200:
        return memCpyFixed<200>(dest, src);
    case 201:
        return memCpyFixed<201>(dest, src);
    case 202:
        return memCpyFixed<202>(dest, src);
    case 203:
        return memCpyFixed<203>(dest, src);
    case 204:
        return memCpyFixed<204>(dest, src);
    case 205:
        return memCpyFixed<205>(dest, src);
    case 206:
        return memCpyFixed<206>(dest, src);
    case 207:
        return memCpyFixed<207>(dest, src);
    case 208:
        return memCpyFixed<208>(dest, src);
    case 209:
        return memCpyFixed<209>(dest, src);
    case 210:
        return memCpyFixed<210>(dest, src);
    case 211:
        return memCpyFixed<211>(dest, src);
    case 212:
        return memCpyFixed<212>(dest, src);
    case 213:
        return memCpyFixed<213>(dest, src);
    case 214:
        return memCpyFixed<214>(dest, src);
    case 215:
        return memCpyFixed<215>(dest, src);
    case 216:
        return memCpyFixed<216>(dest, src);
    case 217:
        return memCpyFixed<217>(dest, src);
    case 218:
        return memCpyFixed<218>(dest, src);
    case 219:
        return memCpyFixed<219>(dest, src);
    case 220:
        return memCpyFixed<220>(dest, src);
    case 221:
        return memCpyFixed<221>(dest, src);
    case 222:
        return memCpyFixed<222>(dest, src);
    case 223:
        return memCpyFixed<223>(dest, src);
    case 224:
        return memCpyFixed<224>(dest, src);
    case 225:
        return memCpyFixed<225>(dest, src);
    case 226:
        return memCpyFixed<226>(dest, src);
    case 227:
        return memCpyFixed<227>(dest, src);
    case 228:
        return memCpyFixed<228>(dest, src);
    case 229:
        return memCpyFixed<229>(dest, src);
    case 230:
        return memCpyFixed<230>(dest, src);
    case 231:
        return memCpyFixed<231>(dest, src);
    case 232:
        return memCpyFixed<232>(dest, src);
    case 233:
        return memCpyFixed<233>(dest, src);
    case 234:
        return memCpyFixed<234>(dest, src);
    case 235:
        return memCpyFixed<235>(dest, src);
    case 236:
        return memCpyFixed<236>(dest, src);
    case 237:
        return memCpyFixed<237>(dest, src);
    case 238:
        return memCpyFixed<238>(dest, src);
    case 239:
        return memCpyFixed<239>(dest, src);
    case 240:
        return memCpyFixed<240>(dest, src);
    case 241:
        return memCpyFixed<241>(dest, src);
    case 242:
        return memCpyFixed<242>(dest, src);
    case 243:
        return memCpyFixed<243>(dest, src);
    case 244:
        return memCpyFixed<244>(dest, src);
    case 245:
        return memCpyFixed<245>(dest, src);
    case 246:
        return memCpyFixed<246>(dest, src);
    case 247:
        return memCpyFixed<247>(dest, src);
    case 248:
        return memCpyFixed<248>(dest, src);
    case 249:
        return memCpyFixed<249>(dest, src);
    case 250:
        return memCpyFixed<250>(dest, src);
    case 251:
        return memCpyFixed<251>(dest, src);
    case 252:
        return memCpyFixed<252>(dest, src);
    case 253:
        return memCpyFixed<253>(dest, src);
    case 254:
        return memCpyFixed<254>(dest, src);
    case 255:
        return memCpyFixed<255>(dest, src);
    case 256:
        return memCpyFixed<256>(dest, src);
    default:
        memcpy(dest, src, size);
    }
    // LCOV_EXCL_STOP
}

//! This templated memcmp is significantly faster than std::memcmp,
//! but only when you are calling memcmp with a const size in a loop.
//! For instance `while (<cond>) { memcmp(<str1>, <str2>, const_size); ... }`
inline int FastMemcmp(const void* str1, const void* str2, const size_t size) {
    // LCOV_EXCL_START
    switch (size) {
    case 0:
        return 0;
    case 1:
        return memCmpFixed<1>(str1, str2);
    case 2:
        return memCmpFixed<2>(str1, str2);
    case 3:
        return memCmpFixed<3>(str1, str2);
    case 4:
        return memCmpFixed<4>(str1, str2);
    case 5:
        return memCmpFixed<5>(str1, str2);
    case 6:
        return memCmpFixed<6>(str1, str2);
    case 7:
        return memCmpFixed<7>(str1, str2);
    case 8:
        return memCmpFixed<8>(str1, str2);
    case 9:
        return memCmpFixed<9>(str1, str2);
    case 10:
        return memCmpFixed<10>(str1, str2);
    case 11:
        return memCmpFixed<11>(str1, str2);
    case 12:
        return memCmpFixed<12>(str1, str2);
    case 13:
        return memCmpFixed<13>(str1, str2);
    case 14:
        return memCmpFixed<14>(str1, str2);
    case 15:
        return memCmpFixed<15>(str1, str2);
    case 16:
        return memCmpFixed<16>(str1, str2);
    case 17:
        return memCmpFixed<17>(str1, str2);
    case 18:
        return memCmpFixed<18>(str1, str2);
    case 19:
        return memCmpFixed<19>(str1, str2);
    case 20:
        return memCmpFixed<20>(str1, str2);
    case 21:
        return memCmpFixed<21>(str1, str2);
    case 22:
        return memCmpFixed<22>(str1, str2);
    case 23:
        return memCmpFixed<23>(str1, str2);
    case 24:
        return memCmpFixed<24>(str1, str2);
    case 25:
        return memCmpFixed<25>(str1, str2);
    case 26:
        return memCmpFixed<26>(str1, str2);
    case 27:
        return memCmpFixed<27>(str1, str2);
    case 28:
        return memCmpFixed<28>(str1, str2);
    case 29:
        return memCmpFixed<29>(str1, str2);
    case 30:
        return memCmpFixed<30>(str1, str2);
    case 31:
        return memCmpFixed<31>(str1, str2);
    case 32:
        return memCmpFixed<32>(str1, str2);
    case 33:
        return memCmpFixed<33>(str1, str2);
    case 34:
        return memCmpFixed<34>(str1, str2);
    case 35:
        return memCmpFixed<35>(str1, str2);
    case 36:
        return memCmpFixed<36>(str1, str2);
    case 37:
        return memCmpFixed<37>(str1, str2);
    case 38:
        return memCmpFixed<38>(str1, str2);
    case 39:
        return memCmpFixed<39>(str1, str2);
    case 40:
        return memCmpFixed<40>(str1, str2);
    case 41:
        return memCmpFixed<41>(str1, str2);
    case 42:
        return memCmpFixed<42>(str1, str2);
    case 43:
        return memCmpFixed<43>(str1, str2);
    case 44:
        return memCmpFixed<44>(str1, str2);
    case 45:
        return memCmpFixed<45>(str1, str2);
    case 46:
        return memCmpFixed<46>(str1, str2);
    case 47:
        return memCmpFixed<47>(str1, str2);
    case 48:
        return memCmpFixed<48>(str1, str2);
    case 49:
        return memCmpFixed<49>(str1, str2);
    case 50:
        return memCmpFixed<50>(str1, str2);
    case 51:
        return memCmpFixed<51>(str1, str2);
    case 52:
        return memCmpFixed<52>(str1, str2);
    case 53:
        return memCmpFixed<53>(str1, str2);
    case 54:
        return memCmpFixed<54>(str1, str2);
    case 55:
        return memCmpFixed<55>(str1, str2);
    case 56:
        return memCmpFixed<56>(str1, str2);
    case 57:
        return memCmpFixed<57>(str1, str2);
    case 58:
        return memCmpFixed<58>(str1, str2);
    case 59:
        return memCmpFixed<59>(str1, str2);
    case 60:
        return memCmpFixed<60>(str1, str2);
    case 61:
        return memCmpFixed<61>(str1, str2);
    case 62:
        return memCmpFixed<62>(str1, str2);
    case 63:
        return memCmpFixed<63>(str1, str2);
    case 64:
        return memCmpFixed<64>(str1, str2);
    default:
        return memcmp(str1, str2, size);
    }
    // LCOV_EXCL_STOP
}

inline void fastMemset(void* ptr, int value, size_t size) {
    // LCOV_EXCL_START
    switch (size) {
    case 0:
        return;
    case 1:
        return memSetFixed<1>(ptr, value);
    case 2:
        return memSetFixed<2>(ptr, value);
    case 3:
        return memSetFixed<3>(ptr, value);
    case 4:
        return memSetFixed<4>(ptr, value);
    case 5:
        return memSetFixed<5>(ptr, value);
    case 6:
        return memSetFixed<6>(ptr, value);
    case 7:
        return memSetFixed<7>(ptr, value);
    case 8:
        return memSetFixed<8>(ptr, value);
    case 9:
        return memSetFixed<9>(ptr, value);
    case 10:
        return memSetFixed<10>(ptr, value);
    case 11:
        return memSetFixed<11>(ptr, value);
    case 12:
        return memSetFixed<12>(ptr, value);
    case 13:
        return memSetFixed<13>(ptr, value);
    case 14:
        return memSetFixed<14>(ptr, value);
    case 15:
        return memSetFixed<15>(ptr, value);
    case 16:
        return memSetFixed<16>(ptr, value);
    case 17:
        return memSetFixed<17>(ptr, value);
    case 18:
        return memSetFixed<18>(ptr, value);
    case 19:
        return memSetFixed<19>(ptr, value);
    case 20:
        return memSetFixed<20>(ptr, value);
    case 21:
        return memSetFixed<21>(ptr, value);
    case 22:
        return memSetFixed<22>(ptr, value);
    case 23:
        return memSetFixed<23>(ptr, value);
    case 24:
        return memSetFixed<24>(ptr, value);
    case 25:
        return memSetFixed<25>(ptr, value);
    case 26:
        return memSetFixed<26>(ptr, value);
    case 27:
        return memSetFixed<27>(ptr, value);
    case 28:
        return memSetFixed<28>(ptr, value);
    case 29:
        return memSetFixed<29>(ptr, value);
    case 30:
        return memSetFixed<30>(ptr, value);
    case 31:
        return memSetFixed<31>(ptr, value);
    case 32:
        return memSetFixed<32>(ptr, value);
    case 33:
        return memSetFixed<33>(ptr, value);
    case 34:
        return memSetFixed<34>(ptr, value);
    case 35:
        return memSetFixed<35>(ptr, value);
    case 36:
        return memSetFixed<36>(ptr, value);
    case 37:
        return memSetFixed<37>(ptr, value);
    case 38:
        return memSetFixed<38>(ptr, value);
    case 39:
        return memSetFixed<39>(ptr, value);
    case 40:
        return memSetFixed<40>(ptr, value);
    case 41:
        return memSetFixed<41>(ptr, value);
    case 42:
        return memSetFixed<42>(ptr, value);
    case 43:
        return memSetFixed<43>(ptr, value);
    case 44:
        return memSetFixed<44>(ptr, value);
    case 45:
        return memSetFixed<45>(ptr, value);
    case 46:
        return memSetFixed<46>(ptr, value);
    case 47:
        return memSetFixed<47>(ptr, value);
    case 48:
        return memSetFixed<48>(ptr, value);
    case 49:
        return memSetFixed<49>(ptr, value);
    case 50:
        return memSetFixed<50>(ptr, value);
    case 51:
        return memSetFixed<51>(ptr, value);
    case 52:
        return memSetFixed<52>(ptr, value);
    case 53:
        return memSetFixed<53>(ptr, value);
    case 54:
        return memSetFixed<54>(ptr, value);
    case 55:
        return memSetFixed<55>(ptr, value);
    case 56:
        return memSetFixed<56>(ptr, value);
    case 57:
        return memSetFixed<57>(ptr, value);
    case 58:
        return memSetFixed<58>(ptr, value);
    case 59:
        return memSetFixed<59>(ptr, value);
    case 60:
        return memSetFixed<60>(ptr, value);
    case 61:
        return memSetFixed<61>(ptr, value);
    case 62:
        return memSetFixed<62>(ptr, value);
    case 63:
        return memSetFixed<63>(ptr, value);
    case 64:
        return memSetFixed<64>(ptr, value);
    case 65:
        return memSetFixed<65>(ptr, value);
    case 66:
        return memSetFixed<66>(ptr, value);
    case 67:
        return memSetFixed<67>(ptr, value);
    case 68:
        return memSetFixed<68>(ptr, value);
    case 69:
        return memSetFixed<69>(ptr, value);
    case 70:
        return memSetFixed<70>(ptr, value);
    case 71:
        return memSetFixed<71>(ptr, value);
    case 72:
        return memSetFixed<72>(ptr, value);
    case 73:
        return memSetFixed<73>(ptr, value);
    case 74:
        return memSetFixed<74>(ptr, value);
    case 75:
        return memSetFixed<75>(ptr, value);
    case 76:
        return memSetFixed<76>(ptr, value);
    case 77:
        return memSetFixed<77>(ptr, value);
    case 78:
        return memSetFixed<78>(ptr, value);
    case 79:
        return memSetFixed<79>(ptr, value);
    case 80:
        return memSetFixed<80>(ptr, value);
    case 81:
        return memSetFixed<81>(ptr, value);
    case 82:
        return memSetFixed<82>(ptr, value);
    case 83:
        return memSetFixed<83>(ptr, value);
    case 84:
        return memSetFixed<84>(ptr, value);
    case 85:
        return memSetFixed<85>(ptr, value);
    case 86:
        return memSetFixed<86>(ptr, value);
    case 87:
        return memSetFixed<87>(ptr, value);
    case 88:
        return memSetFixed<88>(ptr, value);
    case 89:
        return memSetFixed<89>(ptr, value);
    case 90:
        return memSetFixed<90>(ptr, value);
    case 91:
        return memSetFixed<91>(ptr, value);
    case 92:
        return memSetFixed<92>(ptr, value);
    case 93:
        return memSetFixed<93>(ptr, value);
    case 94:
        return memSetFixed<94>(ptr, value);
    case 95:
        return memSetFixed<95>(ptr, value);
    case 96:
        return memSetFixed<96>(ptr, value);
    case 97:
        return memSetFixed<97>(ptr, value);
    case 98:
        return memSetFixed<98>(ptr, value);
    case 99:
        return memSetFixed<99>(ptr, value);
    case 100:
        return memSetFixed<100>(ptr, value);
    case 101:
        return memSetFixed<101>(ptr, value);
    case 102:
        return memSetFixed<102>(ptr, value);
    case 103:
        return memSetFixed<103>(ptr, value);
    case 104:
        return memSetFixed<104>(ptr, value);
    case 105:
        return memSetFixed<105>(ptr, value);
    case 106:
        return memSetFixed<106>(ptr, value);
    case 107:
        return memSetFixed<107>(ptr, value);
    case 108:
        return memSetFixed<108>(ptr, value);
    case 109:
        return memSetFixed<109>(ptr, value);
    case 110:
        return memSetFixed<110>(ptr, value);
    case 111:
        return memSetFixed<111>(ptr, value);
    case 112:
        return memSetFixed<112>(ptr, value);
    case 113:
        return memSetFixed<113>(ptr, value);
    case 114:
        return memSetFixed<114>(ptr, value);
    case 115:
        return memSetFixed<115>(ptr, value);
    case 116:
        return memSetFixed<116>(ptr, value);
    case 117:
        return memSetFixed<117>(ptr, value);
    case 118:
        return memSetFixed<118>(ptr, value);
    case 119:
        return memSetFixed<119>(ptr, value);
    case 120:
        return memSetFixed<120>(ptr, value);
    case 121:
        return memSetFixed<121>(ptr, value);
    case 122:
        return memSetFixed<122>(ptr, value);
    case 123:
        return memSetFixed<123>(ptr, value);
    case 124:
        return memSetFixed<124>(ptr, value);
    case 125:
        return memSetFixed<125>(ptr, value);
    case 126:
        return memSetFixed<126>(ptr, value);
    case 127:
        return memSetFixed<127>(ptr, value);
    case 128:
        return memSetFixed<128>(ptr, value);
    case 129:
        return memSetFixed<129>(ptr, value);
    case 130:
        return memSetFixed<130>(ptr, value);
    case 131:
        return memSetFixed<131>(ptr, value);
    case 132:
        return memSetFixed<132>(ptr, value);
    case 133:
        return memSetFixed<133>(ptr, value);
    case 134:
        return memSetFixed<134>(ptr, value);
    case 135:
        return memSetFixed<135>(ptr, value);
    case 136:
        return memSetFixed<136>(ptr, value);
    case 137:
        return memSetFixed<137>(ptr, value);
    case 138:
        return memSetFixed<138>(ptr, value);
    case 139:
        return memSetFixed<139>(ptr, value);
    case 140:
        return memSetFixed<140>(ptr, value);
    case 141:
        return memSetFixed<141>(ptr, value);
    case 142:
        return memSetFixed<142>(ptr, value);
    case 143:
        return memSetFixed<143>(ptr, value);
    case 144:
        return memSetFixed<144>(ptr, value);
    case 145:
        return memSetFixed<145>(ptr, value);
    case 146:
        return memSetFixed<146>(ptr, value);
    case 147:
        return memSetFixed<147>(ptr, value);
    case 148:
        return memSetFixed<148>(ptr, value);
    case 149:
        return memSetFixed<149>(ptr, value);
    case 150:
        return memSetFixed<150>(ptr, value);
    case 151:
        return memSetFixed<151>(ptr, value);
    case 152:
        return memSetFixed<152>(ptr, value);
    case 153:
        return memSetFixed<153>(ptr, value);
    case 154:
        return memSetFixed<154>(ptr, value);
    case 155:
        return memSetFixed<155>(ptr, value);
    case 156:
        return memSetFixed<156>(ptr, value);
    case 157:
        return memSetFixed<157>(ptr, value);
    case 158:
        return memSetFixed<158>(ptr, value);
    case 159:
        return memSetFixed<159>(ptr, value);
    case 160:
        return memSetFixed<160>(ptr, value);
    case 161:
        return memSetFixed<161>(ptr, value);
    case 162:
        return memSetFixed<162>(ptr, value);
    case 163:
        return memSetFixed<163>(ptr, value);
    case 164:
        return memSetFixed<164>(ptr, value);
    case 165:
        return memSetFixed<165>(ptr, value);
    case 166:
        return memSetFixed<166>(ptr, value);
    case 167:
        return memSetFixed<167>(ptr, value);
    case 168:
        return memSetFixed<168>(ptr, value);
    case 169:
        return memSetFixed<169>(ptr, value);
    case 170:
        return memSetFixed<170>(ptr, value);
    case 171:
        return memSetFixed<171>(ptr, value);
    case 172:
        return memSetFixed<172>(ptr, value);
    case 173:
        return memSetFixed<173>(ptr, value);
    case 174:
        return memSetFixed<174>(ptr, value);
    case 175:
        return memSetFixed<175>(ptr, value);
    case 176:
        return memSetFixed<176>(ptr, value);
    case 177:
        return memSetFixed<177>(ptr, value);
    case 178:
        return memSetFixed<178>(ptr, value);
    case 179:
        return memSetFixed<179>(ptr, value);
    case 180:
        return memSetFixed<180>(ptr, value);
    case 181:
        return memSetFixed<181>(ptr, value);
    case 182:
        return memSetFixed<182>(ptr, value);
    case 183:
        return memSetFixed<183>(ptr, value);
    case 184:
        return memSetFixed<184>(ptr, value);
    case 185:
        return memSetFixed<185>(ptr, value);
    case 186:
        return memSetFixed<186>(ptr, value);
    case 187:
        return memSetFixed<187>(ptr, value);
    case 188:
        return memSetFixed<188>(ptr, value);
    case 189:
        return memSetFixed<189>(ptr, value);
    case 190:
        return memSetFixed<190>(ptr, value);
    case 191:
        return memSetFixed<191>(ptr, value);
    case 192:
        return memSetFixed<192>(ptr, value);
    case 193:
        return memSetFixed<193>(ptr, value);
    case 194:
        return memSetFixed<194>(ptr, value);
    case 195:
        return memSetFixed<195>(ptr, value);
    case 196:
        return memSetFixed<196>(ptr, value);
    case 197:
        return memSetFixed<197>(ptr, value);
    case 198:
        return memSetFixed<198>(ptr, value);
    case 199:
        return memSetFixed<199>(ptr, value);
    case 200:
        return memSetFixed<200>(ptr, value);
    case 201:
        return memSetFixed<201>(ptr, value);
    case 202:
        return memSetFixed<202>(ptr, value);
    case 203:
        return memSetFixed<203>(ptr, value);
    case 204:
        return memSetFixed<204>(ptr, value);
    case 205:
        return memSetFixed<205>(ptr, value);
    case 206:
        return memSetFixed<206>(ptr, value);
    case 207:
        return memSetFixed<207>(ptr, value);
    case 208:
        return memSetFixed<208>(ptr, value);
    case 209:
        return memSetFixed<209>(ptr, value);
    case 210:
        return memSetFixed<210>(ptr, value);
    case 211:
        return memSetFixed<211>(ptr, value);
    case 212:
        return memSetFixed<212>(ptr, value);
    case 213:
        return memSetFixed<213>(ptr, value);
    case 214:
        return memSetFixed<214>(ptr, value);
    case 215:
        return memSetFixed<215>(ptr, value);
    case 216:
        return memSetFixed<216>(ptr, value);
    case 217:
        return memSetFixed<217>(ptr, value);
    case 218:
        return memSetFixed<218>(ptr, value);
    case 219:
        return memSetFixed<219>(ptr, value);
    case 220:
        return memSetFixed<220>(ptr, value);
    case 221:
        return memSetFixed<221>(ptr, value);
    case 222:
        return memSetFixed<222>(ptr, value);
    case 223:
        return memSetFixed<223>(ptr, value);
    case 224:
        return memSetFixed<224>(ptr, value);
    case 225:
        return memSetFixed<225>(ptr, value);
    case 226:
        return memSetFixed<226>(ptr, value);
    case 227:
        return memSetFixed<227>(ptr, value);
    case 228:
        return memSetFixed<228>(ptr, value);
    case 229:
        return memSetFixed<229>(ptr, value);
    case 230:
        return memSetFixed<230>(ptr, value);
    case 231:
        return memSetFixed<231>(ptr, value);
    case 232:
        return memSetFixed<232>(ptr, value);
    case 233:
        return memSetFixed<233>(ptr, value);
    case 234:
        return memSetFixed<234>(ptr, value);
    case 235:
        return memSetFixed<235>(ptr, value);
    case 236:
        return memSetFixed<236>(ptr, value);
    case 237:
        return memSetFixed<237>(ptr, value);
    case 238:
        return memSetFixed<238>(ptr, value);
    case 239:
        return memSetFixed<239>(ptr, value);
    case 240:
        return memSetFixed<240>(ptr, value);
    case 241:
        return memSetFixed<241>(ptr, value);
    case 242:
        return memSetFixed<242>(ptr, value);
    case 243:
        return memSetFixed<243>(ptr, value);
    case 244:
        return memSetFixed<244>(ptr, value);
    case 245:
        return memSetFixed<245>(ptr, value);
    case 246:
        return memSetFixed<246>(ptr, value);
    case 247:
        return memSetFixed<247>(ptr, value);
    case 248:
        return memSetFixed<248>(ptr, value);
    case 249:
        return memSetFixed<249>(ptr, value);
    case 250:
        return memSetFixed<250>(ptr, value);
    case 251:
        return memSetFixed<251>(ptr, value);
    case 252:
        return memSetFixed<252>(ptr, value);
    case 253:
        return memSetFixed<253>(ptr, value);
    case 254:
        return memSetFixed<254>(ptr, value);
    case 255:
        return memSetFixed<255>(ptr, value);
    case 256:
        return memSetFixed<256>(ptr, value);
    default:
        memset(ptr, value, size);
    }
    // LCOV_EXCL_STOP
}

} // namespace json_extension
} // namespace kuzu
