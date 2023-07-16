
// Generated from Cypher.g4 by ANTLR 4.9

#pragma once


#include "antlr4-runtime.h"




class  CypherLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, T__23 = 24, T__24 = 25, T__25 = 26, 
    T__26 = 27, T__27 = 28, T__28 = 29, T__29 = 30, T__30 = 31, T__31 = 32, 
    T__32 = 33, T__33 = 34, T__34 = 35, T__35 = 36, T__36 = 37, T__37 = 38, 
    T__38 = 39, T__39 = 40, T__40 = 41, T__41 = 42, T__42 = 43, T__43 = 44, 
    T__44 = 45, T__45 = 46, T__46 = 47, CALL = 48, GLOB = 49, COPY = 50, 
    FROM = 51, NPY = 52, COLUMN = 53, NODE = 54, TABLE = 55, DROP = 56, 
    ALTER = 57, DEFAULT = 58, RENAME = 59, ADD = 60, PRIMARY = 61, KEY = 62, 
    REL = 63, TO = 64, EXPLAIN = 65, PROFILE = 66, UNION = 67, ALL = 68, 
    OPTIONAL = 69, MATCH = 70, UNWIND = 71, CREATE = 72, SET = 73, DELETE = 74, 
    WITH = 75, RETURN = 76, DISTINCT = 77, STAR = 78, AS = 79, ORDER = 80, 
    BY = 81, L_SKIP = 82, LIMIT = 83, ASCENDING = 84, ASC = 85, DESCENDING = 86, 
    DESC = 87, WHERE = 88, SHORTEST = 89, OR = 90, XOR = 91, AND = 92, NOT = 93, 
    INVALID_NOT_EQUAL = 94, MINUS = 95, FACTORIAL = 96, STARTS = 97, ENDS = 98, 
    CONTAINS = 99, IS = 100, NULL_ = 101, TRUE = 102, FALSE = 103, EXISTS = 104, 
    CASE = 105, ELSE = 106, END = 107, WHEN = 108, THEN = 109, StringLiteral = 110, 
    EscapedChar = 111, DecimalInteger = 112, HexLetter = 113, HexDigit = 114, 
    Digit = 115, NonZeroDigit = 116, NonZeroOctDigit = 117, ZeroDigit = 118, 
    RegularDecimalReal = 119, UnescapedSymbolicName = 120, IdentifierStart = 121, 
    IdentifierPart = 122, EscapedSymbolicName = 123, SP = 124, WHITESPACE = 125, 
    Comment = 126, Unknown = 127
  };

  explicit CypherLexer(antlr4::CharStream *input);
  ~CypherLexer();

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames() const override; // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

