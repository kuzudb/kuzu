
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
    T__44 = 45, T__45 = 46, GLOB = 47, COPY = 48, FROM = 49, NPY = 50, COLUMN = 51, 
    NODE = 52, TABLE = 53, DROP = 54, ALTER = 55, DEFAULT = 56, RENAME = 57, 
    ADD = 58, PRIMARY = 59, KEY = 60, REL = 61, TO = 62, EXPLAIN = 63, PROFILE = 64, 
    UNION = 65, ALL = 66, OPTIONAL = 67, MATCH = 68, UNWIND = 69, CREATE = 70, 
    SET = 71, DELETE = 72, WITH = 73, RETURN = 74, DISTINCT = 75, STAR = 76, 
    AS = 77, ORDER = 78, BY = 79, L_SKIP = 80, LIMIT = 81, ASCENDING = 82, 
    ASC = 83, DESCENDING = 84, DESC = 85, WHERE = 86, SHORTEST = 87, OR = 88, 
    XOR = 89, AND = 90, NOT = 91, INVALID_NOT_EQUAL = 92, MINUS = 93, FACTORIAL = 94, 
    STARTS = 95, ENDS = 96, CONTAINS = 97, IS = 98, NULL_ = 99, TRUE = 100, 
    FALSE = 101, EXISTS = 102, CASE = 103, ELSE = 104, END = 105, WHEN = 106, 
    THEN = 107, StringLiteral = 108, EscapedChar = 109, DecimalInteger = 110, 
    HexLetter = 111, HexDigit = 112, Digit = 113, NonZeroDigit = 114, NonZeroOctDigit = 115, 
    ZeroDigit = 116, RegularDecimalReal = 117, UnescapedSymbolicName = 118, 
    IdentifierStart = 119, IdentifierPart = 120, EscapedSymbolicName = 121, 
    SP = 122, WHITESPACE = 123, Comment = 124, Unknown = 125
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

