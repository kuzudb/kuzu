
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
    ASC = 83, DESCENDING = 84, DESC = 85, WHERE = 86, OR = 87, XOR = 88, 
    AND = 89, NOT = 90, INVALID_NOT_EQUAL = 91, MINUS = 92, FACTORIAL = 93, 
    STARTS = 94, ENDS = 95, CONTAINS = 96, IS = 97, NULL_ = 98, TRUE = 99, 
    FALSE = 100, EXISTS = 101, CASE = 102, ELSE = 103, END = 104, WHEN = 105, 
    THEN = 106, StringLiteral = 107, EscapedChar = 108, DecimalInteger = 109, 
    HexLetter = 110, HexDigit = 111, Digit = 112, NonZeroDigit = 113, NonZeroOctDigit = 114, 
    ZeroDigit = 115, RegularDecimalReal = 116, UnescapedSymbolicName = 117, 
    IdentifierStart = 118, IdentifierPart = 119, EscapedSymbolicName = 120, 
    SP = 121, WHITESPACE = 122, Comment = 123, Unknown = 124
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

