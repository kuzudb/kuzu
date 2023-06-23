
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
    T__44 = 45, T__45 = 46, CALL = 47, GLOB = 48, COPY = 49, FROM = 50, 
    NPY = 51, COLUMN = 52, NODE = 53, TABLE = 54, DROP = 55, ALTER = 56, 
    DEFAULT = 57, RENAME = 58, ADD = 59, PRIMARY = 60, KEY = 61, REL = 62, 
    TO = 63, EXPLAIN = 64, PROFILE = 65, UNION = 66, ALL = 67, OPTIONAL = 68, 
    MATCH = 69, UNWIND = 70, CREATE = 71, SET = 72, DELETE = 73, WITH = 74, 
    RETURN = 75, DISTINCT = 76, STAR = 77, AS = 78, ORDER = 79, BY = 80, 
    L_SKIP = 81, LIMIT = 82, ASCENDING = 83, ASC = 84, DESCENDING = 85, 
    DESC = 86, WHERE = 87, SHORTEST = 88, OR = 89, XOR = 90, AND = 91, NOT = 92, 
    INVALID_NOT_EQUAL = 93, MINUS = 94, FACTORIAL = 95, STARTS = 96, ENDS = 97, 
    CONTAINS = 98, IS = 99, NULL_ = 100, TRUE = 101, FALSE = 102, EXISTS = 103, 
    CASE = 104, ELSE = 105, END = 106, WHEN = 107, THEN = 108, StringLiteral = 109, 
    EscapedChar = 110, DecimalInteger = 111, HexLetter = 112, HexDigit = 113, 
    Digit = 114, NonZeroDigit = 115, NonZeroOctDigit = 116, ZeroDigit = 117, 
    RegularDecimalReal = 118, UnescapedSymbolicName = 119, IdentifierStart = 120, 
    IdentifierPart = 121, EscapedSymbolicName = 122, SP = 123, WHITESPACE = 124, 
    Comment = 125, Unknown = 126
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

