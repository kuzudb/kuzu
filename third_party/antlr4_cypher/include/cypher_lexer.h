
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
    T__44 = 45, T__45 = 46, GLOB = 47, COPY = 48, FROM = 49, NODE = 50, 
    TABLE = 51, DROP = 52, ALTER = 53, DEFAULT = 54, RENAME = 55, ADD = 56, 
    PRIMARY = 57, KEY = 58, REL = 59, TO = 60, EXPLAIN = 61, PROFILE = 62, 
    UNION = 63, ALL = 64, OPTIONAL = 65, MATCH = 66, UNWIND = 67, CREATE = 68, 
    SET = 69, DELETE = 70, WITH = 71, RETURN = 72, DISTINCT = 73, STAR = 74, 
    AS = 75, ORDER = 76, BY = 77, L_SKIP = 78, LIMIT = 79, ASCENDING = 80, 
    ASC = 81, DESCENDING = 82, DESC = 83, WHERE = 84, OR = 85, XOR = 86, 
    AND = 87, NOT = 88, INVALID_NOT_EQUAL = 89, MINUS = 90, FACTORIAL = 91, 
    STARTS = 92, ENDS = 93, CONTAINS = 94, IS = 95, NULL_ = 96, TRUE = 97, 
    FALSE = 98, EXISTS = 99, CASE = 100, ELSE = 101, END = 102, WHEN = 103, 
    THEN = 104, StringLiteral = 105, EscapedChar = 106, DecimalInteger = 107, 
    HexLetter = 108, HexDigit = 109, Digit = 110, NonZeroDigit = 111, NonZeroOctDigit = 112, 
    ZeroDigit = 113, RegularDecimalReal = 114, UnescapedSymbolicName = 115, 
    IdentifierStart = 116, IdentifierPart = 117, EscapedSymbolicName = 118, 
    SP = 119, WHITESPACE = 120, Comment = 121, Unknown = 122
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

