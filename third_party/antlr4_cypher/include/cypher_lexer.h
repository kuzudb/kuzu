
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
    T__44 = 45, T__45 = 46, GLOB = 47, COPY = 48, FROM = 49, FROMNPY = 50, 
    NODE = 51, TABLE = 52, DROP = 53, ALTER = 54, DEFAULT = 55, RENAME = 56, 
    ADD = 57, PRIMARY = 58, KEY = 59, REL = 60, TO = 61, EXPLAIN = 62, PROFILE = 63, 
    UNION = 64, ALL = 65, OPTIONAL = 66, MATCH = 67, UNWIND = 68, CREATE = 69, 
    SET = 70, DELETE = 71, WITH = 72, RETURN = 73, DISTINCT = 74, STAR = 75, 
    AS = 76, ORDER = 77, BY = 78, L_SKIP = 79, LIMIT = 80, ASCENDING = 81, 
    ASC = 82, DESCENDING = 83, DESC = 84, WHERE = 85, OR = 86, XOR = 87, 
    AND = 88, NOT = 89, INVALID_NOT_EQUAL = 90, MINUS = 91, FACTORIAL = 92, 
    STARTS = 93, ENDS = 94, CONTAINS = 95, IS = 96, NULL_ = 97, TRUE = 98, 
    FALSE = 99, EXISTS = 100, CASE = 101, ELSE = 102, END = 103, WHEN = 104, 
    THEN = 105, StringLiteral = 106, EscapedChar = 107, DecimalInteger = 108, 
    HexLetter = 109, HexDigit = 110, Digit = 111, NonZeroDigit = 112, NonZeroOctDigit = 113, 
    ZeroDigit = 114, RegularDecimalReal = 115, UnescapedSymbolicName = 116, 
    IdentifierStart = 117, IdentifierPart = 118, EscapedSymbolicName = 119, 
    SP = 120, WHITESPACE = 121, Comment = 122, Unknown = 123
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

