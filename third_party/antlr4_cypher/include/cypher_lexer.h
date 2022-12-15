
// Generated from /Users/xiyangfeng/kuzu/kuzu/src/antlr4/Cypher.g4 by ANTLR 4.9

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
    T__44 = 45, COPY = 46, FROM = 47, NODE = 48, TABLE = 49, DROP = 50, 
    PRIMARY = 51, KEY = 52, REL = 53, TO = 54, EXPLAIN = 55, PROFILE = 56, 
    UNION = 57, ALL = 58, OPTIONAL = 59, MATCH = 60, UNWIND = 61, CREATE = 62, 
    SET = 63, DELETE = 64, WITH = 65, RETURN = 66, DISTINCT = 67, STAR = 68, 
    AS = 69, ORDER = 70, BY = 71, L_SKIP = 72, LIMIT = 73, ASCENDING = 74, 
    ASC = 75, DESCENDING = 76, DESC = 77, WHERE = 78, OR = 79, XOR = 80, 
    AND = 81, NOT = 82, INVALID_NOT_EQUAL = 83, MINUS = 84, FACTORIAL = 85, 
    STARTS = 86, ENDS = 87, CONTAINS = 88, IS = 89, NULL_ = 90, TRUE = 91, 
    FALSE = 92, EXISTS = 93, CASE = 94, ELSE = 95, END = 96, WHEN = 97, 
    THEN = 98, StringLiteral = 99, EscapedChar = 100, DecimalInteger = 101, 
    HexLetter = 102, HexDigit = 103, Digit = 104, NonZeroDigit = 105, NonZeroOctDigit = 106, 
    ZeroDigit = 107, RegularDecimalReal = 108, UnescapedSymbolicName = 109, 
    IdentifierStart = 110, IdentifierPart = 111, EscapedSymbolicName = 112, 
    SP = 113, WHITESPACE = 114, Comment = 115, Unknown = 116
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

