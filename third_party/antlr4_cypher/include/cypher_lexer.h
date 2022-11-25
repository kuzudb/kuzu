
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
    T__38 = 39, T__39 = 40, T__40 = 41, T__41 = 42, COPY = 43, FROM = 44, 
    NODE = 45, TABLE = 46, DROP = 47, PRIMARY = 48, KEY = 49, REL = 50, 
    TO = 51, EXPLAIN = 52, PROFILE = 53, UNION = 54, ALL = 55, OPTIONAL = 56, 
    MATCH = 57, UNWIND = 58, CREATE = 59, SET = 60, DELETE = 61, WITH = 62, 
    RETURN = 63, DISTINCT = 64, STAR = 65, AS = 66, ORDER = 67, BY = 68, 
    L_SKIP = 69, LIMIT = 70, ASCENDING = 71, ASC = 72, DESCENDING = 73, 
    DESC = 74, WHERE = 75, OR = 76, XOR = 77, AND = 78, NOT = 79, INVALID_NOT_EQUAL = 80, 
    MINUS = 81, STARTS = 82, ENDS = 83, CONTAINS = 84, IS = 85, NULL_ = 86, 
    TRUE = 87, FALSE = 88, EXISTS = 89, StringLiteral = 90, EscapedChar = 91, 
    DecimalInteger = 92, HexLetter = 93, HexDigit = 94, Digit = 95, NonZeroDigit = 96, 
    NonZeroOctDigit = 97, ZeroDigit = 98, RegularDecimalReal = 99, UnescapedSymbolicName = 100, 
    IdentifierStart = 101, IdentifierPart = 102, EscapedSymbolicName = 103, 
    SP = 104, WHITESPACE = 105, Comment = 106, Unknown = 107
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

