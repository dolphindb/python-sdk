/*
 * DictionaryImp.h
 *
 *  Created on: Oct 23, 2013
 *      Author: dzhou
 */

#ifndef DICTIONARYIMP_H_
#define DICTIONARYIMP_H_

#include "Util.h"
#include <unordered_map>
#include "Dictionary.h"
namespace dolphindb {

typedef void (*U8VectorReader)(const ConstantSP& value, int start, int count, U8* output);
typedef void (*U8ScalarReader)(const ConstantSP& value, U8& output);
typedef void (*U8VectorWriter)(U8* input, const ConstantSP& output, int start, int count);
typedef void (*U8ScalarWriter)(const U8& value, const ConstantSP& output);

class AbstractDictionary: public Dictionary{
public:
	AbstractDictionary(DATA_TYPE keyType, DATA_TYPE valueType):type_(valueType),keyType_(keyType){
		internalType_=Util::convertToIntegralDataType(valueType);
		keyCategory_=Util::getCategory(keyType_);
		init();
	}

	~AbstractDictionary() override{}
	DATA_TYPE getType() const override {return type_;}
	DATA_TYPE getRawType() const override {return internalType_ == DT_SYMBOL ? DT_INT : internalType_;}
	DATA_CATEGORY getCategory() const override {return Util::getCategory(type_);}
	DATA_TYPE getKeyType() const override {return keyType_;}
	DATA_CATEGORY getKeyCategory() const override {return keyCategory_;}
	const string& getStringRef() const override {throw RuntimeException("dictionary doesn't support random access.");}
	ConstantSP get(INDEX  /*index*/) const override {throw RuntimeException("dictionary doesn't support random access.");}
	ConstantSP get(INDEX  /*column*/, INDEX  /*row*/) const override {throw RuntimeException("dictionary doesn't support random access.");}
	ConstantSP getColumn(INDEX  /*index*/) const override {throw RuntimeException("dictionary doesn't support random access.");}
	ConstantSP getRow(INDEX  /*index*/) const override {throw RuntimeException("dictionary doesn't support random access.");}
	ConstantSP getItem(INDEX  /*index*/) const override {throw RuntimeException("dictionary doesn't support random access.");}

protected:
	void init();
	ConstantSP createValues(const ConstantSP& keys) const;

protected:
	DATA_TYPE internalType_;
	DATA_TYPE type_;
	DATA_TYPE keyType_;
	DATA_CATEGORY keyCategory_;
	U8VectorReader vreader_;
	U8ScalarReader sreader_;
	U8VectorWriter vwriter_;
	U8ScalarWriter swriter_;
	U8 nullVal_;
};

class CharDictionary: public AbstractDictionary{
public:
	CharDictionary(DATA_TYPE keyType, DATA_TYPE type):AbstractDictionary(keyType,type){}
	CharDictionary(const std::unordered_map<char,U8>& dict, DATA_TYPE keyType, DATA_TYPE type);
	~CharDictionary() override;
	void clear() override{dict_.clear();}
	INDEX size() const override {return (INDEX)dict_.size();}
	INDEX count() const override {return (INDEX)dict_.size();}
	ConstantSP getInstance() const override { return new CharDictionary(keyType_,type_);}
	ConstantSP getValue() const override {return new CharDictionary(dict_,keyType_,type_);}
	bool set(const ConstantSP& key, const ConstantSP& value) override;
	bool remove(const ConstantSP& value) override;
	ConstantSP getMember(const ConstantSP& key) const override;
	ConstantSP keys() const override;
	ConstantSP values() const override;
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	string getString() const override;
	long long getAllocatedMemory() const override;
private:
	std::unordered_map<char,U8> dict_;
};

class ShortDictionary: public AbstractDictionary{
public:
	ShortDictionary(DATA_TYPE keyType, DATA_TYPE type):AbstractDictionary(keyType,type){}
	ShortDictionary(const std::unordered_map<short,U8>& dict, DATA_TYPE keyType, DATA_TYPE type);
	~ShortDictionary() override;
	void clear() override{dict_.clear();}
	INDEX size() const override {return (INDEX)dict_.size();}
	INDEX count() const override {return (INDEX)dict_.size();}
	ConstantSP getInstance() const override { return new ShortDictionary(keyType_,type_);}
	ConstantSP getValue() const override {return new ShortDictionary(dict_,keyType_,type_);}
	bool set(const ConstantSP& key, const ConstantSP& value) override;
	bool remove(const ConstantSP& value) override;
	ConstantSP getMember(const ConstantSP& key) const override;
	ConstantSP keys() const override;
	ConstantSP values() const override;
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	string getString() const override;
	long long getAllocatedMemory() const override;
private:
	std::unordered_map<short,U8> dict_;
};

class IntDictionary: public AbstractDictionary{
public:
	IntDictionary(DATA_TYPE keyType, DATA_TYPE type):AbstractDictionary(keyType,type){}
	IntDictionary(const std::unordered_map<int,U8>& dict, DATA_TYPE keyType, DATA_TYPE type);
	~IntDictionary() override;
	void clear() override{dict_.clear();}
	INDEX size() const override {return (INDEX)dict_.size();}
	INDEX count() const override {return (INDEX)dict_.size();}
	ConstantSP getInstance() const override { return new IntDictionary(keyType_,type_);}
	ConstantSP getValue() const override {return new IntDictionary(dict_,keyType_,type_);}
	bool set(const ConstantSP& key, const ConstantSP& value) override;
	bool remove(const ConstantSP& value) override;
	ConstantSP getMember(const ConstantSP& key) const override;
	ConstantSP keys() const override;
	ConstantSP values() const override;
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	string getString() const override;
	long long getAllocatedMemory() const override;
private:
	std::unordered_map<int,U8> dict_;
};

class LongDictionary: public AbstractDictionary{
public:
	LongDictionary(DATA_TYPE keyType, DATA_TYPE type):AbstractDictionary(keyType,type){}
	LongDictionary(const std::unordered_map<long long,U8>& dict, DATA_TYPE keyType,DATA_TYPE type);
	~LongDictionary() override;
	void clear() override{dict_.clear();}
	INDEX size() const override {return (INDEX)dict_.size();}
	INDEX count() const override {return (INDEX)dict_.size();}
	ConstantSP getInstance() const override { return new LongDictionary(keyType_,type_);}
	ConstantSP getValue() const override {return new LongDictionary(dict_,keyType_,type_);}
	bool set(const ConstantSP& key, const ConstantSP& value) override;
	bool remove(const ConstantSP& value) override;
	ConstantSP getMember(const ConstantSP& key) const override;
	ConstantSP keys() const override;
	ConstantSP values() const override;
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	string getString() const override;
	long long getAllocatedMemory() const override;
private:
	std::unordered_map<long long,U8> dict_;
};

class FloatDictionary: public AbstractDictionary{
public:
	FloatDictionary(DATA_TYPE type):AbstractDictionary(DT_FLOAT,type){}
	FloatDictionary(const std::unordered_map<float,U8>& dict, DATA_TYPE type);
	~FloatDictionary() override;
	void clear() override{dict_.clear();}
	INDEX size() const override {return (INDEX)dict_.size();}
	INDEX count() const override {return (INDEX)dict_.size();}
	ConstantSP getInstance() const override { return new FloatDictionary(type_);}
	ConstantSP getValue() const override {return new FloatDictionary(dict_,type_);}
	bool set(const ConstantSP& key, const ConstantSP& value) override;
	bool remove(const ConstantSP& value) override;
	ConstantSP getMember(const ConstantSP& key) const override;
	ConstantSP keys() const override;
	ConstantSP values() const override;
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	string getString() const override;
	long long getAllocatedMemory() const override;
private:
	std::unordered_map<float,U8> dict_;
};

class DoubleDictionary: public AbstractDictionary{
public:
	DoubleDictionary(DATA_TYPE type):AbstractDictionary(DT_DOUBLE,type){}
	DoubleDictionary(const std::unordered_map<double,U8>& dict, DATA_TYPE type);
	~DoubleDictionary() override;
	void clear() override{dict_.clear();}
	INDEX size() const override {return (INDEX)dict_.size();}
	INDEX count() const override {return (INDEX)dict_.size();}
	ConstantSP getInstance() const override { return new DoubleDictionary(type_);}
	ConstantSP getValue() const override {return new DoubleDictionary(dict_,type_);}
	bool set(const ConstantSP& key, const ConstantSP& value) override;
	bool remove(const ConstantSP& value) override;
	ConstantSP getMember(const ConstantSP& key) const override;
	ConstantSP keys() const override;
	ConstantSP values() const override;
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	string getString() const override;
	long long getAllocatedMemory() const override;
private:
	std::unordered_map<double,U8> dict_;
};

class StringDictionary: public AbstractDictionary{
public:
	StringDictionary(DATA_TYPE keyType, DATA_TYPE type):AbstractDictionary(keyType,type){}
	StringDictionary(const std::unordered_map<string,U8>& dict, DATA_TYPE keyType,DATA_TYPE type);
	~StringDictionary() override;
	void clear() override{dict_.clear();}
	INDEX size() const override {return (INDEX)dict_.size();}
	INDEX count() const override {return (INDEX)dict_.size();}
	ConstantSP getInstance() const override { return new StringDictionary(keyType_,type_);}
	ConstantSP getValue() const override {return new StringDictionary(dict_,keyType_,type_);}
	bool set(const string& key, const ConstantSP& value) override;
	bool set(const ConstantSP& key, const ConstantSP& value) override;
	bool remove(const ConstantSP& value) override;
	ConstantSP getMember(const ConstantSP& key) const override;
	ConstantSP getMember(const string& key) const override;
	ConstantSP keys() const override;
	ConstantSP values() const override;
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	string getString() const override;
	long long getAllocatedMemory() const override;
private:
	std::unordered_map<string,U8> dict_;
};

class Int128Dictionary: public AbstractDictionary{
public:
	Int128Dictionary(DATA_TYPE keyType, DATA_TYPE type):AbstractDictionary(keyType,type){}
	Int128Dictionary(const std::unordered_map<Guid,U8>& dict, DATA_TYPE keyType, DATA_TYPE type);
	std::unordered_map<Guid,U8>& getInternalDict() { return dict_;}
	~Int128Dictionary() override;
	void clear() override{dict_.clear();}
	INDEX size() const override {return (INDEX)dict_.size();}
	INDEX count() const override {return (INDEX)dict_.size();}
	ConstantSP getInstance() const override { return new Int128Dictionary(keyType_,type_);}
	ConstantSP getValue() const override {return new Int128Dictionary(dict_,keyType_,type_);}
	bool set(const ConstantSP& key, const ConstantSP& value) override;
	bool remove(const ConstantSP& value) override;
	ConstantSP getMember(const ConstantSP& key) const override;
	ConstantSP keys() const override;
	ConstantSP values() const override;
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	string getString() const override;
	long long getAllocatedMemory() const override;
private:
	std::unordered_map<Guid,U8> dict_;
};

class AnyDictionary: public AbstractDictionary{
public:
	AnyDictionary():AbstractDictionary(DT_STRING,DT_ANY){}
	AnyDictionary(const std::unordered_map<string,ConstantSP>& dict):AbstractDictionary(DT_STRING,DT_ANY),dict_(dict){}
	~AnyDictionary() override{};
	void clear() override{dict_.clear();}
	INDEX size() const override {return (INDEX)dict_.size();}
	INDEX count() const override {return (INDEX)dict_.size();}
	ConstantSP getInstance() const override { return new AnyDictionary();}
	ConstantSP getValue() const override {return new AnyDictionary(dict_);}
	bool set(const string& key, const ConstantSP& value) override;
	bool set(const ConstantSP& key, const ConstantSP& value) override;
	bool remove(const ConstantSP& value) override;
	ConstantSP getMember(const string& key) const override;
	ConstantSP getMember(const ConstantSP& key) const override;
	ConstantSP keys() const override;
	ConstantSP values() const override;
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	string getString() const override;
	long long getAllocatedMemory() const override;
	bool containNotMarshallableObject() const override;
private:
	std::unordered_map<string,ConstantSP> dict_;
};

class IntAnyDictionary: public AbstractDictionary{
public:
	IntAnyDictionary(DATA_TYPE keyType = DT_INT):AbstractDictionary(keyType,DT_ANY){}
	IntAnyDictionary(const std::unordered_map<int,ConstantSP>& dict, DATA_TYPE keyType = DT_INT):AbstractDictionary(keyType,DT_ANY),dict_(dict){}
	~IntAnyDictionary() override{};
	void clear() override{dict_.clear();}
	INDEX size() const override {return (INDEX)dict_.size();}
	INDEX count() const override {return (INDEX)dict_.size();}
	ConstantSP getInstance() const override { return new IntAnyDictionary(keyType_);}
	ConstantSP getValue() const override {return new IntAnyDictionary(dict_, keyType_);}
	bool set(const ConstantSP& key, const ConstantSP& value) override;
	bool set(int key, const ConstantSP& value) override;
	bool remove(const ConstantSP& value) override;
	ConstantSP getMember(const ConstantSP& key) const override;
	ConstantSP keys() const override;
	ConstantSP values() const override;
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	string getString() const override;
	long long getAllocatedMemory() const override;
	bool containNotMarshallableObject() const override;
private:
	std::unordered_map<int,ConstantSP> dict_;
};

class FloatAnyDictionary: public AbstractDictionary{
public:
    FloatAnyDictionary(DATA_TYPE keyType = DT_FLOAT):AbstractDictionary(keyType,DT_ANY){}
    FloatAnyDictionary(const std::unordered_map<float,ConstantSP>& dict, DATA_TYPE keyType = DT_FLOAT):AbstractDictionary(keyType,DT_ANY), dict_(dict){}
    std::unordered_map<float, ConstantSP>& getInternalDict() { return dict_;}
    ~FloatAnyDictionary() override{};
    void clear() override{dict_.clear();}
    INDEX size() const override {return (INDEX)dict_.size();}
    INDEX count() const override {return (INDEX)dict_.size();}
    ConstantSP getInstance() const override { return new FloatAnyDictionary(keyType_);}
    ConstantSP getValue() const override {return new FloatAnyDictionary(dict_, keyType_);}
    bool set(const ConstantSP& key, const ConstantSP& value) override;
    bool remove(const ConstantSP& key) override;
    ConstantSP getMember(const ConstantSP& key) const override;
    ConstantSP keys() const override;
    ConstantSP values() const override;
    void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
    string getString() const override;
    long long getAllocatedMemory() const override;
    bool containNotMarshallableObject() const override;
private:
    std::unordered_map<float, ConstantSP> dict_;
};

class DoubleAnyDictionary: public AbstractDictionary{
public:
    DoubleAnyDictionary(DATA_TYPE keyType = DT_DOUBLE):AbstractDictionary(keyType,DT_ANY){}
    DoubleAnyDictionary(const std::unordered_map<double,ConstantSP>& dict, DATA_TYPE keyType = DT_DOUBLE):AbstractDictionary(keyType,DT_ANY), dict_(dict){}
    std::unordered_map<double, ConstantSP>& getInternalDict() { return dict_;}
    ~DoubleAnyDictionary() override{};
    void clear() override{dict_.clear();}
    INDEX size() const override {return (INDEX)dict_.size();}
    INDEX count() const override {return (INDEX)dict_.size();}
    ConstantSP getInstance() const override { return new DoubleAnyDictionary(keyType_);}
    ConstantSP getValue() const override {return new DoubleAnyDictionary(dict_, keyType_);}
    bool set(const ConstantSP& key, const ConstantSP& value) override;
    bool remove(const ConstantSP& key) override;
    ConstantSP getMember(const ConstantSP& key) const override;
    ConstantSP keys() const override;
    ConstantSP values() const override;
    void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
    string getString() const override;
    long long getAllocatedMemory() const override;
    bool containNotMarshallableObject() const override;
private:
    std::unordered_map<double, ConstantSP> dict_;
};

class LongAnyDictionary: public AbstractDictionary{
public:
	LongAnyDictionary(DATA_TYPE keyType = DT_LONG):AbstractDictionary(keyType,DT_ANY){}
	LongAnyDictionary(const std::unordered_map<long long,ConstantSP>& dict, DATA_TYPE keyType = DT_LONG):AbstractDictionary(keyType,DT_ANY), dict_(dict){}
	std::unordered_map<long long,ConstantSP>& getInternalDict() { return dict_;}
	~LongAnyDictionary() override{};
	void clear() override{dict_.clear();}
	INDEX size() const override {return (INDEX)dict_.size();}
	INDEX count() const override {return (INDEX)dict_.size();}
	ConstantSP getInstance() const override { return new LongAnyDictionary(keyType_);}
	ConstantSP getValue() const override {return new LongAnyDictionary(dict_, keyType_);}
	bool set(const ConstantSP& key, const ConstantSP& value) override;
	bool remove(const ConstantSP& value) override;
	ConstantSP getMember(const ConstantSP& key) const override;
	ConstantSP keys() const override;
	ConstantSP values() const override;
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	string getString() const override;
	long long getAllocatedMemory() const override;
	bool containNotMarshallableObject() const override;
private:
	std::unordered_map<long long,ConstantSP> dict_;
};

class Int128AnyDictionary: public AbstractDictionary{
public:
	Int128AnyDictionary(DATA_TYPE keyType = DT_INT128):AbstractDictionary(keyType,DT_ANY){}
	Int128AnyDictionary(const std::unordered_map<Guid,ConstantSP>& dict, DATA_TYPE keyType = DT_INT128):AbstractDictionary(keyType,DT_ANY), dict_(dict){}
	std::unordered_map<Guid,ConstantSP>& getInternalDict() { return dict_;}
	~Int128AnyDictionary() override{};
	void clear() override{dict_.clear();}
	INDEX size() const override {return (INDEX)dict_.size();}
	INDEX count() const override {return (INDEX)dict_.size();}
	ConstantSP getInstance() const override { return new Int128AnyDictionary(keyType_);}
	ConstantSP getValue() const override {return new Int128AnyDictionary(dict_, keyType_);}
	bool set(const ConstantSP& key, const ConstantSP& value) override;
	bool remove(const ConstantSP& value) override;
	ConstantSP getMember(const ConstantSP& key) const override;
	ConstantSP keys() const override;
	ConstantSP values() const override;
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	string getString() const override;
	long long getAllocatedMemory() const override;
	bool containNotMarshallableObject() const override;
private:
	std::unordered_map<Guid,ConstantSP> dict_;
};

}

#endif /* DICTIONARYIMP_H_ */
