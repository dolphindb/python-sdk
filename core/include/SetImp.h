/*
 * SetImp.h
 *
 *  Created on: Jan 17, 2016
 *      Author: dzhou
 */

#ifndef SETIMP_H_
#define SETIMP_H_

#include <unordered_set>

#include "Set.h"
#include "Util.h"

namespace dolphindb {

template<class T>
class EXPORT_DECL AbstractSet : public Set {
public:
	AbstractSet(DATA_TYPE type, INDEX capacity = 0) : type_(type), category_(Util::getCategory(type_)){
		if(capacity > 0) data_.reserve(capacity);
	}
	AbstractSet(DATA_TYPE type, const std::unordered_set<T>& data) : type_(type), category_(Util::getCategory(type_)), data_(data){}
	bool sizeable() const override {return true;}
	INDEX size() const override {return static_cast<INDEX>(data_.size());}
	ConstantSP keys() const override {
		return getSubVector(0, static_cast<INDEX>(data_.size()));
	}
	DATA_TYPE getType() const override {return type_;}
	DATA_TYPE getRawType() const override {return type_ == DT_SYMBOL ? DT_INT : Util::convertToIntegralDataType(type_);}
	DATA_CATEGORY getCategory() const override {return category_;}
	long long getAllocatedMemory() const override { return sizeof(T) * data_.bucket_count();}
	std::string getString() const override {
		int len=(std::min)(Util::DISPLAY_ROWS,size());
		ConstantSP key = getSubVector(0, len);
		std::string str("set(");

		if(len>0){
			if(len == 1 && key->isNull(0))
				str.append(key->get(0)->getScript());
			else{
				if(isNull(0)){
					//do nothing
				}
				else
					str.append(key->get(0)->getScript());
			}
		}
		for(int i=1;i<len;++i){
			str.append(",");
			if(isNull(i)){
				//do nothing
			}
			else
				str.append(key->get(i)->getScript());
		}
		if(size()>len)
			str.append("...");
		str.append(")");
		return str;
	}
	void clear() override{ data_.clear();}
	ConstantSP get(const ConstantSP&  /*index*/) const override {throw RuntimeException("set doesn't support random access.");}
	const std::string& getStringRef() const override {throw RuntimeException("set doesn't support random access.");}
	ConstantSP get(INDEX  /*index*/) const override {throw RuntimeException("set doesn't support random access.");}
	ConstantSP get(INDEX  /*column*/, INDEX  /*row*/) const override {throw RuntimeException("set doesn't support random access.");}
	ConstantSP getColumn(INDEX  /*index*/) const override {throw RuntimeException("set doesn't support random access.");}
	ConstantSP getRow(INDEX  /*index*/) const override {throw RuntimeException("set doesn't support random access.");}
	ConstantSP getItem(INDEX  /*index*/) const override {throw RuntimeException("set doesn't support random access.");}

protected:
	DATA_TYPE type_;
	DATA_CATEGORY category_;
	std::unordered_set<T> data_;
};

class EXPORT_DECL CharSet : public AbstractSet<char> {
public:
	CharSet(INDEX capacity = 0) : AbstractSet<char>(DT_CHAR, capacity){}
	CharSet(const std::unordered_set<char>& data) : AbstractSet<char>(DT_CHAR, data){}
	ConstantSP getInstance() const override { return new CharSet();}
	ConstantSP getValue() const override { return new CharSet(data_);}
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	bool remove(const ConstantSP& value) override { return manipulate(value, true);}
	bool append(const ConstantSP& value) override { return manipulate(value, false);}
	bool inverse(const ConstantSP& value) override;
	bool isSuperset(const ConstantSP& target) const override;
	ConstantSP interaction(const ConstantSP& target) const override;
	ConstantSP getSubVector(INDEX start, INDEX length) const override;
	bool manipulate(const ConstantSP& value, bool deletion);
};

class EXPORT_DECL ShortSet : public AbstractSet<short> {
public:
	ShortSet(INDEX capacity = 0) : AbstractSet<short>(DT_SHORT, capacity){}
	ShortSet(const std::unordered_set<short>& data) : AbstractSet<short>(DT_SHORT, data){}
	ConstantSP getInstance() const override { return new ShortSet();}
	ConstantSP getValue() const override { return new ShortSet(data_);}
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	bool remove(const ConstantSP& value) override { return manipulate(value, true);}
	bool append(const ConstantSP& value) override { return manipulate(value, false);}
	bool inverse(const ConstantSP& value) override;
	bool isSuperset(const ConstantSP& target) const override;
	ConstantSP interaction(const ConstantSP& target) const override;
	ConstantSP getSubVector(INDEX start, INDEX length) const override;
	bool manipulate(const ConstantSP& value, bool deletion);
};

class EXPORT_DECL IntSet : public AbstractSet<int> {
public:
	IntSet(DATA_TYPE type = DT_INT, INDEX capacity = 0) : AbstractSet<int>(type, capacity){}
	IntSet(DATA_TYPE type, const std::unordered_set<int>& data) : AbstractSet<int>(type, data){}
	ConstantSP getInstance() const override { return new IntSet(type_);}
	ConstantSP getValue() const override { return new IntSet(type_, data_);}
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	bool remove(const ConstantSP& value) override { return manipulate(value, true);}
	bool append(const ConstantSP& value) override { return manipulate(value, false);}
	bool inverse(const ConstantSP& value) override;
	bool isSuperset(const ConstantSP& target) const override;
	ConstantSP interaction(const ConstantSP& target) const override;
	ConstantSP getSubVector(INDEX start, INDEX length) const override;
	bool manipulate(const ConstantSP& value, bool deletion);
};

class EXPORT_DECL LongSet : public AbstractSet<long long> {
public:
	LongSet(DATA_TYPE type = DT_LONG, INDEX capacity = 0) : AbstractSet<long long>(type, capacity){}
	LongSet(DATA_TYPE type, const std::unordered_set<long long>& data) : AbstractSet<long long>(type, data){}
	ConstantSP getInstance() const override { return new LongSet(type_);}
	ConstantSP getValue() const override { return new LongSet(type_, data_);}
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	bool remove(const ConstantSP& value) override { return manipulate(value, true);}
	bool append(const ConstantSP& value) override { return manipulate(value, false);}
	bool inverse(const ConstantSP& value) override;
	bool isSuperset(const ConstantSP& target) const override;
	ConstantSP interaction(const ConstantSP& target) const override;
	ConstantSP getSubVector(INDEX start, INDEX length) const override;
	bool manipulate(const ConstantSP& value, bool deletion);
};

class EXPORT_DECL FloatSet : public AbstractSet<float> {
public:
	FloatSet(INDEX capacity = 0) : AbstractSet<float>(DT_FLOAT, capacity){}
	FloatSet(const std::unordered_set<float>& data) : AbstractSet<float>(DT_FLOAT, data){}
	ConstantSP getInstance() const override { return new FloatSet();}
	ConstantSP getValue() const override { return new FloatSet(data_);}
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	bool remove(const ConstantSP& value) override { return manipulate(value, true);}
	bool append(const ConstantSP& value) override { return manipulate(value, false);}
	bool inverse(const ConstantSP& value) override;
	bool isSuperset(const ConstantSP& target) const override;
	ConstantSP interaction(const ConstantSP& target) const override;
	ConstantSP getSubVector(INDEX start, INDEX length) const override;
	bool manipulate(const ConstantSP& value, bool deletion);
};

class EXPORT_DECL DoubleSet : public AbstractSet<double> {
public:
	DoubleSet(INDEX capacity = 0) : AbstractSet<double>(DT_DOUBLE, capacity){}
	DoubleSet(const std::unordered_set<double>& data) : AbstractSet<double>(DT_DOUBLE, data){}
	ConstantSP getInstance() const override { return new DoubleSet();}
	ConstantSP getValue() const override { return new DoubleSet(data_);}
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	bool remove(const ConstantSP& value) override { return manipulate(value, true);}
	bool append(const ConstantSP& value) override { return manipulate(value, false);}
	bool inverse(const ConstantSP& value) override;
	bool isSuperset(const ConstantSP& target) const override;
	ConstantSP interaction(const ConstantSP& target) const override;
	ConstantSP getSubVector(INDEX start, INDEX length) const override;
	bool manipulate(const ConstantSP& value, bool deletion);
};

class EXPORT_DECL StringSet : public AbstractSet<std::string> {
public:
	StringSet(INDEX capacity = 0, bool isBlob = false, bool isSymbol = false)
		: AbstractSet<std::string>(isBlob ? DT_BLOB : isSymbol ? DT_SYMBOL : DT_STRING, capacity), isBlob_(isBlob), isSymbol_(isSymbol){}
	StringSet(const std::unordered_set<std::string>& data, bool isBlob = false, bool isSymbol = false)
		: AbstractSet<std::string>(isBlob ? DT_BLOB : isSymbol ? DT_SYMBOL : DT_STRING, data), isBlob_(isBlob), isSymbol_(isSymbol){}
	ConstantSP getInstance() const override { return new StringSet(0, isBlob_, isSymbol_);}
	ConstantSP getValue() const override { return new StringSet(data_, isBlob_, isSymbol_);}
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	bool remove(const ConstantSP& value) override { return manipulate(value, true);}
	bool append(const ConstantSP& value) override { return manipulate(value, false);}
	bool inverse(const ConstantSP& value) override;
	bool isSuperset(const ConstantSP& target) const override;
	ConstantSP interaction(const ConstantSP& target) const override;
	ConstantSP getSubVector(INDEX start, INDEX length) const override;
	bool manipulate(const ConstantSP& value, bool deletion);
private:
	bool isBlob_;
	bool isSymbol_;
};

class EXPORT_DECL Int128Set : public AbstractSet<Guid> {
public:
	Int128Set(DATA_TYPE type = DT_INT128, INDEX capacity = 0) : AbstractSet<Guid>(type, capacity){}
	Int128Set(DATA_TYPE type, const std::unordered_set<Guid>& data) : AbstractSet<Guid>(type, data){}
	ConstantSP getInstance() const override { return new Int128Set(type_);}
	ConstantSP getValue() const override { return new Int128Set(type_, data_);}
	void contain(const ConstantSP& target, const ConstantSP& resultSP) const override;
	bool remove(const ConstantSP& value) override { return manipulate(value, true);}
	bool append(const ConstantSP& value) override { return manipulate(value, false);}
	bool inverse(const ConstantSP& value) override;
	bool isSuperset(const ConstantSP& target) const override;
	ConstantSP interaction(const ConstantSP& target) const override;
	ConstantSP getSubVector(INDEX start, INDEX length) const override;
	bool manipulate(const ConstantSP& value, bool deletion);
};

}
#endif /* SETIMP_H_ */
