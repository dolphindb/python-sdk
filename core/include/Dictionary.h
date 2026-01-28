#pragma once

#include "Constant.h"
#include "SmartPointer.h"
namespace dolphindb {

class EXPORT_DECL Dictionary:public Constant{
public:
    Dictionary() : Constant(1283){}
    ~Dictionary() override {}
    INDEX size() const override = 0;
    virtual INDEX count() const = 0;
    virtual void clear()=0;
    ConstantSP getMember(const ConstantSP& key) const override =0;
    virtual ConstantSP getMember(const string&  /*key*/) const {throw RuntimeException("String key not supported");}
    virtual ConstantSP get(INDEX  /*column*/, INDEX  /*row*/){throw RuntimeException("Dictionary does not support cell function");}
    virtual DATA_TYPE getKeyType() const = 0;
    virtual DATA_CATEGORY getKeyCategory() const = 0;
    DATA_TYPE getType() const override = 0;
    DATA_CATEGORY getCategory() const override = 0;
    ConstantSP keys() const override = 0;
    ConstantSP values() const override = 0;
    string getString() const override = 0;
    string getScript() const override {return "dict()";}
    string getString(int  /*index*/) const override {throw RuntimeException("Dictionary::getString(int index) not supported");}
    virtual bool remove(const ConstantSP& key) = 0;
    bool set(const ConstantSP& key, const ConstantSP& value) override =0;
    virtual bool set(const string&  /*key*/, const ConstantSP&  /*value*/){throw RuntimeException("String key not supported");}
    ConstantSP get(const ConstantSP& key) const override {return getMember(key);}
    virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const = 0;
    bool isLargeConstant() const override {return true;}

};

typedef SmartPointer<Dictionary> DictionarySP;
}