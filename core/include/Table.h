#pragma once

#include "Constant.h"
#include "SmartPointer.h"

namespace dolphindb {

class EXPORT_DECL Table: public Constant{
public:
    Table() : Constant(1539){}
    ~Table() override{}
    string getScript() const override {return getName();}
    virtual ConstantSP getColumn(const string& name) const = 0;
    virtual ConstantSP getColumn(const string& qualifier, const string& name) const = 0;
    ConstantSP getColumn(INDEX index) const override = 0;
    virtual ConstantSP getColumn(const string& name, const ConstantSP& rowFilter) const = 0;
    virtual ConstantSP getColumn(const string& qualifier, const string& name, const ConstantSP& rowFilter) const = 0;
    virtual ConstantSP getColumn(INDEX index, const ConstantSP& rowFilter) const = 0;
    INDEX columns() const override = 0;
    virtual const string& getColumnName(int index) const = 0;
    virtual const string& getColumnQualifier(int index) const = 0;
    virtual void setColumnName(int index, const string& name)=0;
    virtual int getColumnIndex(const string& name) const = 0;
    virtual DATA_TYPE getColumnType(int index) const = 0;
    virtual bool contain(const string& name) const = 0;
    virtual bool contain(const string& qualifier, const string& name) const = 0;
    virtual void setName(const string& name)=0;
    virtual const string& getName() const = 0;
    ConstantSP get(INDEX index) const override {return getColumn(index);}
    ConstantSP get(const ConstantSP& index) const override = 0;
    virtual ConstantSP getValue(INDEX capacity) const = 0;
    ConstantSP getValue() const override = 0;
    virtual ConstantSP getInstance(INDEX size) const = 0;
    INDEX size() const override = 0;
    bool sizeable() const override = 0;
    string getString(INDEX index) const override = 0;
    string getString() const override = 0;
    ConstantSP getWindow(INDEX colStart, int colLength, INDEX rowStart, int rowLength) const override = 0;
    ConstantSP getMember(const ConstantSP& key) const override = 0;
    ConstantSP values() const override = 0;
    ConstantSP keys() const override = 0;
    virtual TABLE_TYPE getTableType() const = 0;
    virtual void drop(std::vector<int>&  /*columns*/) {throw RuntimeException("Table::drop() not supported");}
    virtual bool update(std::vector<ConstantSP>& values, const ConstantSP& indexSP, std::vector<string>& colNames, string& errMsg) = 0;
    virtual bool append(std::vector<ConstantSP>& values, INDEX& insertedRows, string& errMsg) = 0;
    virtual bool remove(const ConstantSP& indexSP, string& errMsg) = 0;
    DATA_TYPE getType() const override {return DT_DICTIONARY;}
    DATA_TYPE getRawType() const override {return DT_DICTIONARY;}
    DATA_CATEGORY getCategory() const override {return MIXED;}
    bool isLargeConstant() const override {return true;}
    virtual void release() const {}
    virtual void checkout() const {}
    long long getAllocatedMemory() const override = 0;
    virtual ConstantSP getSubTable(std::vector<int> indices) const = 0;
    virtual COMPRESS_METHOD getColumnCompressMethod(INDEX index) = 0;
    virtual void setColumnCompressMethods(const std::vector<COMPRESS_METHOD> &methods) = 0;
    virtual bool clear()=0;
    virtual void updateSize() = 0;
};
typedef SmartPointer<Table> TableSP;
}