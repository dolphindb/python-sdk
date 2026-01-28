/*
 * Table.h
 *
 *  Created on: Nov 3, 2012
 *      Author: dzhou
 */

#ifndef TABLE_H_
#define TABLE_H_

#include <atomic>

#include "Table.h"
namespace dolphindb {

class EXPORT_DECL AbstractTable : public Table {
public:
	AbstractTable(const SmartPointer<std::vector<string>>& colNames);
	AbstractTable(const SmartPointer<std::vector<string>>& colNames, SmartPointer<std::unordered_map<string,int>> colMap);
	~AbstractTable() override{}
	ConstantSP getColumn(const string& name) const override;
	ConstantSP getColumn(const string& qualifier, const string& name) const override;
	ConstantSP getColumn(const string& name, const ConstantSP& rowFilter) const override;
	ConstantSP getColumn(const string& qualifier, const string& name, const ConstantSP& rowFilter) const override;
	ConstantSP getColumn(INDEX index, const ConstantSP& rowFilter) const override;
	ConstantSP getColumn(INDEX index) const override = 0;
	ConstantSP get(INDEX col, INDEX row) const override = 0;
	INDEX columns() const override {return static_cast<INDEX>(colNames_->size());}
	const string& getColumnName(int index) const override {return colNames_->at(index);}
	const string& getColumnQualifier(int  /*index*/) const override {return name_;}
	void setColumnName(int index, const string& name) override;
	int getColumnIndex(const string& name) const override;
	bool contain(const string& name) const override;
	bool contain(const string& qualifier, const string& name) const override;
	ConstantSP getColumnLabel() const override;
	ConstantSP values() const override;
	ConstantSP keys() const override { return getColumnLabel();}
	void setName(const string& name) override{name_=name;}
	const string& getName() const override { return name_;}
	virtual bool isTemporary() const {return false;}
    virtual void setTemporary(bool /*temp*/) {}
    bool sizeable() const override {return false;}
	string getString(INDEX index) const override;
	string getString() const override;
	ConstantSP get(INDEX index) const override { return getInternal(index);}
	bool set(INDEX index, const ConstantSP& value) override;
	ConstantSP get(const ConstantSP& index) const override { return getInternal(index);}
	ConstantSP getWindow(int colStart, int colLength, int rowStart, int rowLength) const override {return getWindowInternal(colStart, colLength, rowStart, rowLength);}
	ConstantSP getMember(const ConstantSP& key) const override { return getMemberInternal(key);}
	ConstantSP getInstance() const override {return getInstance(0);}
	ConstantSP getInstance(int size) const override;
	ConstantSP getValue() const override;
	ConstantSP getValue(INDEX capacity) const override;
	bool append(std::vector<ConstantSP>& values, INDEX& insertedRows, string& errMsg) override;
	bool update(std::vector<ConstantSP>& values, const ConstantSP& indexSP, std::vector<string>& colNames, string& errMsg) override;
	bool remove(const ConstantSP& indexSP, string& errMsg) override;
	ConstantSP getSubTable(std::vector<int> indices) const override = 0;
	COMPRESS_METHOD getColumnCompressMethod(INDEX index) override;
	void setColumnCompressMethods(const std::vector<COMPRESS_METHOD> &methods) override;
	bool clear() override =0;
	void updateSize() override = 0;
protected:
	ConstantSP getInternal(INDEX index) const;
	ConstantSP getInternal(const ConstantSP& index) const;
	ConstantSP getWindowInternal(int colStart, int colLength, int rowStart, int rowLength) const;
	ConstantSP getMemberInternal(const ConstantSP& key) const;

private:
	string getTableClassName() const;
	string getTableTypeName() const;

protected:
	SmartPointer<std::vector<string>> colNames_;
	SmartPointer<std::unordered_map<string,int>> colMap_;
	string name_;
	std::vector<COMPRESS_METHOD> colCompresses_;
};


class EXPORT_DECL BasicTable: public AbstractTable{
public:
	BasicTable(const std::vector<ConstantSP>& cols, const std::vector<string>& colNames, const std::vector<int>& keys);
	BasicTable(const std::vector<ConstantSP>& cols, const std::vector<string>& colNames);
	~BasicTable() override;
	virtual bool isBasicTable() const {return true;}
	ConstantSP getColumn(INDEX index) const override;
	ConstantSP get(INDEX col, INDEX row) const override {return cols_[col]->get(row);}
	DATA_TYPE getColumnType(const int index) const override { return cols_[index]->getType();}
	void setColumnName(int index, const string& name) override;
	INDEX size() const override {return size_;}
	bool sizeable() const override {return isReadOnly()==false;}
	bool set(INDEX index, const ConstantSP& value) override;
	ConstantSP get(INDEX index) const override;
	ConstantSP get(const ConstantSP& index) const override;
	ConstantSP getWindow(INDEX colStart, int colLength, INDEX rowStart, int rowLength) const override;
	ConstantSP getMember(const ConstantSP& key) const override;
	ConstantSP getInstance(int size) const override;
	ConstantSP getValue() const override;
	ConstantSP getValue(INDEX capacity) const override;
	bool append(std::vector<ConstantSP>& values, INDEX& insertedRows, string& errMsg) override;
	bool update(std::vector<ConstantSP>& values, const ConstantSP& indexSP, std::vector<string>& colNames, string& errMsg) override;
	bool remove(const ConstantSP& indexSP, string& errMsg) override;
	long long getAllocatedMemory() const override;
	TABLE_TYPE getTableType() const override {return BASICTBL;}
	void drop(std::vector<int>& columns) override;
	bool join(std::vector<ConstantSP>& columns);
	bool clear() override;
	void updateSize() override;
	ConstantSP getSubTable(std::vector<int> indices) const override;

private:
	bool increaseCapacity(long long newCapacity, string& errMsg);
	void initData(const std::vector<ConstantSP>& cols, const std::vector<string>& colNames);
	//bool internalAppend(std::vector<ConstantSP>& values, string& errMsg);
	bool internalRemove(const ConstantSP& indexSP, string& errMsg);
	void internalDrop(std::vector<int>& columns);
	bool internalUpdate(std::vector<ConstantSP>& values, const ConstantSP& indexSP, std::vector<string>& colNames, string& errMsg);

private:
	std::vector<ConstantSP> cols_;
	//bool readOnly_;
	INDEX size_;
	INDEX capacity_;
};

typedef SmartPointer<BasicTable> BasicTableSP;

}

#endif /* TABLE_H_ */
