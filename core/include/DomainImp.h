/*
 * ConstantFactory.h
 *
 *  Created on: Jan 19, 2013
 *      Author: dzhou
 */

#ifndef DOMAIN_H
#define DOMAIN_H

#include "Domain.h"
#include "Dictionary.h"
#include "Constant.h"

namespace dolphindb{

class EXPORT_DECL HashDomain : public Domain{
public:
    HashDomain(DATA_TYPE partitionColType, ConstantSP partitionSchema) : Domain(HASH, partitionColType){
        buckets_ = partitionSchema->getInt();
    }

	vector<int> getPartitionKeys(const ConstantSP& partitionCol) const override;

private:
    int buckets_;
};

class EXPORT_DECL ListDomain : public Domain {
public:
    ListDomain(DATA_TYPE partitionColType, ConstantSP partitionSchema);

    vector<int> getPartitionKeys(const ConstantSP& partitionCol) const override;

private:
	DictionarySP dict_;
};


class EXPORT_DECL ValueDomain : public Domain{
public:
	ValueDomain(DATA_TYPE partitionColType, ConstantSP  /*partitionSchema*/) : Domain(VALUE, partitionColType){}

	vector<int> getPartitionKeys(const ConstantSP& partitionCol) const override;
};

class EXPORT_DECL RangeDomain : public Domain{
public:
    RangeDomain(DATA_TYPE partitionColType, ConstantSP partitionSchema) : Domain(RANGE, partitionColType), range_(partitionSchema){ }

	vector<int> getPartitionKeys(const ConstantSP& partitionCol) const override;
private:
    VectorSP range_;
};

}

#endif /* TABLE_H_ */