/*
 * Exceptions.h
 *
 *  Created on: Jul 22, 2012
 *      Author: dzhou
 */

#ifndef EXCEPTIONS_H_
#define EXCEPTIONS_H_

#include <exception>
#include <string>
#include "Types.h"
#include "Exports.h"
using std::exception;
using std::string;

namespace dolphindb {

class EXPORT_DECL IncompatibleTypeException: public exception{
public:
	IncompatibleTypeException(DATA_TYPE expected, DATA_TYPE actual);
	~IncompatibleTypeException() throw() override{}
	const char* what() const throw() override { return errMsg_.c_str();}
	DATA_TYPE expectedType(){return expected_;}
	DATA_TYPE actualType(){return actual_;}
private:
	DATA_TYPE expected_;
	DATA_TYPE actual_;
	string errMsg_;
};

class EXPORT_DECL SyntaxException: public exception{
public:
	SyntaxException(const string& errMsg): errMsg_(errMsg){}
	const char* what() const throw() override{
		return errMsg_.c_str();
	}
	~SyntaxException() throw() override{}

private:
	const string errMsg_;
};

class EXPORT_DECL IllegalArgumentException : public exception{
public:
	IllegalArgumentException(const string& functionName, const string& errMsg): functionName_(functionName), errMsg_(errMsg){}
	const char* what() const throw() override{
		return errMsg_.c_str();
	}
	~IllegalArgumentException() throw() override{}
	const string& getFunctionName() const { return functionName_;}

private:
	const string functionName_;
	const string errMsg_;
};

class EXPORT_DECL RuntimeException: public exception{
public:
	RuntimeException(const string& errMsg):errMsg_(errMsg){}
	const char* what() const throw() override{
		return errMsg_.c_str();
	}
	~RuntimeException() throw() override{}

private:
	const string errMsg_;
};

class EXPORT_DECL OperatorRuntimeException: public exception{
public:
	OperatorRuntimeException(const string& optr,const string& errMsg): operator_(optr),errMsg_(errMsg){}
	const char* what() const throw() override{
		return errMsg_.c_str();
	}
	~OperatorRuntimeException() throw() override{}
	const string& getOperatorName() const { return operator_;}

private:
	const string operator_;
	const string errMsg_;
};

class EXPORT_DECL TableRuntimeException: public exception{
public:
	TableRuntimeException(const string& errMsg): errMsg_(errMsg){}
	const char* what() const throw() override{
		return errMsg_.c_str();
	}
	~TableRuntimeException() throw() override{}

private:
	const string errMsg_;
};

class EXPORT_DECL MemoryException: public exception{
public:
	MemoryException():errMsg_("Out of memory"){}
	const char* what() const throw() override{
		return errMsg_.c_str();
	}
	~MemoryException() throw() override{}

private:
	const string errMsg_;
};

class EXPORT_DECL IOException: public exception{
public:
	IOException(const string& errMsg): errMsg_(errMsg), errCode_(OTHERERR){}
	IOException(const string& errMsg, IO_ERR errCode): errMsg_(errMsg + ". " + getCodeDescription(errCode)), errCode_(errCode){}
	IOException(IO_ERR errCode): errMsg_(getCodeDescription(errCode)), errCode_(errCode){}
	const char* what() const throw() override{
		return errMsg_.c_str();
	}
	~IOException() throw() override{}
	IO_ERR getErrorCode() const {return errCode_;}
private:
	string getCodeDescription(IO_ERR errCode) const;

private:
	const string errMsg_;
	const IO_ERR errCode_;
};

class EXPORT_DECL DataCorruptionException: public exception {
public:
	DataCorruptionException(const string& errMsg) : errMsg_("<DataCorruption>" + errMsg){}
	const char* what() const throw() override{
		return errMsg_.c_str();
	}
	~DataCorruptionException() throw() override{}

private:
	const string errMsg_;
};

class EXPORT_DECL NotLeaderException: public exception {
public:
	//Electing a leader. Wait for a while to retry.
	NotLeaderException() : errMsg_("<NotLeader>"){}
	//Use the new leader specified in the input argument. format: <host>:<port>:<alias>, e.g. 192.168.1.10:8801:nodeA
	NotLeaderException(const string& newLeader) : errMsg_("<NotLeader>" + newLeader), newLeader_(newLeader){}
	const string& getNewLeader() const {return newLeader_;}
	const char* what() const throw() override{
		return errMsg_.c_str();
	}
	~NotLeaderException() throw() override{}

private:
	const string errMsg_;
	const string newLeader_;
};

class EXPORT_DECL MathException: public exception {
public:
	MathException(const string& errMsg) : errMsg_(errMsg){}
	const char* what() const throw() override{
		return errMsg_.c_str();
	}
	~MathException() throw() override{}

private:
	const string errMsg_;
};

class EXPORT_DECL TestingException: public exception{
public:
	TestingException(const string& caseName,const string& subCaseName): name_(caseName),subName_(subCaseName){
		if(subName_.empty())
			errMsg_="Testing case "+name_+" failed";
		else
			errMsg_="Testing case "+name_+"_"+subName_+" failed";
	}
	const char* what() const throw() override{
		return errMsg_.c_str();
	}
	const string& getCaseName() const {return name_;}
	const string& getSubCaseName() const {return subName_;}
	~TestingException() throw() override{}

private:
	const string name_;
	const string subName_;
	string errMsg_;

};

class EXPORT_DECL UserException: public exception{
public:
	UserException(const string exceptionType, const string& msg) : exceptionType_(exceptionType), msg_(msg){}
	const char* what() const throw() override{
		return msg_.c_str();
	}
	const string& getExceptionType() const { return exceptionType_;}
	const string& getMessage() const { return msg_;}
	~UserException() throw() override{}
private:
	string exceptionType_;
	string msg_;
};

}
#endif /* EXCEPTIONS_H_ */
