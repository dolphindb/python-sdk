#ifndef DdbUtil_H_
#define DdbUtil_H_
#include "DolphinDB.h"
#include "Concurrent.h"
#include "MultithreadedTableWriter.h"
#include "pybind11/numpy.h"
#include "pybind11/embed.h"
#include "pybind11/stl.h"
#include "Wrappers.h"

#ifdef _MSC_VER
	#ifdef _USRDLL	
		#define EXPORT_DECL _declspec(dllexport)
	#else
		#define EXPORT_DECL __declspec(dllimport)
	#endif
#else
    #define EXPORT_DECL
#endif

namespace dolphindb {

#ifdef DLOG
    #undef DLOG
#endif
#define DLOG true?ddb::DLogger::GetMinLevel():ddb::DLogger::Info

#define EXPARAM_DEFAULT INT_MIN


struct HIDEVISIBILITY Preserved {
    // instantiation only once for frequently use
    bool np_above_1_20_;
    bool pd_above_2_0_;
    bool pd_above_1_2_;
    bool pyarrow_import_;
    bool has_arrow_;
    // modules and methods
    py::object numpy_;         // module
    //static py::object isnan_;         // func
    //static py::object sum_;           // func
    py::object datetime64_;    // type, equal to np.datetime64
    py::object nan_;
    py::object pandas_;        // module
    py::object pyarrow_;        // pyarrow
    
    // pandas types (use py::isinstance)
    py::object pdseries_;
    py::object pddataframe_;
    py::object pdNaT_;
    py::object pdNA_;//np.nan=pd.NA
    py::object pdtimestamp_;
    py::object pdindex_;
    py::object pdextensiondtype_;

    // pandas extension dtypes (use equal)
    py::object pdBooleanDtype_;
    py::object pdFloat32Dtype_;
    py::object pdFloat64Dtype_;
    py::object pdInt8Dtype_;
    py::object pdInt16Dtype_;
    py::object pdInt32Dtype_;
    py::object pdInt64Dtype_;
    py::object pdStringDtype_;

    // numpy dtypes (instances of dtypes, use equal)
    py::object nparray_;
    py::object npmatrix_;
    py::object npbool_;
    py::object npint8_;
    py::object npint16_;
    py::object npint32_;
    py::object npint64_;
    py::object npfloat32_;
    py::object npfloat64_;
    //npstr has many format, like U4,U8..., they have same type
    py::object npstrType_;

    py::object npdatetime64M_ins_;
    py::object npdatetime64D_ins_;
    py::object npdatetime64m_ins_;
    py::object npdatetime64s_ins_;
    py::object npdatetime64h_ins_;
    py::object npdatetime64ms_ins_;
    py::object npdatetime64us_ins_;
    py::object npdatetime64ns_ins_;
    py::object npdatetime64_ins_;

    py::object npobject_;
    
    py::object pynone_;
    py::object pybool_;
    py::object pyint_;
    py::object pyfloat_;
    py::object pystr_;
    py::object pybytes_;
    py::object pyset_;
    py::object pytuple_;
    py::object pylist_;
    py::object pydict_;

    py::object m_decimal_;
    py::object decimal_;

    // pyarrow types
    py::object pdarrowdtype_;
    py::object paboolean_;
    py::object paint8_;
    py::object paint16_;
    py::object paint32_;
    py::object paint64_;
    py::object padate32_;
    py::object patime32_ms_;
    py::object patime32_s_;
    py::object patime64_ns_;
    py::object patimestamp_ns_;
    py::object patimestamp_ms_;
    py::object patimestamp_s_;
    py::object pafloat32_;
    py::object pafloat64_;
    py::object padictionary_int32_utf8_;
    py::object pautf8_;
    py::object pafixed_size_binary_16_;
    py::object palarge_binary_;
    py::object padecimal128_;
    py::object palist_;
    py::object pachunkedarray_;

    py::object npdatetime64M_(){ 
        if(np_above_1_20_) return npdatetime64M_ins_;
        else return name2dtype("datetime64[M]");
    }
    py::object npdatetime64D_(){ 
        if(np_above_1_20_) return npdatetime64D_ins_;
        else return name2dtype("datetime64[D]");
    }
    py::object npdatetime64m_(){ 
        if(np_above_1_20_) return npdatetime64m_ins_;
        else return name2dtype("datetime64[m]");
    }
    py::object npdatetime64s_(){
        if(np_above_1_20_) return npdatetime64s_ins_;
        else return name2dtype("datetime64[s]");
    }
    py::object npdatetime64h_(){ 
        if(np_above_1_20_) return npdatetime64h_ins_;
        else return name2dtype("datetime64[h]");
    }
    py::object npdatetime64ms_(){ 
        if(np_above_1_20_) return npdatetime64ms_ins_;
        else return name2dtype("datetime64[ms]");
    }
    py::object npdatetime64us_(){ 
        if(np_above_1_20_) return npdatetime64us_ins_;
        else return name2dtype("datetime64[us]");
    }
    py::object npdatetime64ns_(){ 
        if(np_above_1_20_) return npdatetime64ns_ins_;
        else return name2dtype("datetime64[ns]");
    }
    py::object npdatetime64_(){ 
        if(np_above_1_20_) return npdatetime64_ins_;
        else return name2dtype("datetime64");
    }

    Preserved();

    py::object name2dtype(const char *pname){
        py::object type = py::reinterpret_borrow<py::object>(numpy_.attr("dtype")(pname));
        // py::dtype type(pname);
        //py::object dtname=py::getattr(type, "name");
        //std::string name=py::str(dtname);
        //printf("DType: %s.",name.data());
        return type;
    }
    static py::object getType(py::object obj){
        //std::string text=py::str(obj.get_type());
        //printf("Type: %s.",text.data());
        return py::reinterpret_borrow<py::object>(obj.get_type());
    }
};

enum CHILD_VECTOR_OPTION {
    DISABLE,
    ANY_VECTOR,
    ARRAY_VECTOR,
};

struct TableVectorInfo {
    bool        is_empty;
    std::string colName;
};

class EXPORT_DECL TableChecker: public std::map<std::string, Type>{
public:
    TableChecker() {}
    TableChecker(const py::dict &pydict);
};

class EXPORT_DECL DdbPythonUtil{
public:
    struct ToPythonOption;
    struct ToPythonOption{
        bool table2List;//if object is table, false: convert to pandas, true: convert to list
        ToPythonOption(){
            table2List = false;
        }
        ToPythonOption(bool table2Lista){
            table2List = table2Lista;
        }
    };

    static ConstantSP toDolphinDB(py::object obj, Type typeIndicator = {DT_UNK, EXPARAM_DEFAULT}, TableChecker checker = TableChecker());
    static py::object toPython(ConstantSP obj, bool tableFlag=false, ToPythonOption *poption = NULL);
    static ConstantSP toDolphinDB_Scalar(py::object obj, Type typeIndicator = {DT_UNK, EXPARAM_DEFAULT});
    static ConstantSP toDolphinDB_Vector(py::object obj, Type typeIndicator = {DT_UNK, EXPARAM_DEFAULT}, CHILD_VECTOR_OPTION option = CHILD_VECTOR_OPTION::DISABLE);
    static py::object loadPickleFile(const std::string &filepath);
    static void createPyVector(const ConstantSP &obj,py::object &pyObject,bool tableFlag,ToPythonOption *poption);
    static Preserved *preserved_;
	static void preservedinit() { if(preserved_ == nullptr) preserved_ = new Preserved(); }

protected:
    friend class PytoDdbRowPool;
};

class HIDEVISIBILITY PytoDdbRowPool{
public:
    PytoDdbRowPool(MultithreadedTableWriter &writer);
    ~PytoDdbRowPool();
    void startExit();
    void endExit();
    bool isExit(){
        return exitWhenEmpty_ || writer_.hasError_;
    }
    bool add(vector<py::object> *prow){
        if(isExit())
            return false;
        LockGuard<Mutex> LockGuard(&mutex_);
        rows_.push(prow);
        nonempty_.set();
        return true;
    }
    void getStatus(MultithreadedTableWriter::Status &status);
    void getUnwrittenData(vector<vector<py::object>*> &pyData,vector<vector<ConstantSP>*> &ddbData);

    class ConvertExecutor : public Runnable
    {
    public:
        ConvertExecutor(PytoDdbRowPool &rowPool) : rowPool_(rowPool){};
        virtual void run(){ rowPool_.convertLoop(); }
    private:
        PytoDdbRowPool &rowPool_;
    };

private:
    friend class ConvertExecutor;
    void convertLoop();
    MultithreadedTableWriter &writer_;
    ThreadSP thread_;
    bool exitWhenEmpty_;
    Semaphore idle_;
    Signal nonempty_;
    Mutex mutex_;
    int convertingCount_;
	std::queue<vector<py::object>*> rows_;
    std::queue<vector<py::object>*> failedRows_;
    //when main thread call exit, new pGilRelease_ to release GIL, then run can exit
    SmartPointer<ProtectGil> pGil_;
};

}

#endif //DdbUtil_H_