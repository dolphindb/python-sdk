#pragma once

#include <string>
#include <iostream>
#include "Exports.h"

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/basic_file_sink.h"

#include "pybind11/pybind11.h"

namespace py = pybind11;

constexpr bool LOGGING_ENABLED = false;

namespace dolphindb {


class EXPORT_DECL identifier {
public:
    identifier(const std::string &name): name_(name) {}
    const std::string & get_identifier() const { return name_; }
private:
    std::string name_;
};


class EXPORT_DECL custom_sink : public spdlog::sinks::base_sink<std::mutex>, public identifier {
public:
    custom_sink(const std::string &name)
    : spdlog::sinks::base_sink<std::mutex>(), identifier(name) {}
    ~custom_sink() override {}
    virtual void handle(const py::handle &msg) = 0;
    virtual void pyflush() {}
    std::string print() { return std::string("Sink(") + get_identifier() + ")"; }
protected:
    void sink_it_(const spdlog::details::log_msg &msg) override;
    void flush_() override { this->pyflush(); }
};


class EXPORT_DECL py_custom_sink : public custom_sink {
public:
    py_custom_sink(const std::string &name): custom_sink(name) {}

    void handle(const py::handle &msg) override {
        PYBIND11_OVERRIDE_PURE(void, custom_sink, handle, msg);
    }

    void pyflush() override {
        PYBIND11_OVERRIDE_PURE(void, custom_sink, pyflush, );
    }
};


class EXPORT_DECL Logger {
public:
    enum Level {
        LevelDebug = spdlog::level::debug,
        LevelInfo = spdlog::level::info,
        LevelWarn = spdlog::level::warn,
        LevelError = spdlog::level::err,
        LevelCount,
    };
    Logger(): minLevel_(LevelWarn), enableStdout_(true), filePath_("") {}
    Logger(Logger::Level minLevel): minLevel_(minLevel), enableStdout_(true), filePath_("") {}
    ~Logger() {}
    template<typename... TArgs>
    bool Info(TArgs... args) {
        std::string text;
        return Write(text, LevelInfo, 0, args...);
    }
    template<typename... TArgs>
    bool Debug(TArgs... args) {
        std::string text;
        return Write(text, LevelDebug, 0, args...);
    }
    template<typename... TArgs>
    bool Warn(TArgs... args) {
        std::string text;
        return Write(text, LevelWarn, 0, args...);
    }
    template<typename... TArgs>
    bool Error(TArgs... args) {
        std::string text;
        return Write(text, LevelError, 0, args...);
    }
    void SetMinLevel(Level level);
    Level GetMinLevel() { return minLevel_; }
    void init(bool useFormatter=true);

    void setStdoutFlag(bool flag) { enableStdout_ = flag; }
    void setFilePath(const std::string &path);
    void addSink(py::handle sink);
    std::vector<py::handle> listSinks();
    void removeSink(const std::string & name);
    void clear();
private:
    bool useFormatter_;
    Level minLevel_;
    bool enableStdout_;
    std::string filePath_;
    bool WriteLog(std::string &text, Level level);
    std::shared_ptr<spdlog::logger> stdoutlogger_;
    std::shared_ptr<spdlog::logger> filelogger_;
    std::shared_ptr<spdlog::logger> customlogger_;
    std::vector<py::handle> customsinks_;
    static spdlog::level::level_enum levelMap_[LevelCount];
private:
    template<typename TA, typename... TArgs>
    bool Write(std::string &text, Level level, int deepth, TA first, TArgs... args) {
        if (deepth == 0) text += Create(first);
        else text += " " + Create(first);
        return Write(text, level, deepth + 1, args...);
    }
    template<typename TA>
    bool Write(std::string &text, Level level, int deepth, TA first) {
        if (deepth == 0) text += Create(first);
        else text += " " + Create(first);
        return WriteLog(text, level);
    }
    static std::string Create(const char *value) {
        std::string str(value);
        return str;
    }
    static std::string Create(const void *value) {
        return Create((unsigned long long)value);
    }
    static std::string Create(std::string str) {
        return str;
    }
    static std::string Create(int value) {
        return std::to_string(value);
    }
    static std::string Create(char value) {
        return std::string(&value, 1);
    }
    static std::string Create(unsigned value) {
        return std::to_string(value);
    }
    static std::string Create(long value) {
        return std::to_string(value);
    }
    static std::string Create(unsigned long value) {
        return std::to_string(value);
    }
    static std::string Create(long long value) {
        return std::to_string(value);
    }
    static std::string Create(unsigned long long value) {
        return std::to_string(value);
    }
    static std::string Create(float value) {
        return std::to_string(value);
    }
    static std::string Create(double value) {
        return std::to_string(value);
    }
    static std::string Create(long double value) {
        return std::to_string(value);
    }
};


class EXPORT_DECL LogMessage {
public:
    Logger::Level level_;
    std::string msg_;
    LogMessage(): level_(Logger::LevelDebug), msg_("") {}
    LogMessage(Logger::Level level, const std::string &msg): level_(level), msg_(msg) {}
};


class EXPORT_DECL DLogger {
public:
    enum Level {
        LevelDebug = Logger::LevelDebug,
        LevelInfo = Logger::LevelInfo,
        LevelWarn = Logger::LevelWarn,
        LevelError = Logger::LevelError,
        LevelCount,
    };
    template<typename... TArgs>
    static bool Info(TArgs... args) {
        return defaultLogger_->Info(args...);
    }
    template<typename... TArgs>
    static bool Debug(TArgs... args) {
        return defaultLogger_->Debug(args...);
    }
    template<typename... TArgs>
    static bool Warn(TArgs... args) {
        return defaultLogger_->Warn(args...);
    }
    template<typename... TArgs>
    static bool Error(TArgs... args) {
        return defaultLogger_->Error(args...);
    }
    static void SetLogFilePath(const std::string &filepath);
    static void SetMinLevel(Level level) { defaultLogger_->SetMinLevel((Logger::Level)level); }
    static Level GetMinLevel(){ return (DLogger::Level)defaultLogger_->GetMinLevel(); }

    static void init();

    static std::shared_ptr<Logger> defaultLogger_;
};


#define DLOG(...)  \
    do { \
        if constexpr (LOGGING_ENABLED) { \
            dolphindb::DLogger::Debug(__VA_ARGS__); \
        } \
    } while(0)

#define LOG_DEBUG dolphindb::DLogger::Debug
#define LOG_INFO dolphindb::DLogger::Info
#define LOG_WARN dolphindb::DLogger::Warn
#define LOG_ERR dolphindb::DLogger::Error

}