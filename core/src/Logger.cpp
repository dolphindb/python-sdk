#include "Logger.h"
#include "Util.h"
#include "DolphinDB.h"

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/basic_file_sink.h"

#include <chrono>
#include <ctime>

#include "pybind11/pybind11.h"

namespace py = pybind11;

namespace dolphindb {


class CustomFormatter : public spdlog::formatter {
public:
    void format(const spdlog::details::log_msg& msg, spdlog::memory_buf_t& dest) override {
        std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
        std::string data = Util::toMicroTimestampStr(now, true) + ": [" +
            std::to_string(Util::getCurThreadId()) + "] " + to_capital(msg.level) + ": " + msg.payload.data() + "\n";
        dest.append(data.data(), data.data() + data.size());
    }
    std::unique_ptr<spdlog::formatter> clone() const override {
        return std::unique_ptr<spdlog::formatter>(new CustomFormatter(*this));
    }
private:
    static std::string to_capital(spdlog::level::level_enum level) {
        switch (level) {
            case spdlog::level::info: return "Info";
            case spdlog::level::warn: return "Warn";
            case spdlog::level::err: return "Error";
            case spdlog::level::debug: return "Debug";
            default: return "Unknown";
        }
    }
};


class MsgFormatter : public spdlog::formatter {
public:
    void format(const spdlog::details::log_msg& msg, spdlog::memory_buf_t& dest) override {
        std::string data = std::string(msg.payload.data()) + "\n";
        dest.append(data.data(), data.data() + data.size());
    }
    std::unique_ptr<spdlog::formatter> clone() const override {
        return std::unique_ptr<spdlog::formatter>(new MsgFormatter(*this));
    }
};


void _set_formatter(std::shared_ptr<spdlog::logger> logger, bool use_formatter = true) {
    if (use_formatter)
        logger->set_formatter(std::unique_ptr<CustomFormatter>(new CustomFormatter()));
    else
        logger->set_formatter(std::unique_ptr<MsgFormatter>(new MsgFormatter()));
}


void custom_sink::sink_it_(const spdlog::details::log_msg &msg) {
    Logger::Level level = (Logger::Level)msg.level;
    py::gil_scoped_acquire gil;
    PyObject* tmp = PyUnicode_DecodeUTF8Stateful(msg.payload.data(), msg.payload.size(), "replace", NULL);
    if (PyErr_Occurred() || tmp == nullptr) {
        throw py::error_already_set();
    }
    this->handle(py::cast(LogMessage(level, PyUnicode_AsUTF8(tmp))));
    Py_XDECREF(tmp);
}


spdlog::level::level_enum Logger::levelMap_[] = {
    spdlog::level::debug,
    spdlog::level::info,
    spdlog::level::warn,
    spdlog::level::err,
};

void Logger::init(bool useFormatter) {
    useFormatter_ = useFormatter;

    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    stdoutlogger_ = std::make_shared<spdlog::logger>("dolphindb_api_stdout", console_sink);
    _set_formatter(stdoutlogger_, useFormatter_);

    customlogger_ = std::make_shared<spdlog::logger>("dolphindb_api_custom");
    _set_formatter(customlogger_, useFormatter_);

    filelogger_ = nullptr;

    SetMinLevel(minLevel_);
}

void Logger::clear() {
    if (customsinks_.size() > 0) {
        py::gil_scoped_acquire gil;
        for (auto &it : customsinks_) {
            it.dec_ref();
        }
    }
}

void Logger::SetMinLevel(Level level) {
    spdlog::level::level_enum spdLevel = (spdlog::level::level_enum)level;
    if (stdoutlogger_ != nullptr) {
        stdoutlogger_->set_level(spdLevel);
    }
    if (filelogger_ != nullptr) {
        filelogger_->set_level(spdLevel);
    }
    if (customlogger_ != nullptr) {
        customlogger_->set_level(spdLevel);
    }
    minLevel_ = level;
}

void Logger::setFilePath(const std::string &path) {
    if (path.empty()) {
        filelogger_ = nullptr;
        return;
    }
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(path);
    filelogger_ = std::make_shared<spdlog::logger>("dolphindb_api_file", file_sink);
    _set_formatter(filelogger_, useFormatter_);
}

void Logger::addSink(py::handle pysink) {
    pysink.inc_ref();
    customsinks_.push_back(pysink);
    auto sink = py::cast<std::shared_ptr<custom_sink>>(pysink);
    auto sinks = customlogger_->sinks();
    sinks.push_back(sink);
    customlogger_ = std::make_shared<spdlog::logger>("dolphindb_api_custom", sinks.begin(), sinks.end());
    _set_formatter(customlogger_, useFormatter_);
    SetMinLevel(minLevel_);
}

std::vector<py::handle> Logger::listSinks() {
    return customsinks_;
}

void Logger::removeSink(const std::string &name) {
    auto pysinks = customsinks_;
    auto pyit = std::remove_if(pysinks.begin(), pysinks.end(), [&name](const py::handle &sink) {
        if (sink.attr("name").cast<std::string>() == name) return true;
        return false;
    });
    pyit->dec_ref();
    pysinks.erase(pyit, pysinks.end());
    customsinks_ = pysinks;
    
    auto sinks = customlogger_->sinks();
    auto it = std::remove_if(sinks.begin(), sinks.end(), [&name](const spdlog::sink_ptr &sink) {
        auto cus_sink = std::dynamic_pointer_cast<custom_sink>(sink);
        if (cus_sink && cus_sink->get_identifier() == name) {
            return true;
        }
        return false;
    });
    sinks.erase(it, sinks.end());

    customlogger_ = std::make_shared<spdlog::logger>("dolphindb_api_custom", sinks.begin(), sinks.end());
    _set_formatter(customlogger_, useFormatter_);
    SetMinLevel(minLevel_);
}

bool Logger::WriteLog(std::string &text, Level level) {
    auto spdLevel = (spdlog::level::level_enum)level;
    if (enableStdout_)
        stdoutlogger_->log(spdLevel, text);
    if (filelogger_ != nullptr)
        filelogger_->log(spdLevel, text);
    customlogger_->log(spdLevel, text);
    return true;
}


void DLogger::init() {
    defaultLogger_->init(true);
}


} // namespace dolphindb