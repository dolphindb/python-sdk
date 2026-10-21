import sys
import operator
from datetime import timedelta

import numpy as np

import pydantic
from pydantic import BaseModel, Field, GetCoreSchemaHandler, model_validator, field_serializer, field_validator, ConfigDict
from pydantic_core import core_schema

from ._core import DolphinDBRuntime

from ._hints import (
    Generic,
    List,
    Literal,
    Optional,
    ParserType,
    SqlStd,
    Type,
    TypeVar,
    Union,
)
from .settings import (
    PROTOCOL_ARROW,
    PROTOCOL_DDB,
    PROTOCOL_DEFAULT,
    PROTOCOL_PICKLE,
    default_protocol,
)
from .utils import _helper_check_parser, _version_check, standard_deprecated

ddbcpp = DolphinDBRuntime()._ddbcpp


T = TypeVar("T")


class AnnotatedTemplate(Generic[T]):
    base: Type

    @classmethod
    def __get_pydantic_core_schema__(cls, source, handler: GetCoreSchemaHandler) -> core_schema.CoreSchema:
        return core_schema.no_info_plain_validator_function(cls.validate_ndarray)

    @classmethod
    def validate_ndarray(cls, value):
        if not isinstance(value, cls.base):
            raise ValueError("Input should be a numpy ndarray.")
        return value


class NDArrayAnnotated(AnnotatedTemplate[np.ndarray], np.ndarray):
    base = np.ndarray


custom_config_dict = ConfigDict()
if _version_check(pydantic.__version__, operator.ge, "2.11"):
    custom_config_dict["validate_by_name"] = True
    custom_config_dict["validate_by_alias"] = True
else:
    custom_config_dict["populate_by_name"] = True
custom_config_dict["extra"] = "forbid"


class CustomBaseModel(BaseModel):

    model_config = custom_config_dict


class ConnectionSetting(CustomBaseModel):
    enable_ssl: bool = False
    enable_async: bool = False
    compress: bool = False
    parser: Union[ParserType, Literal["dolphindb", "python", "kdb"], int] = (
        ParserType.Default
    )
    protocol: Union[
        int,
        Literal["default", "pickle", "ddb", "arrow"],
    ] = PROTOCOL_DEFAULT
    show_output: bool = True
    sql_std: Union[SqlStd, Literal["ddb", "mysql", "oracle"], int] = (
        SqlStd.DolphinDB
    )

    @model_validator(mode='after')
    def convert_protocol(self):
        if isinstance(self.protocol, str):
            protocol_map = {
                "default": PROTOCOL_DEFAULT,
                "pickle": PROTOCOL_PICKLE,
                "ddb": PROTOCOL_DDB,
                "arrow": PROTOCOL_ARROW,
            }
            self.protocol = protocol_map.get(self.protocol.lower(), PROTOCOL_DEFAULT)
        if self.protocol not in [PROTOCOL_DEFAULT, PROTOCOL_DDB, PROTOCOL_PICKLE, PROTOCOL_ARROW]:
            raise ValueError(f"Protocol {self.protocol} is not supported. ")
        if self.protocol == PROTOCOL_ARROW and self.compress:
            raise ValueError("The Arrow protocol does not support compression.")
        if self.protocol == PROTOCOL_ARROW:
            __import__("pyarrow")
        if self.protocol == PROTOCOL_PICKLE:
            standard_deprecated(
                "The use of PROTOCOL_PICKLE has been deprecated and removed in Python 3.13. "
                "Please migrate to an alternative protocol or serialization method."
            )
            if sys.version_info.minor >= 13:
                self.protocol = default_protocol
        if self.protocol == PROTOCOL_DEFAULT:
            self.protocol = default_protocol
        return self

    @model_validator(mode='after')
    def convert_parser(self):
        self.parser = _helper_check_parser(self.parser)
        return self

    @model_validator(mode='after')
    def convert_sql_std(self):
        if isinstance(self.sql_std, str):
            sql_std_map = {
                "ddb": SqlStd.DolphinDB,
                "mysql": SqlStd.MySQL,
                "oracle": SqlStd.Oracle,
            }
            self.sql_std = sql_std_map.get(self.sql_std.lower(), SqlStd.DolphinDB)
        elif isinstance(self.sql_std, int):
            self.sql_std = SqlStd(self.sql_std)
        return self


class ConnectionConfig(CustomBaseModel):
    host: str
    port: int
    userid: str = Field("", alias="userId")
    password: str = ""

    startup: str = ""
    enable_high_availability: bool = False
    high_availability_sites: Optional[List[str]] = Field(default_factory=list)
    keep_alive_time: int = -1
    reconnect: bool = False
    try_reconnect_nums: Optional[int] = -1
    net_timeout: Optional[timedelta] = None
    timeout: Optional[timedelta] = None
    read_timeout: Optional[float] = -1
    write_timeout: Optional[float] = -1
    use_public_name: bool = False

    @model_validator(mode='before')
    @classmethod
    def omit_unspecified_timeouts(cls, values):
        if isinstance(values, dict):
            values = {
                key: value for key, value in values.items()
                if key not in ("timeout", "net_timeout") or value is not None
            }
        return values

    @model_validator(mode='after')
    def check_high_availability_sites(self):
        if self.high_availability_sites is None:
            self.high_availability_sites = []
        return self

    @model_validator(mode='after')
    def check_try_reconnect_nums(self):
        if self.try_reconnect_nums is None:
            self.try_reconnect_nums = -1
        return self

    @field_validator("net_timeout", "timeout", mode="before")
    @classmethod
    def check_duration_timeout_fields(cls, value, info):
        if value is None:
            return None
        if not isinstance(value, timedelta):
            raise TypeError(f"The parameter '{info.field_name}' must be datetime.timedelta.")
        if value.total_seconds() < 0:
            raise ValueError(f"The parameter '{info.field_name}' must be non-negative.")
        if value == timedelta(0):
            return None
        return value

    @field_serializer("net_timeout", "timeout")
    def serialize_duration_timeout_fields(self, value: Optional[timedelta]):
        if value is None:
            return -1
        return round(value.total_seconds() * 1000)

    @model_validator(mode='after')
    def check_read_timeout(self):
        if self.read_timeout is None:
            self.read_timeout = -1
        return self

    @model_validator(mode='after')
    def check_write_timeout(self):
        if self.write_timeout is None:
            self.write_timeout = -1
        return self

    @model_validator(mode='after')
    def normalize_legacy_timeout_fields(self):
        explicit_fields = self.model_fields_set.copy()
        timeout_is_explicit = "timeout" in self.model_fields_set
        if timeout_is_explicit and self.timeout is not None:
            self.read_timeout = self.timeout.total_seconds()
        elif timeout_is_explicit:
            self.read_timeout = -1
        elif self.read_timeout >= 0:
            self.timeout = None if self.read_timeout == 0 else timedelta(seconds=self.read_timeout)

        net_timeout_candidates = []
        net_timeout_is_explicit = "net_timeout" in self.model_fields_set
        if net_timeout_is_explicit and self.net_timeout is not None:
            net_timeout_candidates.append(round(self.net_timeout.total_seconds() * 1000))
        elif net_timeout_is_explicit:
            self.write_timeout = -1
        if self.write_timeout > 0 and (not net_timeout_is_explicit or self.net_timeout is not None):
            net_timeout_candidates.append(self.write_timeout * 1000)
        if net_timeout_candidates:
            net_timeout = min(net_timeout_candidates)
            self.net_timeout = timedelta(milliseconds=net_timeout)
            self.write_timeout = self.net_timeout.total_seconds()
        elif self.write_timeout == 0:
            self.net_timeout = None
        # Derived aliases must not become explicit inputs when a config is reused.
        self.__pydantic_fields_set__ = explicit_fields
        return self


def organize_config(config_type: type, config, kwargs: dict) -> BaseModel:
    if config is None:
        config = config_type(**kwargs)
    elif isinstance(config, dict):
        config = config_type(**config, **kwargs)
    elif isinstance(config, config_type):
        if kwargs:
            # Include current values: mutable defaults can change without entering model_fields_set.
            fields = type(config).model_fields
            overridden = {
                name for name, field in fields.items()
                if name in kwargs or (field.alias is not None and field.alias in kwargs)
            }
            explicit_fields = config.model_fields_set | overridden
            # Read Python values directly: model_dump serializes durations to milliseconds.
            values = {name: getattr(config, name) for name in fields if name not in overridden}
            if isinstance(config, ConnectionConfig):
                for name in ("timeout", "net_timeout", "read_timeout", "write_timeout"):
                    if name not in explicit_fields:
                        values.pop(name, None)
            values.update(kwargs)
            config = type(config)(**values)
            config.__pydantic_fields_set__ = explicit_fields
        else:
            config = config.model_copy()
    else:
        raise TypeError(
            f"config should be None, dict or {config_type.__name__} instance"
        )
    return config
