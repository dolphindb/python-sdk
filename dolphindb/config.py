import sys
import operator

import numpy as np

import pydantic
from pydantic import BaseModel, Field, GetCoreSchemaHandler, model_validator, ConfigDict
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
    keep_alive_time: int = 30
    reconnect: bool = False
    try_reconnect_nums: Optional[int] = -1
    read_timeout: Optional[int] = -1
    write_timeout: Optional[int] = -1
    use_public_name: bool = False

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


def organize_config(config_type: type, config, kwargs: dict) -> BaseModel:
    if config is None:
        config = config_type(**kwargs)
    elif isinstance(config, dict):
        config = config_type(**config, **kwargs)
    elif isinstance(config, config_type):
        config = config.model_copy(update=kwargs)
    else:
        raise TypeError(
            f"config should be None, dict or {config_type.__name__} instance"
        )
    return config
