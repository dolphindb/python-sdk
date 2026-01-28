from abc import ABC, abstractmethod
from typing import final, overload

import numpy as np
from pydantic import Field, model_validator
from pydantic.dataclasses import dataclass

from ._core import DolphinDBRuntime
from ._hints import Any, Callable, Dict, List, Optional, Union
from .config import AnnotatedTemplate, NDArrayAnnotated, organize_config, CustomBaseModel
from .settings import DDB_EPSILON
from .connection import DBConnection
from .utils import dispatcher

ddbcpp = DolphinDBRuntime()._ddbcpp


DEFAULT_ACTION_NAME = ""


class StreamDeserializer(object):
    """Deserializer stream blob in multistreamingtable reply.

    Args:
        sym2table : a dict object indicating the corresponding relationship between the unique identifiers of the tables and the table objects.
        conn : a DBConnection object. Defaults to None.
    """

    def __init__(
        self,
        sym2table: Dict[str, Union[List[str], str]],
        conn: DBConnection = None,
        *,
        session: DBConnection = None,
    ):
        """Constructor of streamDeserializer."""
        if session is not None:
            conn = session
        if not isinstance(conn, DBConnection) and conn is not None:
            raise TypeError("conn should be a DBConnection object.")
        self.cpp = ddbcpp.streamDeserializer(
            sym2table, conn.cpp if conn else None
        )


# class SubscribeState:
#     Connected = 0
#     Disconnected = 1
#     Resubscribing = 2


@dataclass
class SubscribeInfo:
    host: str
    port: int
    table_name: str
    action_name: Optional[str] = DEFAULT_ACTION_NAME

    def __str__(self):
        return f"{self.host}/{self.port}/{self.table_name}/{self.action_name}"

    @classmethod
    def parse(cls, topic_str: str) -> "SubscribeInfo":
        parts = topic_str.split('/')
        assert len(parts) == 4
        host, port_str, table_name, action_name = parts
        port = int(port_str)
        return cls(host, port, table_name, action_name)

    @model_validator(mode="after")
    def check_action_name(self) -> "SubscribeInfo":
        if self.action_name is None:
            self.action_name = ""
        return self


class StreamingClientConfig(CustomBaseModel):
    # callback: Optional[Callable[[SubscribeState, SubscribeInfo], None]] = None
    pass


class StreamDeserializerAnnotated(AnnotatedTemplate[StreamDeserializer], StreamDeserializer):
    base = StreamDeserializer


class SubscriptionConfig(CustomBaseModel):
    host: str
    port: int
    handler: Callable[[Any], None]
    table_name: str
    action_name: str = DEFAULT_ACTION_NAME
    offset: int = -1
    resub: bool = False
    filter: Union[str, NDArrayAnnotated, None] = None
    msg_as_table: bool = False
    batch_size: int = 0
    throttle: float = 1.0
    userid: str = ""
    password: str = ""
    stream_deserializer: Optional[StreamDeserializerAnnotated] = None
    backup_sites: Optional[List[str]] = Field(default_factory=list)
    resubscribe_interval: int = 100
    sub_once: bool = False

    @model_validator(mode="after")
    def throttle_check(self):
        if self.throttle <= DDB_EPSILON:
            raise ValueError("throttle must be greater than 0.")
        return self

    @model_validator(mode="after")
    def filter_check(self):
        if self.filter is None:
            self.filter = np.array([], dtype='int64')
        return self

    @model_validator(mode="after")
    def backup_sites_check(self):
        if self.backup_sites is None:
            self.backup_sites = []
        return self


class StreamingClient(ABC):
    _cpp: Any

    @abstractmethod
    def subscribe(self, *args, **kwargs): ...

    @final
    @overload
    def unsubscribe(self, host: str, port, table_name: str, action_name: str = DEFAULT_ACTION_NAME): ...

    @final
    @overload
    def unsubscribe(self, subscribe_info: SubscribeInfo): ...

    @final
    @dispatcher
    def unsubscribe(self, *args, **kwargs):
        raise TypeError("Invalid arguments for unsubscribe method.")

    @unsubscribe.register
    def _(self, host: str, port: int, table_name: str, action_name: str = DEFAULT_ACTION_NAME):
        subscribe_info = SubscribeInfo(host, port, table_name, action_name)
        return self._unsubscribe_internal(subscribe_info)

    @unsubscribe.register
    def _(self, subscribe_info: SubscribeInfo):
        return self._unsubscribe_internal(subscribe_info)

    def _unsubscribe_internal(self, info: SubscribeInfo):
        self._cpp.unsubscribe(info.host, info.port, info.table_name, info.action_name)

    @property
    def topics(self) -> List[SubscribeInfo]:
        topics = self._cpp.getSubscriptionTopics()
        res = []
        for topic_str in topics:
            topic = SubscribeInfo.parse(topic_str)
            res.append(topic)
        return res

    @property
    def topic_strs(self) -> List[str]:
        return self._cpp.getSubscriptionTopics()


class ThreadedStreamingClientConfig(StreamingClientConfig):
    port: int = Field(0, ge=0)


class ThreadedClient(StreamingClient):
    def __init__(self, config: ThreadedStreamingClientConfig = None, **kwargs):
        config = organize_config(ThreadedStreamingClientConfig, config, kwargs)
        self._cpp = ddbcpp.ThreadedClient(config.model_dump())

    @overload
    def subscribe(
        self,
        host: str,
        port: int,
        handler: Callable,
        table_name: str,
        action_name: str = DEFAULT_ACTION_NAME,
        *,
        offset: int = -1,
        resub: bool = False,
        filter: Union[str, NDArrayAnnotated, None] = None,
        msg_as_table: bool = False,
        batch_size: int = 0,
        throttle: float = 1.0,
        userid: str = "",
        password: str = "",
        stream_deserializer: Optional[StreamDeserializerAnnotated] = None,
        backup_sites: Optional[List[str]] = Field(default_factory=list),
        resubscribe_interval: int = 100,
        sub_once: bool = False,
    ) -> SubscribeInfo: ...

    @overload
    def subscribe(
        self,
        *,
        config: SubscriptionConfig = None,
        **kwargs,
    ) -> SubscribeInfo: ...

    def subscribe(
        self,
        host: str = None,
        port: int = None,
        handler: Callable = None,
        table_name: str = None,
        action_name: str = None,
        *,
        config: SubscriptionConfig = None,
        **kwargs,
    ) -> SubscribeInfo:
        if host is not None:
            kwargs['host'] = host
        if port is not None:
            kwargs['port'] = port
        if handler is not None:
            kwargs['handler'] = handler
        if table_name is not None:
            kwargs['table_name'] = table_name
        if action_name is not None:
            kwargs['action_name'] = action_name
        config = organize_config(SubscriptionConfig, config, kwargs)
        d = config.model_dump()
        d['stream_deserializer'] = (
            config.stream_deserializer.cpp
            if config.stream_deserializer
            else None
        )
        topic_str = self._cpp.subscribe(d)
        return SubscribeInfo.parse(topic_str)


class ThreadPooledStreamingClientConfig(ThreadedStreamingClientConfig):
    thread_count: int = Field(1)


class ThreadPooledClient(StreamingClient):
    def __init__(self, config: ThreadPooledStreamingClientConfig = None, **kwargs):
        config = organize_config(ThreadPooledStreamingClientConfig, config, kwargs)
        self._cpp = ddbcpp.ThreadPooledClient(config.model_dump())

    @overload
    def subscribe(
        self,
        host: str,
        port: int,
        handler: Callable,
        table_name: str,
        action_name: str = DEFAULT_ACTION_NAME,
        *,
        offset: int = -1,
        resub: bool = False,
        filter: Union[str, NDArrayAnnotated, None] = None,
        msg_as_table: bool = False,
        batch_size: int = 0,
        throttle: float = 1.0,
        userid: str = "",
        password: str = "",
        stream_deserializer: Optional[StreamDeserializerAnnotated] = None,
        backup_sites: Optional[List[str]] = Field(default_factory=list),
        resubscribe_interval: int = 100,
        sub_once: bool = False,
    ) -> SubscribeInfo: ...

    @overload
    def subscribe(
        self,
        *,
        config: SubscriptionConfig = None,
        **kwargs,
    ) -> SubscribeInfo: ...

    def subscribe(
        self,
        host: str = None,
        port: int = None,
        handler: Callable = None,
        table_name: str = None,
        action_name: str = None,
        *,
        config: SubscriptionConfig = None,
        **kwargs,
    ) -> SubscribeInfo:
        if host is not None:
            kwargs['host'] = host
        if port is not None:
            kwargs['port'] = port
        if handler is not None:
            kwargs['handler'] = handler
        if table_name is not None:
            kwargs['table_name'] = table_name
        if action_name is not None:
            kwargs['action_name'] = action_name
        config = organize_config(SubscriptionConfig, config, kwargs)
        d = config.model_dump()
        d['stream_deserializer'] = config.stream_deserializer.cpp \
            if config.stream_deserializer else None
        topic_str = self._cpp.subscribe(d)
        return SubscribeInfo.parse(topic_str)
