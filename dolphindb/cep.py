from dolphindb._core import DolphinDBRuntime
from dolphindb import Session
from dolphindb.typing import _DATA_FORM

from typing import List, Optional, Callable, Union

ddbcpp = DolphinDBRuntime()._ddbcpp


def _check_type(cls):
    if cls._is_base:
        return dict()
    type_dict = dict()
    if not hasattr(cls, "__annotations__"):
        setattr(cls, "__annotations__", dict())
    for k, v in cls.__annotations__.items():
        if not isinstance(v, _DATA_FORM):
            raise ValueError("Invalid data form.")
        data_type = v._data_type
        data_form = v._data_form
        exparam = v._exparam
        type_dict[k] = [data_type, data_form, exparam]
    return type_dict


class _EventMeta(type):
    def __init__(cls, *args):
        cls._type_cache = _check_type(cls)
        if cls._is_base:
            cls._event_name = None
            cls._is_base = False
        elif not cls._event_name:
            cls._event_name = cls.__name__
        super().__init__(*args)


class Event(metaclass=_EventMeta):
    """To define event types, for example, defining
    an event with three attributes:

    ```
    >>> from dolphindb.cep import Event
    >>> from dolphindb.typing import Scalar, Vector
    >>> import dolphindb.settings as keys
    >>>
    >>> class MyEvent(Event):
    ...     a: Scalar[keys.DT_INT]
    ...     b: Scalar[keys.DT_DECIMAL32, 3]
    ...     c: Vector[keys.DT_DOUBLE]
    ...
    ```

    Attribute a: Scalar of type INT
    Attribute b: Scalar of type DECIMAL32 with scale 3
    Attribute c: Vector of type DOUBLE

    For this custom event, the event type is MyEvent and it
    has a default constructor, which can be constructed in
    the following ways:
    ```
    >>> from decimal import Decimal
    >>> MyEvent(1, Decimal("1.234"), [1, 2, 3])
    >>> MyEvent(1, Decimal("1.234"), c=[1, 2, 3])
    >>> MyEvent(a=1, b=Decimal("1.234"), c=[1, 2, 3])
    ```

    Note:
        By default, the class name is used as the event type
        when defining events. You can specify the event type
        by setting `_event_name`. See the example below:

    ```
    >>> class TestEvent1(Event):
    ...     a: Scalar[keys.DT_INT]
    ...
    >>> class TestEvent2(Event):
    ...     _event_name = "Test"
    ...     a: Scalar[keys.DT_INT]
    ...
    >>> TestEvent1._event_name
    TestEvent1
    >>> TestEvent2._event_name
    Test
    ```
    """
    _type_cache = dict()
    _event_name = None
    _is_base = True

    def __init__(self, *args, **kwargs) -> None:
        i = 0
        for k, v in self._type_cache.items():
            if i < len(args):
                setattr(self, k, args[i])
            elif k in kwargs:
                setattr(self, k, kwargs[k])
            i += 1

    def __repr__(self) -> str:
        data_dict = dict()
        for _ in self._type_cache.keys():
            data_dict[_] = getattr(self, _)
        return f"{self._event_name}({str(data_dict)})"


class EventSender:
    """To write events to a heterogeneous stream table,
    it is necessary to define events in the Python client
    with the same schema as on the DolphinDB server.

    Args:
        ddbSession : a Session connected to DolphinDB server.

        tableName : a str, indicating the heterogeneous stream table to be written into.

        eventSchema : custom event types being sent, events need
        to inherit from the Event class.

        eventTimeFields : a str or a list of str specifying the
        time field(s) for each event. If there is a time column
        in the heterogeneous stream table, it's necessary to specify
        the time field(s) for each event; otherwise, it's not required.
        If all events share the same time field name, simply specify
        it as a single string. If the time field varies among events,
        specify a list of strings corresponding to each time field,
        with the same order as the eventType specified in the eventSchema
        parameter. The default value is None, indicating no time field
        is specified.

        commonFields : a list of str representing the common fields
        among events. It is an optional parameter that used to filter
        common field from events. It can only be specified when the
        heterogeneous stream table contains common fields across
        different event types.
    """
    def __init__(
        self,
        ddbSession: Session,
        tableName: str,
        eventSchema: List[_EventMeta],
        eventTimeFields: Optional[Union[List[str], str]] = None,
        commonFields: Optional[List[str]] = None,
    ) -> None:
        if not isinstance(ddbSession, Session):
            raise TypeError("ddbSession must be a dolphindb Session.")
        if not isinstance(tableName, str):
            raise TypeError("tableName must be a str.")
        if not isinstance(eventSchema, list):
            raise TypeError("eventSchema must be a list of child class of Event.")
        for scheme in eventSchema:
            if scheme is Event:
                raise ValueError("The event defined must be a child class of Event.")
            if not isinstance(scheme, type):
                raise ValueError("The event defined must be a child class of Event.")
            if not issubclass(scheme, Event):
                raise ValueError("The event defined must be a child class of Event.")
        if not eventTimeFields:
            eventTimeFields = []
        if not commonFields:
            commonFields = []
        if isinstance(eventTimeFields, str):
            eventTimeFields = [eventTimeFields]
        if not isinstance(eventTimeFields, list):
            raise TypeError("eventTimeFields must be a str or a list of str.")
        for elm in eventTimeFields:
            if not isinstance(elm, str):
                raise TypeError("eventTimeFields must be a str or a list of str.")
        if not isinstance(commonFields, list):
            raise TypeError("commFields must be a list of str.")
        for elm in commonFields:
            if not isinstance(elm, str):
                raise TypeError("commonFields must be a str or a list of str.")
        self.sender = ddbcpp.EventSender(ddbSession.cpp, tableName, eventSchema, eventTimeFields, commonFields)
        self.sess = ddbSession

    def __del__(self):
        self.sender = None
        self.sess = None

    def sendEvent(self, event: Event):
        """Send the events to the DolphinDB server.

        Args:
            event : custom event instance.
        """
        if not self.sess:
            raise RuntimeError("The connection to dolphindb has not been established.")
        if self.sess.isClosed():
            raise RuntimeError("Session has been closed.")
        self.sender.sendEvent(event)


class EventClient:
    """Event subscription client.

    Args:
        eventSchema : custom event types subscribed to,
        events need to inherit from the Event class.

        eventTimeFields : a str or a list of str. If there
        is a time column in the heterogeneous stream table,
        it's necessary to specify the time field(s) for each
        event. If all events share the same time field name,
        simply specify it as a string representing the time field.
        If the time field varies among events, you need to specify
        the corresponding time field(s) based on the order of events
        passed in the eventSchema parameter. The default value
        is None, indicating no time field is specified.

        commonFields : a list of str representing common
        fields among events. If there are columns in the
        heterogeneous stream table that represent the same
        fields, this parameter needs to be specified; otherwise,
        it is not required. The default value is None, indicating
        no common fields are specified.
    """
    def __init__(
        self,
        eventSchema: List[_EventMeta],
        eventTimeFields: Optional[Union[List[str], str]] = None,
        commonFields: Optional[List[str]] = None,
    ) -> None:
        if not isinstance(eventSchema, list):
            raise TypeError("eventSchema must be a list of child class of Event.")
        for scheme in eventSchema:
            if scheme is Event:
                raise ValueError("The event defined must be a child class of Event.")
            if not isinstance(scheme, type):
                raise ValueError("The event defined must be a child class of Event.")
            if not issubclass(scheme, Event):
                raise ValueError("The event defined must be a child class of Event.")
        if not eventTimeFields:
            eventTimeFields = []
        if not commonFields:
            commonFields = []
        if isinstance(eventTimeFields, str):
            eventTimeFields = [eventTimeFields]
        if not isinstance(eventTimeFields, list):
            raise TypeError("eventTimeFields must be a str or a list of str.")
        for elm in eventTimeFields:
            if not isinstance(elm, str):
                raise TypeError("eventTimeFields must be a str or a list of str.")
        if not isinstance(commonFields, list):
            raise TypeError("commFields must be a list of str.")
        for elm in commonFields:
            if not isinstance(elm, str):
                raise TypeError("commonFields must be a str or a list of str.")
        self.client = ddbcpp.EventClient(eventSchema, eventTimeFields, commonFields)

    def subscribe(
        self,
        host: str,
        port: int,
        handler: Callable[[Event], None],
        tableName: str,
        actionName: str = None,
        offset: int = -1,
        resub: bool = False,
        userName: str = None,
        password: str = None,
    ):
        """Subscribe events from the heterogeneous stream table.

        Args:
            host : server address. It can be IP address, domain, or LAN hostname, etc
            port : port name.
            handler : user-defined callback function for processing incoming data.
            tableName : name of the published table.
            actionName : name of the subscription task. Defaults to "".
            offset : an integer indicating the position of the first message where the subscription begins. Defaults to -1.
            resub : True means to keep trying to subscribe to table after the subscription attempt fails. Defaults to False.
            userName : username. Defaults to None, indicating no login.
            password : password. Defaults to None, indicating no login.
        """
        if actionName is None:
            actionName = ""
        if userName is None:
            userName = ""
        if password is None:
            password = ""
        self.client.subscribe(
            host,
            port,
            handler,
            tableName,
            actionName,
            offset,
            resub,
            userName,
            password,
        )

    def unsubscribe(self, host: str, port: int, tableName: str, actionName: str = None):
        """Unsubscribe.

        Args:
            host : server address. It can be IP address, domain, or LAN hostname, etc.
            port : port name.
            tableName : name of the published table.
            actionName : name of the subscription task. Defaults to None.
        """
        if actionName is None:
            actionName = ""
        self.client.unsubscribe(host, port, tableName, actionName)

    def getSubscriptionTopics(self):
        """Get all subscription topics.

        Returns:
            a list of all subscription topics in the format of "host/port/tableName/actionName".
        """
        return self.client.getSubscriptionTopics()


__all__ = [
    Event,
    EventSender,
    EventClient,
]
