# DolphinDB Python SDK

A C++ boosted API for Python, built on Pybind11.

## Requirements

To use DolphinDB Python SDK, you'll need:

- Python:
  - CPython: version 3.8 and newer
- DolphinDB Server
- Packages:
  - NumPy: version 1.18 and newer
  - pandas: version 1.0 and newer, but earlier than 3.0, and not version 1.3.0
  - future
  - packaging
  - pydantic: version 2.0 and newer
- Extension Packages:
  - PyArrow: version 9.0.0 and newer

## Installation

To install the DolphinDB package, use the following command:

```sh
pip install dolphindb
```

## Example Code

Here's an example of using DolphinDB Python SDK:

```python
import dolphindb as ddb

conn = ddb.Session()
conn.connect("localhost", 8848, "admin", "123456")

conn.run("1+1;")
-----------------------
2
```