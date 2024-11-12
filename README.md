# ETL Processor

A sample project for a Python-based ETL tool

## Installation

1. Download the zip release asset from the [releases page](https://github.com/cruz-f/etl-processor/releases)
2. Unzip the release asset

```sh
unzip etl-processor-vRELEASE_VERSION.zip
```

3. Change directory to the unzipped folder

```sh
cd etl-processor-vRELEASE_VERSION
```

4. Install the package

```sh
pip install etl_processor-vRELEASE_VERSION-py3-none-any.whl
```

## Usage

### 1. Extract

Extract financial instruments in the financial instrument reference data system (FIRDS). It starts by extracting DLTINS files from the FIRDS database by ESMA. Then, it parses the main attributes of the financial instruments returning a list of FIRDS documents.

Asynchronous extraction:

```python
import asyncio
firds_extractor = FIRDSExtractor(
    firds_url='https://example.com',
    data_dir='data',
)
async def main() -> None:
    await firds_extractor.arun()
asyncio.run(main())
```

Synchronous extraction:

```python
from etl_processor import FIRDSExtractor

extractor = FIRDSExtractor(
    firds_url='https://example.com',
    data_dir='data',
)
extractor.run()
```

Example output:

```md
| FinInstrmGnlAttrbts.Id | FinInstrmGnlAttrbts.FullNm                  | FinInstrmGnlAttrbts.ClssfctnTp | FinInstrmGnlAttrbts.CmmdtyDerivInd | FinInstrmGnlAttrbts.NtnlCcy | Issr                  |
|------------------------|---------------------------------------------|-------------------------------|-----------------------------------|----------------------------|-----------------------|
| DE000A1R07V3           | Kreditanst.f.Wiederaufbau Anl.v.2014 (2021) | DBFTFB                        | False                             | EUR                        | 549300GDPG70E3MBBU98  |
| DE000A1R07V3           | KFW 1 5/8 01/15/21                          | DBFTFB                        | False                             | EUR                        | 549300GDPG70E3MBBU98  |
```

### 2. Transform

Transformation tool to obtain new insights from the financial instruments in the financial instrument reference data system (FIRDS).

Asynchronous transformation:

```python
import asyncio
from etl_processor import FIRDSTransformer

transformer = FIRDSTransformer(
    data_dir='data',
)
async def main() -> None:
    await transformer.arun()
asyncio.run(main())
```

Synchronous transformation:

```python
from etl_processor import FIRDSTransformer

transformer = FIRDSTransformer(
    data_dir='data',
)
transformer.run()
```

### 3. Load

Loading tool to save the FIRDS CSV into a file storage system.

Asynchronous loading:

```python
import asyncio
from etl_processor import FIRDSLoader

storage_options = {
    'anon': False,
}
loader = FIRDSLoader(
    data_dir='data',
    system='s3',
    target_path='s3://my-bucket/firds_gold.csv',
    storage_options=storage_options,
)
async def main() -> None:
    await loader.arun()
asyncio.run(main())
```

Synchronous loading:

```python
from etl_processor import FIRDSLoader

storage_options = {
    'anon': False,
}
loader = FIRDSLoader(
    data_dir='data',
    system='s3',
    target_path='s3://my-bucket/firds_gold.csv',
    storage_options=storage_options,
)
loader.run()
```

## Development

### 1. Install dependencies

```sh
poetry install
```

### 2. Run tests

```sh
poetry run coverage run -m pytest tests
poetry run coverage combine coverage
poetry run coverage report
```

### 3. Build package

```sh
poetry build
```

### How to implement my own ETL tool?

One can implement their own ETL tool by extending the `Tool` interface. The `Tool` interface provides a template for implementing the ETL tool with synchronous and asynchronous extraction logic. Hence, one must implement the `run` and `arun` methods. In addition, one can add custom attributes and initialization logic.

```python
from etl_processor import Tool

class YourExtractor(Tool):
    """Your extraction tool"""

    def __init__(self, *args, **kwargs):
        """Add your own initialization logic and custom attributes."""
        super().__init__(*args, **kwargs)

    def run(self):
        """Your synchronous extraction logic."""
        pass

    async def arun(self):
        """Your asynchronous extraction logic."""
        pass

```

## Roadmap

- [ ] Add Pipeline class to orchestrate the ETL process. The pipeline class will be responsible for running the ETL process in a sequence of steps. It will be responsible for using the ETL `Tool` objects to extract, transform, and load the data.
- [ ] Increase unit test granularity by patching the `xml.etree.ElementTree` library. This will allow for more fine-grained testing of the `FIRDSExtractor` class.
- [ ] Obtain more insights and attributes in the transformation step.

## License

Check the [LICENSE](LICENSE) file for more information.

## Authors

- [@cruz-f](https://github.com/cruz-f)
