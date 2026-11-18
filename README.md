# bloodhound-import

![Python 2.7 and 3 compatible](https://img.shields.io/badge/python-3.7-blue.svg)
![PyPI version](https://img.shields.io/pypi/v/bloodhound_import.svg)
![License: MIT](https://img.shields.io/pypi/l/bloodhound_import.svg)

Bloodhound-import accepts BloodHound collection files. It can submit them to
BloodHound CE or write Active Directory graph data directly to Neo4j.

## BloodHound CE

Set `BLOODHOUND_CE_TOKEN` to a BloodHound CE session JWT, then run:

```sh
bloodhound-import --ce-url http://localhost:8080 collection.zip
```

Pass multiple ZIP or JSON files to submit them as one job. The tool streams each
file to the [BloodHound API](https://bloodhound.specterops.io/integrations/bloodhound-api/working-with-api)
and prints the job ID when CE queues it. Check **Administration → Data
Collection → File Ingest** for processing results. Use HTTPS for remote instances.
CE handles validation, graph creation and analysis, so this mode has the same
ingestion pipeline as a browser upload.

## Installation

Requires Python 3.8 or later.

Install with `pip install bloodhound_import` or clone the git and install with `sudo python setup.py install`.

## Direct Neo4j import

Without `--ce-url`, the tool streams AD JSON and ZIP files to Neo4j 5 using the
BloodHound CE labels and writes records in batches. It covers RustHound CE v6 and
the main SharpHound AD types, including AD CS and standalone sessions.

Use a fresh Neo4j graph when replacing an import made by an earlier version.
Direct import merges records. It does not remove stale relationships from old
imports or trigger the BloodHound CE ingestion job and analysis. That analysis
computes additional relationships from the imported graph. For full pipeline
coverage, including Azure, OpenGraph and collector-specific cases such as
OWNER RIGHTS restrictions and collected DC registry data, use `--ce-url`.

```bash
usage: bloodhound-import.py [-h] [-du DATABASE_USER] [-dp DATABASE_PASSWORD]
                            [--database DATABASE] [-p PORT] [-v]
                            files [files ...]
```

Example:
`bloodhound-import -du neo4j -dp neo4j ~/Desktop/SessionLoop_20190115133114*.json`

If the -du and -dp options are not specified, the tool will try to auto detect these values from the bloodhound config file.
