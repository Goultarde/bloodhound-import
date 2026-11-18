import argparse
import asyncio
import logging
import os

from bloodhound_import.ce import upload_files


async def main():
    """
        Main function
    """
    argparser = argparse.ArgumentParser("bloodhound-import.py")
    argparser.add_argument("files", help="Files to parse.", nargs="+")
    argparser.add_argument("--ce-url", help="BloodHound CE base URL. Uses BLOODHOUND_CE_TOKEN for authentication.")
    argparser.add_argument("-du", "--database-user", help="Username to connect to neo4j, if not specified will try to auto detect a config file.")
    argparser.add_argument("-dp", "--database-password", help="Password to connect to neo4j, if not specified will try to auto detect a config file.")
    argparser.add_argument("--database", help="The host neo4j is running on.", default="localhost")
    argparser.add_argument("-p", "--port", help="Port of neo4j", default=7687)
    argparser.add_argument("-s", "--scheme", help="URI Scheme used to communicate with neo4j", default="bolt")
    argparser.add_argument("-v", "--verbose", help="Verbose output", action='store_true')
    arguments = argparser.parse_args()

    if arguments.ce_url:
        token = os.environ.get("BLOODHOUND_CE_TOKEN", "").strip()
        if not token:
            argparser.error("set BLOODHOUND_CE_TOKEN to a BloodHound CE JWT")
        try:
            job_id = upload_files(arguments.ce_url, token, arguments.files)
        except (OSError, ValueError, RuntimeError) as exc:
            argparser.exit(1, f"BloodHound CE: {exc}\n")
        print(f"Files submitted to BloodHound CE. Follow job {job_id} in File Ingest.")
        return

    from bloodhound_import import database
    from bloodhound_import.importer import parse_file, add_constraints
    from ijson import JSONError
    from neo4j.exceptions import DriverError, Neo4jError
    from zipfile import BadZipFile
    if arguments.database_password is None:
        arguments.database_user, arguments.database_password = database.detect_db_config()
        if arguments.database_password is None:
            argparser.error("could not detect Neo4j credentials; specify -du and -dp")

    logging.basicConfig(format="[%(levelname)s] %(asctime)s - %(message)s")
    if arguments.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    driver = database.init_driver(arguments.database, arguments.port, arguments.scheme, arguments.database_user, arguments.database_password)

    try:
        async with driver.session() as session:
            await session.execute_write(add_constraints)

        logging.info("Parsing %s files", len(arguments.files))
        for filename in arguments.files:
            await parse_file(filename, driver)

        logging.info("Done")
    except (BadZipFile, DriverError, JSONError, Neo4jError, OSError, ValueError) as exc:
        argparser.exit(1, f"Neo4j import failed: {exc}\n")
    finally:
        await driver.close()


if __name__ == "__main__":
    asyncio.run(main())
