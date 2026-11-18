from io import BytesIO
import json
import unittest
from unittest.mock import AsyncMock, patch

from bloodhound_import.importer import parse_json


class DirectImportTest(unittest.IsolatedAsyncioTestCase):
    async def test_current_ad_objects_and_relationships(self):
        tx = AsyncMock()
        tx.run.return_value = AsyncMock()
        session = AsyncMock()

        async def execute(write):
            await write(tx)

        session.execute_write.side_effect = execute

        def principal(kind, identifier):
            return {"ObjectType": kind, "ObjectIdentifier": identifier}

        files = {
            "computers": [
                {"ObjectIdentifier": "pc", "AllowedToDelegate": [principal("Computer", "server")]},
                {"ObjectIdentifier": "server", "LocalGroups": [{
                    "ObjectIdentifier": "server-544", "Collected": True,
                    "Results": [principal("User", "alice")],
                }]},
                {"ObjectIdentifier": "other"},
            ],
            "domains": [{"ObjectIdentifier": "domain", "Links": [{"GUID": "policy"}],
                         "Trusts": [{"TargetDomainSid": "remote", "TrustType": "ParentChild",
                                     "TrustDirection": "Bidirectional"}]}],
            "enterprisecas": [{"ObjectIdentifier": "ca", "HostingComputer": "server",
                               "EnabledCertTemplates": [principal("CertTemplate", "template")]}],
            "sessions": [{"ComputerSID": "pc", "UserSID": "alice", "LogonType": 2}],
        }
        with patch("bloodhound_import.importer.BATCH_SIZE", 2):
            for kind, records in files.items():
                body = json.dumps({"data": records, "meta": {"type": kind, "version": 6}})
                await parse_json(BytesIO(body.encode()), kind, session)

        self.assertEqual(session.execute_write.await_count, 5)
        kinds = {
            (query.args[0].split("[r:")[1].split("]")[0], row["source"], row["target"])
            for query in tx.run.await_args_list if "[r:" in query.args[0]
            for row in query.kwargs["rows"]
        }
        self.assertEqual(kinds, {
            ("AllowedToDelegate", "PC", "SERVER"),
            ("LocalToComputer", "SERVER-544", "SERVER"),
            ("MemberOfLocalGroup", "ALICE", "SERVER-544"),
            ("GPLink", "POLICY", "DOMAIN"),
            ("SameForestTrust", "REMOTE", "DOMAIN"),
            ("SameForestTrust", "DOMAIN", "REMOTE"),
            ("HostsCAService", "SERVER", "CA"),
            ("PublishedTo", "TEMPLATE", "CA"),
            ("HasSession", "PC", "ALICE"),
        })
