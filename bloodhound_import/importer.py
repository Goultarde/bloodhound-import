"""Stream BloodHound AD files into Neo4j using CE node and relationship kinds."""

from collections import defaultdict
import logging
from pathlib import Path
from zipfile import ZipFile

import ijson


NODE_TYPES = {
    "computers": "Computer", "containers": "Container", "users": "User",
    "groups": "Group", "domains": "Domain", "gpos": "GPO", "ous": "OU",
    "aiacas": "AIACA", "rootcas": "RootCA", "enterprisecas": "EnterpriseCA",
    "ntauthstores": "NTAuthStore", "certtemplates": "CertTemplate",
    "issuancepolicies": "IssuancePolicy", "sites": "Site",
    "siteservers": "SiteServer", "sitesubnets": "SiteSubnet",
}
NODE_KINDS = {kind.lower(): kind for kind in (
    *NODE_TYPES.values(), "Base", "ADLocalGroup", "ADLocalUser",
)}
ACL_KINDS = {kind.lower(): kind for kind in """
    AllExtendedRights ForceChangePassword AddMember AddAllowedToAct GenericAll
    WriteDacl GenericWrite AddSelf ReadLAPSPassword ReadGMSAPassword WriteSPN
    AddKeyCredentialLink GetChanges GetChangesAll GetChangesInFilteredSet
    WriteAccountRestrictions WriteGPLink SyncLAPSPassword DCSync ManageCertificates
    ManageCA Enroll WritePKIEnrollmentFlag WritePKINameFlag WriteAltSecurityIdentities
    WritePublicInformation OwnsLimitedRights WriteOwnerLimitedRights OwnsRaw WriteOwnerRaw
""".split()}
ACL_KINDS.update(owns="OwnsRaw", writeowner="WriteOwnerRaw")
LOCAL_GROUPS = {"LocalAdmins": "544", "RemoteDesktopUsers": "555",
                "DcomUsers": "562", "PSRemoteUsers": "580"}
BATCH_SIZE = 500


async def add_constraints(tx):
    await tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:Base) REQUIRE n.objectid IS UNIQUE")


def principal(obj):
    kind = NODE_KINDS.get((obj.get("ObjectType") or "").lower(), "Base")
    return kind, obj.get("ObjectIdentifier")


class Batch:
    def __init__(self):
        self.rows = defaultdict(list)

    def node(self, endpoint, properties, collected=False):
        kind, identifier = endpoint
        if not identifier:
            raise ValueError(f"missing ObjectIdentifier for {kind}")
        props = {key: value for key, value in properties.items()
                 if key not in ("objectid", "lastseen", "lastcollected", "reconcile")}
        for key in ("name", "distinguishedname", "operatingsystem"):
            if isinstance(props.get(key), str):
                props[key] = props[key].upper()
        timestamp = "SET n.lastcollected = datetime() " if collected else ""
        query = (
            f"UNWIND $rows AS row MERGE (n:Base {{objectid: row.id}}) "
            f"SET n:{kind}, n.lastseen = datetime() SET n += row.props {timestamp}"
        )
        self.rows[query].append(dict(id=identifier.upper(), props=props))

    def edge(self, source, target, kind, **properties):
        source_kind, source_id = source
        target_kind, target_id = target
        if not source_id or not target_id:
            return
        query = (
            f"UNWIND $rows AS row MERGE (a:Base {{objectid: row.source}}) "
            f"SET a:{source_kind}, a.lastseen = datetime() "
            f"MERGE (b:Base {{objectid: row.target}}) "
            f"SET b:{target_kind}, b.lastseen = datetime() "
            f"MERGE (a)-[r:{kind}]->(b) SET r += row.props SET r.lastseen = datetime()"
        )
        self.rows[query].append(dict(source=source_id.upper(), target=target_id.upper(),
                                     props={"isacl": False, **properties}))

    async def write(self, tx):
        for query, rows in self.rows.items():
            await (await tx.run(query, rows=rows)).consume()


def session_edge(batch, session, fallback=None):
    props = {"logontype": session["LogonType"]} if "LogonType" in session else {}
    batch.edge(("Computer", session.get("ComputerSID") or fallback),
               ("User", session.get("UserSID")), "HasSession", **props)


def local_group(batch, computer, identifier, members, name=None):
    group = ("ADLocalGroup", identifier)
    batch.node(group, {"name": name} if name else {})
    batch.edge(group, computer, "LocalToComputer")
    for member in members:
        batch.edge(principal(member), group, "MemberOfLocalGroup")


def trusts(batch, domain, entries):
    for trust in entries:
        direction = trust["TrustDirection"]
        direction = {0: "Disabled", 1: "Inbound", 2: "Outbound", 3: "Bidirectional"}.get(direction, direction)
        if direction == "Disabled":
            continue
        if direction not in ("Inbound", "Outbound", "Bidirectional"):
            raise ValueError(f"unknown trust direction {direction!r}")
        trust_type = trust["TrustType"]
        trust_type = {0: "ParentChild", 1: "CrossLink", 2: "Forest",
                      3: "External", 4: "Unknown"}.get(trust_type, trust_type)
        other = ("Domain", trust["TargetDomainSid"])
        batch.node(other, {k: v for k, v in {
            "name": trust.get("TargetDomainName"), "domainsid": other[1].upper(),
        }.items() if v})
        kind = "SameForestTrust" if trust_type in ("ParentChild", "TreeRoot", "CrossLink") else "CrossForestTrust"
        props = dict(trusttype=trust_type, transitive=trust.get("IsTransitive", False))
        attributes = trust.get("TrustAttributes")
        if attributes is not None:
            attributes = int(attributes)
        if direction in ("Inbound", "Bidirectional"):
            inbound = dict(trustattributesinbound=attributes)
            if attributes is not None:
                inbound["tgtdelegation"] = trust.get("TGTDelegationEnabled", False)
            batch.edge(other, domain, kind, **props, **inbound)
            if kind == "CrossForestTrust" and trust.get("TGTDelegationEnabled"):
                batch.edge(other, domain, "AbuseTGTDelegation", trusttype=trust_type)
        if direction in ("Outbound", "Bidirectional"):
            filtering = trust.get("SidFilteringEnabled", False)
            batch.edge(domain, other, kind, **props, spoofsidhistoryblocked=filtering,
                       trustattributesoutbound=attributes)
            if kind == "CrossForestTrust" and not filtering:
                batch.edge(other, domain, "SpoofSIDHistory", trusttype=trust_type)


def convert_object(batch, kind, obj, warnings):
    if kind == "sessions":
        session_edge(batch, obj)
        return

    identifier = obj.get("ObjectIdentifier")
    if not isinstance(identifier, str) or not identifier.strip():
        warnings.add(f"skipped {kind} records without an ObjectIdentifier")
        return
    current = (kind, identifier)
    props = dict(obj.get("Properties") or {})
    for field in ("IsDeleted", "IsACLProtected", "InheritanceHashes"):
        if field in obj:
            props[field.lower()] = obj[field]
    domain_sid = obj.get("DomainSID") or props.get("domainsid")
    if domain_sid:
        props["domainsid"] = domain_sid.upper()

    aces = obj.get("Aces") or []
    registry = obj.get("CARegistryData") or {}
    ca_security = registry.get("CASecurity") or {}
    if kind == "EnterpriseCA" and ca_security.get("Collected"):
        registry_rights = {"manageca", "managecertificates", "enroll"}
        aces = [
            ace for ace in aces
            if ace.get("PrincipalSID") == obj.get("HostingComputer")
            or ace["RightName"].lower() not in registry_rights
        ] + [
            ace for ace in ca_security.get("Data") or []
            if (ace.get("PrincipalType") or "").lower() not in ("adlocalgroup", "localgroup")
            and ace["RightName"].lower() != "owns"
        ]
    owner_rights = any((ace.get("PrincipalSID") or "").endswith("S-1-3-4") for ace in aces)
    for ace in aces:
        sid = ace.get("PrincipalSID")
        if not sid or sid.upper() == identifier.upper() or sid.endswith("S-1-3-4"):
            continue
        right = ACL_KINDS.get(ace["RightName"].lower())
        if not right:
            warnings.add(f"unsupported ACE kind {ace['RightName']!r}")
            continue
        if right == "OwnsRaw":
            props["ownersid"] = sid.upper()
        # shortcut: OWNER RIGHTS limits need CE's full ACL converter to emit limited rights.
        if owner_rights and right in ("OwnsRaw", "WriteOwnerRaw"):
            warnings.add("ownership edges with OWNER RIGHTS restrictions require native CE import")
            continue
        source_kind = NODE_KINDS.get((ace.get("PrincipalType") or "").lower(), "Base")
        batch.edge((source_kind, sid), current, right, isacl=True,
                   isinherited=ace.get("IsInherited", False), inheritancehash=ace.get("InheritanceHash", ""))

    container = obj.get("ContainedBy")
    if container:
        batch.edge(principal(container), current, "Contains")
    for child in obj.get("ChildObjects") or []:
        batch.edge(current, principal(child), "Contains")
    if kind in ("Domain", "OU", "Site"):
        for link in obj.get("Links") or []:
            batch.edge(("GPO", link["GUID"]), current, "GPLink", enforced=link.get("IsEnforced", False))

    primary = obj.get("PrimaryGroupSID") or obj.get("PrimaryGroupSid")
    if primary:
        batch.edge(current, ("Group", primary), "MemberOf", isprimarygroup=True)
    for member in obj.get("Members") or []:
        member_endpoint = principal(member)
        if (member_endpoint[1] or "").upper().startswith("DN="):
            warnings.add("group members referenced by distinguished name require native CE import")
        else:
            batch.edge(member_endpoint, current, "MemberOf", isprimarygroup=False)
    for field in ("AllowedToDelegate", "HasSIDHistory", "DumpSMSAPassword"):
        for target in obj.get(field) or []:
            batch.edge(current, principal(target), field)
    for actor in obj.get("AllowedToAct") or []:
        batch.edge(principal(actor), current, "AllowedToAct")
    for spn in obj.get("SPNTargets") or obj.get("SpnTargets") or []:
        if spn.get("Service", "").lower() == "sqladmin":
            batch.edge(current, ("Computer", spn.get("ComputerSID")), "SQLAdmin",
                       isacl=True, port=spn.get("Port"))
        else:
            warnings.add(f"unsupported SPN service {spn.get('Service')!r}")

    if kind == "Computer":
        for field in ("Sessions", "PrivilegedSessions", "RegistrySessions"):
            result = obj.get(field) or {}
            if result.get("Collected", True):
                for session in result.get("Results") or []:
                    session_edge(batch, session, identifier)
        for group in obj.get("LocalGroups") or []:
            members = group.get("Results") or []
            name = group.get("Name") or ""
            if group.get("Collected") and (members or name.upper().startswith("DIRECT ACCESS USERS")):
                local_group(batch, current, group["ObjectIdentifier"], members, name)
                for local in group.get("LocalNames") or []:
                    batch.node(("Base", local["ObjectIdentifier"]), {"name": local["PrincipalName"]})
        for field, rid in LOCAL_GROUPS.items():
            result = obj.get(field) or {}
            if result.get("Collected", True) and result.get("Results"):
                local_group(batch, current, f"{identifier}-{rid}", result["Results"])
        for right in obj.get("UserRights") or []:
            if right.get("Collected") and right.get("Privilege") == "SeRemoteInteractiveLogonRight":
                for member in right.get("Results") or []:
                    props["hasura"] = True
                    batch.edge(principal(member), current, "RemoteInteractiveLogonRight")
        if obj.get("IsDC") and domain_sid:
            batch.edge(current, ("Domain", domain_sid), "DCFor")

    if kind in ("Computer", "User") and domain_sid and not obj.get("IsDC"):
        if props.get("unconstraineddelegation", obj.get("UnconstrainedDelegation")):
            batch.edge(current, ("Domain", domain_sid), "CoerceToTGT")
    changes = obj.get("GPOChanges") or {}
    for computer in changes.get("AffectedComputers") or []:
        for field, rid in LOCAL_GROUPS.items():
            if changes.get(field):
                computer_id = computer["ObjectIdentifier"]
                local_group(batch, ("Computer", computer_id), f"{computer_id}-{rid}", changes[field])
    if kind == "Domain":
        trusts(batch, current, obj.get("Trusts") or [])

    if domain_sid and kind in ("RootCA", "NTAuthStore"):
        batch.edge(current, ("Domain", domain_sid), f"{kind}For")
    if kind == "EnterpriseCA":
        templates = obj.get("EnabledCertTemplates") or []
        for template in templates:
            batch.edge(("CertTemplate", template.get("ObjectIdentifier")), current, "PublishedTo")
        batch.edge(("Computer", obj.get("HostingComputer")), current, "HostsCAService")
        for field, key in {
            "IsUserSpecifiesSanEnabled": "isuserspecifiessanenabled",
            "RoleSeparationEnabled": "roleseparationenabled",
            "RPCEncryptionEnforced": "rpcencryptionenforced",
            "DisabledExtensions": "disabledextensions",
        }.items():
            result = registry.get(field) or {}
            if result.get("Collected"):
                props[key] = result.get("Data" if field == "DisabledExtensions" else "Value")
        enrollment = registry.get("EnrollmentAgentRestrictions") or {}
        if enrollment.get("Collected"):
            restrictions = enrollment.get("Restrictions") or []
            props["hasenrollmentagentrestrictions"] = bool(restrictions)
            for restriction in restrictions:
                if restriction.get("AccessType") == "AccessAllowedCallback":
                    targets = templates if restriction.get("AllTemplates") else [restriction["Template"]]
                    for target in targets:
                        batch.edge(principal(restriction["Agent"]),
                                   ("CertTemplate", target.get("ObjectIdentifier")),
                                   "DelegatedEnrollmentAgent")
    if kind == "IssuancePolicy" and obj.get("GroupLink"):
        target = principal(obj["GroupLink"])
        if target[1]:
            props["grouplinkid"] = target[1].upper()
            batch.edge(current, target, "OIDGroupLink")
    if kind == "SiteServer" and obj.get("ServerIs"):
        batch.edge(current, principal(obj["ServerIs"]), "ServerIs")
    if any(isinstance(item, dict) and item.get("Collected")
           for item in (obj.get("DCRegistryData") or {}).values()):
        warnings.add("collected DCRegistryData requires native CE import")
    batch.node(current, props, collected=True)


async def parse_json(file, name, session):
    # ijson requires raw UTF-8 without the optional BOM emitted by some collectors.
    if file.read(3) != b"\xef\xbb\xbf":
        file.seek(0)
    start = file.tell()
    meta = next(ijson.items(file, "meta", use_float=True), None)
    data_type = meta.get("type") if isinstance(meta, dict) else None
    kind = "sessions" if data_type == "sessions" else NODE_TYPES.get(data_type)
    if not kind:
        raise ValueError(f"unsupported direct import type {data_type!r} in {name}")

    file.seek(start)
    events = ijson.parse(file, use_float=True)
    for prefix, event, _ in events:
        if prefix == "data":
            if event != "start_array":
                raise ValueError(f"expected a data array in {name}")
            break
    else:
        raise ValueError(f"missing data array in {name}")

    batch = Batch()
    warnings = set()
    count = 0
    for obj in ijson.items(events, "data.item"):
        convert_object(batch, kind, obj, warnings)
        count += 1
        if count % BATCH_SIZE == 0:
            await session.execute_write(batch.write)
            batch = Batch()
    if batch.rows:
        await session.execute_write(batch.write)
    for warning in sorted(warnings):
        logging.warning("%s: %s", name, warning)
    logging.info("Processed %d %s records from %s", count, data_type, name)


async def parse_file(filename, driver):
    path = Path(filename)
    async with driver.session() as session:
        if path.suffix.lower() == ".zip":
            with ZipFile(path) as archive:
                members = [member for member in archive.infolist()
                           if not member.is_dir() and member.filename.lower().endswith(".json")]
                if not members:
                    raise ValueError(f"no JSON files in {path}")
                for member in members:
                    with archive.open(member) as file:
                        await parse_json(file, f"{path}:{member.filename}", session)
        elif path.suffix.lower() == ".json":
            with path.open("rb") as file:
                await parse_json(file, str(path), session)
        else:
            raise ValueError(f"expected a ZIP or JSON file: {path}")
