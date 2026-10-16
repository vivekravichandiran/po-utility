#!/usr/bin/env python3
"""Create the OAuth service principal the utility uses for `workspace=remote` rows.

In <profile>'s workspace this:
  1. creates a service principal,
  2. grants it ALL PRIVILEGES + MANAGE on the given catalogs and CAN USE on one SQL warehouse,
  3. creates an OAuth secret for it,
  4. stores that secret in a (new) secret scope. The secret value is never printed or written anywhere
     else; Databricks shows it only once, at creation.

Usage: integration/setup_remote_sp.py <profile> <warehouse_id> <catalog> [<catalog> ...]
         [--name po-control-remote-sp] [--scope po-remote] [--key remote-client-secret]

Prints the values for the wrapper's remote_* widgets (client id, scope, key), never the secret.
"""
import argparse
import sys
import warnings

warnings.filterwarnings("ignore")

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.errors import NotFound  # noqa: E402
from databricks.sdk.service.catalog import PermissionsChange, Privilege, SecurableType  # noqa: E402
from databricks.sdk.service.iam import AccessControlRequest, ComplexValue, PermissionLevel  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("profile")
    ap.add_argument("warehouse_id")
    ap.add_argument("catalogs", nargs="+")
    ap.add_argument("--name", default="po-control-remote-sp")
    ap.add_argument("--scope", default="po-remote")
    ap.add_argument("--key", default="remote-client-secret")
    a = ap.parse_args()

    w = WorkspaceClient(profile=a.profile)
    print("workspace:", w.config.host)

    # Fail before creating anything if the scope is taken (never leave an orphaned secret).
    if any(s.name == a.scope for s in w.secrets.list_scopes()):
        sys.exit("secret scope %r already exists; pick another with --scope" % a.scope)
    if any(sp.display_name == a.name for sp in w.service_principals.list(filter='displayName eq "%s"' % a.name)):
        sys.exit("service principal %r already exists; pick another with --name" % a.name)
    for c in a.catalogs:
        w.catalogs.get(c)  # raises if missing
    w.warehouses.get(a.warehouse_id)

    sp = w.service_principals.create(
        display_name=a.name, active=True,
        entitlements=[ComplexValue(value="workspace-access"), ComplexValue(value="databricks-sql-access")],
    )
    client_id = sp.application_id
    print("service principal: id=%s name=%s client_id=%s" % (sp.id, sp.display_name, client_id))

    for c in a.catalogs:
        w.grants.update(
            securable_type=SecurableType.CATALOG.value, full_name=c,
            changes=[PermissionsChange(principal=client_id, add=[Privilege.ALL_PRIVILEGES, Privilege.MANAGE])],
        )
        print("granted ALL PRIVILEGES + MANAGE on catalog", c)

    w.permissions.update(
        "sql/warehouses", a.warehouse_id,
        access_control_list=[AccessControlRequest(service_principal_name=client_id,
                                                  permission_level=PermissionLevel.CAN_USE)],
    )
    print("granted CAN USE on warehouse", a.warehouse_id)

    secret = w.service_principal_secrets_proxy.create(service_principal_id=str(sp.id))
    w.secrets.create_scope(a.scope)
    w.secrets.put_secret(a.scope, a.key, string_value=secret.secret)
    print("OAuth secret created (expires %s) and stored in scope=%r key=%r" % (secret.expire_time, a.scope, a.key))
    # The only copy of the value lives in the scope; drop ours.
    del secret

    print("\nWrapper widgets:")
    print("  remote_workspace_url  =", w.config.host.rstrip("/"))
    print("  remote_client_id      =", client_id)
    print("  remote_secret_scope   =", a.scope)
    print("  remote_secret_key     =", a.key)
    print("  remote_warehouse_id   =", a.warehouse_id)


if __name__ == "__main__":
    sys.exit(main())
