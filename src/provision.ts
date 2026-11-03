#!/usr/bin/env node
/**
 * Provisioning CLI.
 *
 * Creates a tenant + app user and writes a password-locked identity bundle to
 * the per-user environment directory, so a later `session.open` can reopen it.
 * Intended for first-time setup and for integration tests; production
 * deployments may instead join an existing tenant and persist the bundle the
 * same way.
 *
 * Usage:
 *   mindoodb-bridge-provision \
 *     --data ./data --tenant acme --admin-name "cn=admin" --admin-password pw \
 *     --user-name "cn=alice" --user-password pw --db notes [--db tasks] \
 *     [--server-url https://sync.example.com]
 */
import { provisionTenant } from "./provisioning";

interface Args {
  data: string;
  tenant: string;
  adminName: string;
  adminPassword: string;
  userName: string;
  userPassword: string;
  databases: string[];
  serverUrl?: string;
}

function parseArgs(argv: string[]): Args {
  const map = new Map<string, string[]>();
  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (token.startsWith("--")) {
      const key = token.slice(2);
      const value = argv[i + 1];
      if (value === undefined || value.startsWith("--")) {
        map.set(key, [...(map.get(key) ?? []), "true"]);
      } else {
        map.set(key, [...(map.get(key) ?? []), value]);
        i++;
      }
    }
  }
  const one = (key: string, fallback?: string): string => {
    const v = map.get(key)?.[0] ?? fallback;
    if (v === undefined) throw new Error(`Missing required --${key}`);
    return v;
  };
  return {
    data: one("data", "./.mindoodb-bridge-data"),
    tenant: one("tenant"),
    adminName: one("admin-name"),
    adminPassword: one("admin-password"),
    userName: one("user-name"),
    userPassword: one("user-password"),
    databases: map.get("db") ?? [],
    serverUrl: map.get("server-url")?.[0],
  };
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));

  const result = await provisionTenant({
    dataRoot: args.data,
    tenantId: args.tenant,
    adminName: args.adminName,
    adminPassword: args.adminPassword,
    userName: args.userName,
    userPassword: args.userPassword,
    databases: args.databases,
    serverUrl: args.serverUrl,
  });

  process.stdout.write(`${JSON.stringify({ ok: true, ...result })}\n`);
}

main().catch((err) => {
  process.stderr.write(`${String((err as Error)?.stack ?? err)}\n`);
  process.exit(1);
});
