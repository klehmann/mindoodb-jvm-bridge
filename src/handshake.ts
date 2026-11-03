import { randomBytes } from "node:crypto";
import { writeFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";

/**
 * The bootstrap handshake contract between the JVM and the sidecar.
 *
 * The supervisor binds a random loopback port and writes this file (mode 600)
 * so the JVM can discover where to connect and which bootstrap token to send.
 * The bootstrap token authorizes opening the WebSocket connection; a
 * per-session token (returned by `session.open`) authorizes individual frames.
 */
export interface HandshakeInfo {
  version: 1;
  host: string;
  port: number;
  bootstrapToken: string;
  pid: number;
}

export function generateBootstrapToken(): string {
  return randomBytes(32).toString("base64url");
}

export function writeHandshakeFile(path: string, info: HandshakeInfo): void {
  mkdirSync(dirname(path), { recursive: true });
  // mode 0o600: owner read/write only — the token is a secret.
  writeFileSync(path, JSON.stringify(info, null, 2), { encoding: "utf-8", mode: 0o600 });
}

export function defaultHandshakePath(): string {
  return (
    process.env.MINDOODB_BRIDGE_HANDSHAKE ??
    `${process.env.MINDOODB_BRIDGE_DATA ?? "./.mindoodb-bridge-data"}/handshake.json`
  );
}
