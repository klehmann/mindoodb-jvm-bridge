#!/usr/bin/env node
import { startSupervisor } from "./supervisor";
import { log } from "./logging";

async function main(): Promise<void> {
  const handle = await startSupervisor({
    host: process.env.MINDOODB_BRIDGE_HOST,
    port: process.env.MINDOODB_BRIDGE_PORT ? Number(process.env.MINDOODB_BRIDGE_PORT) : undefined,
    handshakePath: process.env.MINDOODB_BRIDGE_HANDSHAKE,
    dataRoot: process.env.MINDOODB_BRIDGE_DATA,
  });

  // Announce readiness on stdout so a supervising JVM can await it deterministically
  // (the handshake file is written before this line).
  process.stdout.write(`${JSON.stringify({ ready: true, port: handle.port, handshakePath: handle.handshakePath })}\n`);

  const shutdown = async (signal: string): Promise<void> => {
    log.info("index", `received ${signal}, shutting down`);
    await handle.close();
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
}

main().catch((err) => {
  log.error("index", "fatal", err);
  process.exit(1);
});
