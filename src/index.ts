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

  // Launched by a JVM (or any parent) that keeps our stdin open: when the parent
  // goes away, even without stopping us (quit from the macOS app menu, a crash,
  // kill -9), stdin closes and we exit instead of running on as an orphan that
  // still holds the parent's output pipes.
  if (process.env.MINDOODB_BRIDGE_EXIT_WITH_PARENT === "1") {
    process.stdin.on("end", () => void shutdown("stdin closed (parent exited)"));
    process.stdin.on("error", () => void shutdown("stdin error (parent exited)"));
    process.stdin.resume();
  }
}

main().catch((err) => {
  log.error("index", "fatal", err);
  process.exit(1);
});
