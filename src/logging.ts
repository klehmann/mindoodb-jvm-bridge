/** Minimal stderr logger so stdout stays clean for the provision CLI JSON output. */
const enabled = process.env.MINDOODB_BRIDGE_LOG !== "0";

function ts(): string {
  return new Date().toISOString();
}

export const log = {
  info(scope: string, ...args: unknown[]): void {
    if (enabled) console.error(`[${ts()}] [${scope}]`, ...args);
  },
  warn(scope: string, ...args: unknown[]): void {
    if (enabled) console.error(`[${ts()}] [${scope}] WARN`, ...args);
  },
  error(scope: string, ...args: unknown[]): void {
    console.error(`[${ts()}] [${scope}] ERROR`, ...args);
  },
};
