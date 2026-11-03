import type {
  ContentAddressedStoreFactory,
  CreateStoreResult,
  OpenStoreOptions,
} from "mindoodb";
import { BasicOnDiskContentAddressedStoreFactory } from "mindoodb/node";

/**
 * Wraps the on-disk store factory so every store this session opens is rooted
 * under the session's own `basePath`. This is the mechanism that isolates one
 * user's replica from another's, independent of how `openDB` forwards options.
 */
export class ScopedOnDiskStoreFactory implements ContentAddressedStoreFactory {
  private readonly inner = new BasicOnDiskContentAddressedStoreFactory();

  constructor(private readonly basePath: string) {}

  createStore(dbId: string, options?: OpenStoreOptions): CreateStoreResult {
    return this.inner.createStore(dbId, { ...options, basePath: this.basePath });
  }
}
