import { SelectedOnly } from "mindoodb";
import { BridgeError, ErrorCodes } from "../protocol";
import { projectViewEntry, type ViewEntryDTO } from "./projections";
import type { EphemeralView } from "./dispatcher";

/** A core VirtualViewNavigator (structurally typed to avoid deep generics). */
export interface CoreNavigator {
  getCurrentEntry(): unknown;
  gotoFirst(): boolean;
  gotoLast(): boolean;
  gotoNext(): boolean;
  gotoPrev(): boolean;
  gotoParent(): boolean;
  gotoFirstChild(): boolean;
  expandAll(): unknown;
  collapseAll(): unknown;
  expandToLevel(level: number): unknown;
  gotoNextSibling(): boolean;
  gotoPrevSibling(): boolean;
  gotoLastChild(): boolean;
  gotoPos(posStr: string): boolean;
  expandPos(posStr: string): unknown;
  collapsePos(posStr: string): unknown;
  select(origin: string, docId: string, selectParentCategories?: boolean): unknown;
  deselect(origin: string, docId: string): unknown;
  selectAllEntries(): unknown;
  deselectAllEntries(): unknown;
  isSelected(origin: string, docId: string): boolean;
  expand(origin: string, docId: string): unknown;
  collapse(origin: string, docId: string): unknown;
  isExpanded(entry: unknown): boolean;
  entriesForward(selectedOnly?: unknown): AsyncGenerator<unknown>;
  entriesBackward(selectedOnly?: unknown): AsyncGenerator<unknown>;
}

interface NavigatorEntry {
  navigator: CoreNavigator;
  dbId: string;
  /** The ephemeral view behind the navigator; refreshed on demand and disposed with it. */
  view?: EphemeralView;
  /** Live-update unsubscribe (db change listener) if a subscription is active. */
  unsubscribe?: () => void;
}

/** Owns the open VirtualViewNavigator sessions for one user session. */
export class NavigatorRegistry {
  private nextId = 1;
  private readonly navigators = new Map<string, NavigatorEntry>();

  register(navigator: CoreNavigator, dbId: string, view?: EphemeralView): string {
    const id = `nav-${this.nextId++}`;
    this.navigators.set(id, { navigator, dbId, view });
    return id;
  }

  /** Apply document changes since the last update to the view. */
  async refresh(id: string): Promise<void> {
    await this.require(id).view?.update();
  }

  private require(id: string): NavigatorEntry {
    const entry = this.navigators.get(id);
    if (!entry) throw new BridgeError(ErrorCodes.NotFound, `Unknown navigator: ${id}`);
    return entry;
  }

  get(id: string): NavigatorEntry {
    return this.require(id);
  }

  setUnsubscribe(id: string, unsubscribe: () => void): void {
    const entry = this.require(id);
    entry.unsubscribe?.();
    entry.unsubscribe = unsubscribe;
  }

  clearUnsubscribe(id: string): void {
    const entry = this.navigators.get(id);
    entry?.unsubscribe?.();
    if (entry) entry.unsubscribe = undefined;
  }

  dispose(id: string): void {
    const entry = this.navigators.get(id);
    if (!entry) return;
    entry.unsubscribe?.();
    entry.view?.dispose?.();
    this.navigators.delete(id);
  }

  disposeAll(): void {
    for (const id of [...this.navigators.keys()]) this.dispose(id);
  }

  private projectCurrent(nav: CoreNavigator): ViewEntryDTO | null {
    const current = nav.getCurrentEntry();
    if (!current) return null;
    return projectViewEntry(current as never, nav.isExpanded(current));
  }

  /** Dispatch a curated navigator operation and return a JSON-safe result. */
  async call(id: string, method: string, args: unknown[]): Promise<unknown> {
    const nav = this.require(id).navigator;
    switch (method) {
      case "getCurrentEntry":
        return this.projectCurrent(nav);
      case "gotoFirst":
        return nav.gotoFirst() ? this.projectCurrent(nav) : null;
      case "gotoLast":
        return nav.gotoLast() ? this.projectCurrent(nav) : null;
      case "gotoNext":
        return nav.gotoNext() ? this.projectCurrent(nav) : null;
      case "gotoPrev":
        return nav.gotoPrev() ? this.projectCurrent(nav) : null;
      case "gotoParent":
        return nav.gotoParent() ? this.projectCurrent(nav) : null;
      case "gotoFirstChild":
        return nav.gotoFirstChild() ? this.projectCurrent(nav) : null;
      case "expandAll":
        nav.expandAll();
        return { ok: true };
      case "collapseAll":
        nav.collapseAll();
        return { ok: true };
      case "expandToLevel":
        nav.expandToLevel(Number(args[0]));
        return { ok: true };
      case "expand":
        nav.expand(String(args[0]), String(args[1]));
        return { ok: true };
      case "collapse":
        nav.collapse(String(args[0]), String(args[1]));
        return { ok: true };
      case "gotoNextSibling":
        return nav.gotoNextSibling() ? this.projectCurrent(nav) : null;
      case "gotoPrevSibling":
        return nav.gotoPrevSibling() ? this.projectCurrent(nav) : null;
      case "gotoLastChild":
        return nav.gotoLastChild() ? this.projectCurrent(nav) : null;
      case "gotoPos":
        return nav.gotoPos(String(args[0])) ? this.projectCurrent(nav) : null;
      case "expandPos":
        nav.expandPos(String(args[0]));
        return { ok: true };
      case "collapsePos":
        nav.collapsePos(String(args[0]));
        return { ok: true };
      case "select":
        nav.select(String(args[0]), String(args[1]), Boolean(args[2]));
        return { ok: true };
      case "deselect":
        nav.deselect(String(args[0]), String(args[1]));
        return { ok: true };
      case "selectAll":
        nav.selectAllEntries();
        return { ok: true };
      case "deselectAll":
        nav.deselectAllEntries();
        return { ok: true };
      case "isSelected":
        return nav.isSelected(String(args[0]), String(args[1]));
      case "refresh":
        await this.refresh(id);
        return { ok: true };
      case "readSelected":
        return this.read(nav, "entriesForward", Number(args[0] ?? 100), Number(args[1] ?? 0), "selected");
      case "readForward":
        return this.read(nav, "entriesForward", Number(args[0] ?? 100), Number(args[1] ?? 0));
      case "readBackward":
        return this.read(nav, "entriesBackward", Number(args[0] ?? 100), Number(args[1] ?? 0));
      default:
        throw new BridgeError(ErrorCodes.BadRequest, `Unsupported navigator method: ${method}`);
    }
  }

  private async read(
    nav: CoreNavigator,
    generator: "entriesForward" | "entriesBackward",
    limit: number,
    offset: number,
    selectedOnly?: "selected",
  ): Promise<ViewEntryDTO[]> {
    const out: ViewEntryDTO[] = [];
    let index = 0;
    for await (const entry of nav[generator](selectedOnly ? SelectedOnly.YES : undefined)) {
      if (index++ < offset) continue;
      out.push(projectViewEntry(entry as never, nav.isExpanded(entry)));
      if (out.length >= limit) break;
    }
    return out;
  }
}
