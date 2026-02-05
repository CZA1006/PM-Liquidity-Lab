"use client";

const KEY = "pmliq.watchlist.v1";

export function loadWatchlist(): string[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = window.localStorage.getItem(KEY);
    const arr = raw ? (JSON.parse(raw) as unknown) : [];
    const xs = Array.isArray(arr) ? arr.map(String).filter(Boolean) : [];
    return Array.from(new Set(xs));
  } catch {
    return [];
  }
}

function saveWatchlist(xs: string[]) {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(KEY, JSON.stringify(xs));
}

export function addToWatchlist(slug: string): string[] {
  const s = String(slug || "").trim();
  if (!s) return loadWatchlist();
  const xs = loadWatchlist();
  if (xs.includes(s)) return xs;
  const next = xs.concat([s]);
  saveWatchlist(next);
  return next;
}

export function removeFromWatchlist(slug: string): string[] {
  const s = String(slug || "").trim();
  const xs = loadWatchlist();
  const next = xs.filter((x) => x !== s);
  saveWatchlist(next);
  return next;
}
