import {
  existsSync,
  mkdirSync,
  openSync,
  closeSync,
  readFileSync,
  writeFileSync,
  writeSync,
  readdirSync,
  renameSync,
  fsyncSync,
  rmSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { createHash } from "node:crypto";

export function ensureDir(path: string): void {
  mkdirSync(path, { recursive: true });
}

export function pathExists(path: string): boolean {
  return existsSync(path);
}

export function fsyncDir(path: string): void {
  const fd = openSync(path, "r");
  try {
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}

export function sha256(text: string): string {
  return createHash("sha256").update(text).digest("hex");
}

export { writeFileSync, rmSync };

export function writeJson(path: string, value: unknown): void {
  ensureDir(dirname(path));
  writeFileSync(path, JSON.stringify(value, null, 2));
}

export function readJson<T>(path: string): T {
  return JSON.parse(readFileSync(path, "utf8")) as T;
}

export function readJsonLax<T>(path: string): T | null {
  if (!existsSync(path)) return null;
  const raw = readFileSync(path, "utf8");
  if (!raw.trim()) return null;
  return JSON.parse(raw) as T;
}

export function appendJsonl(path: string, value: unknown): void {
  const fd = openSync(path, "a");
  try {
    const buf = Buffer.from(JSON.stringify(value) + "\n");
    let off = 0;
    while (off < buf.length) off += writeSync(fd, buf, off, buf.length - off, null);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}

export function readJsonl<T>(path: string): T[] {
  if (!existsSync(path)) return [];
  const raw = readFileSync(path, "utf8");
  const out: T[] = [];
  for (const line of raw.split("\n")) {
    const trimmed = line.trim();
    if (trimmed) out.push(JSON.parse(trimmed) as T);
  }
  return out;
}

export interface JsonlReadOutcome<T> {
  records: T[];
  hadTrailingGarbage: boolean;
}

export function readJsonlTolerant<T>(path: string): JsonlReadOutcome<T> {
  if (!existsSync(path)) return { records: [], hadTrailingGarbage: false };
  const raw = readFileSync(path, "utf8");
  const records: T[] = [];
  let hadTrailingGarbage = false;
  for (const line of raw.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      records.push(JSON.parse(trimmed) as T);
    } catch {
      hadTrailingGarbage = true;
    }
  }
  return { records, hadTrailingGarbage };
}

export function listFiles(dir: string): string[] {
  if (!existsSync(dir)) return [];
  return readdirSync(dir);
}

export function renameAtomic(src: string, dst: string): void {
  renameSync(src, dst);
}

export function removeFile(path: string): void {
  if (existsSync(path)) rmSync(path, { force: true });
}

export function joinPath(...parts: string[]): string {
  return join(...parts);
}
