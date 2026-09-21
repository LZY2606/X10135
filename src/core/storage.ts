/**
 * 键值存储抽象。浏览器端用 localStorage 实现持久化，测试用内存实现。
 * rename 模拟原子改名：检查点与提交记录都通过 “写 tmp + rename” 保证原子可见。
 */
export interface KVStore {
  get(key: string): string | null;
  set(key: string, value: string): void;
  delete(key: string): void;
  /** 返回排序后的键列表，可带前缀过滤。 */
  keys(prefix?: string): string[];
  /** 原子地把 from 的内容移动到 to（覆盖已存在的 to）。 */
  rename(from: string, to: string): void;
}

export class MemoryStore implements KVStore {
  private data = new Map<string, string>();

  get(key: string): string | null {
    return this.data.has(key) ? (this.data.get(key) as string) : null;
  }

  set(key: string, value: string): void {
    this.data.set(key, value);
  }

  delete(key: string): void {
    this.data.delete(key);
  }

  keys(prefix = ''): string[] {
    return [...this.data.keys()].filter((k) => k.startsWith(prefix)).sort();
  }

  rename(from: string, to: string): void {
    const value = this.data.get(from);
    if (value === undefined) throw new Error(`rename 源不存在: ${from}`);
    this.data.delete(from);
    this.data.set(to, value);
  }
}

export class LocalStorageStore implements KVStore {
  constructor(private readonly prefix: string) {}

  private full(key: string): string {
    return this.prefix + key;
  }

  get(key: string): string | null {
    return window.localStorage.getItem(this.full(key));
  }

  set(key: string, value: string): void {
    window.localStorage.setItem(this.full(key), value);
  }

  delete(key: string): void {
    window.localStorage.removeItem(this.full(key));
  }

  keys(prefix = ''): string[] {
    const result: string[] = [];
    for (let i = 0; i < window.localStorage.length; i++) {
      const key = window.localStorage.key(i);
      if (key && key.startsWith(this.prefix + prefix)) {
        result.push(key.slice(this.prefix.length));
      }
    }
    return result.sort();
  }

  rename(from: string, to: string): void {
    const value = this.get(from);
    if (value === null) throw new Error(`rename 源不存在: ${from}`);
    this.delete(from);
    this.set(to, value);
  }

  clear(): void {
    for (const key of this.keys()) this.delete(key);
  }
}
