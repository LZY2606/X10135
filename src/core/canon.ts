/**
 * 确定性 JSON 序列化：对象键排序，保证同一逻辑状态总是得到同一字符串。
 * 用于状态哈希、校验和与持久化，避免键顺序影响实验回放结果。
 */
export function canonical(value: unknown): string {
  if (value === null || value === undefined) return 'null';
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new Error('非有限数值无法确定性序列化');
    return JSON.stringify(value);
  }
  if (typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) {
    return '[' + value.map((item) => canonical(item)).join(',') + ']';
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return (
    '{' +
    keys.map((key) => JSON.stringify(key) + ':' + canonical(obj[key])).join(',') +
    '}'
  );
}

/**
 * FNV-1a 64 位哈希（十六进制）。仅用于实验台的状态指纹与校验和，
 * 不用于安全场景。输入按 UTF-16 码元逐字节处理，结果确定性。
 */
export function hashString(input: string): string {
  let hash = 0xcbf29ce484222325n;
  const prime = 0x100000001b3n;
  const mask = 0xffffffffffffffffn;
  for (let i = 0; i < input.length; i++) {
    const unit = input.charCodeAt(i);
    hash ^= BigInt(unit & 0xff);
    hash = (hash * prime) & mask;
    hash ^= BigInt((unit >> 8) & 0xff);
    hash = (hash * prime) & mask;
  }
  return hash.toString(16).padStart(16, '0');
}

/** 对任意可序列化值计算确定性状态哈希。 */
export function hashState(value: unknown): string {
  return hashString(canonical(value));
}
