import type { Connect } from "vite";
import type { IncomingMessage, ServerResponse } from "node:http";
import type { CrashPoint, ExportBundle, ProcessorVersion } from "../shared/types.js";
import { CrashInjectedError, LabEngine } from "./engine.js";
import { ensureDir } from "./storage.js";

const dataDir = process.env.LAB_DATA_DIR ?? ".lab-data";

let enginePromise: Promise<LabEngine> | null = null;

async function engine(): Promise<LabEngine> {
  if (!enginePromise) {
    ensureDir(dataDir);
    enginePromise = LabEngine.open({ dataDir });
  }
  return enginePromise;
}

async function resetEngine(): Promise<LabEngine> {
  if (enginePromise) {
    await (await enginePromise).close();
  }
  enginePromise = null;
  await LabEngine.reset(dataDir);
  return engine();
}

function send(res: ServerResponse, status: number, body: unknown): void {
  const json = JSON.stringify(body);
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(json);
}

async function readBody(req: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let raw = "";
    req.on("data", (chunk: Buffer) => {
      raw += chunk.toString("utf8");
      if (raw.length > 2_000_000) reject(new Error("请求体过大"));
    });
    req.on("end", () => {
      if (!raw) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch (err) {
        reject(err);
      }
    });
    req.on("error", reject);
  });
}

export function labApiPlugin(): {
  name: string;
  configureServer: (server: {
    middlewares: Connect.Server;
    httpServer: { close?: () => void };
  }) => void;
} {
  return {
    name: "checkpoint-lab-api",
    configureServer(server) {
      server.middlewares.use("/api/", async (req, res) => {
        try {
          const url = new URL(req.url ?? "/", "http://127.0.0.1");
          const path = url.pathname.replace(/^\/api/, "") || "/";
          const method = req.method ?? "GET";
          const lab = await engine();
          const body = method === "POST" ? ((await readBody(req)) ?? {}) : {};

          if (path === "/status" && method === "GET") {
            return send(res, 200, lab.status());
          }
          if (path === "/partitions" && method === "POST") {
            return send(res, 200, await lab.addPartition());
          }
          if (path === "/events" && method === "POST") {
            const b = body as {
              partition: number;
              key: string;
              value?: number;
              idemKey?: string;
            };
            return send(
              res,
              200,
              await lab.appendEvent({
                partition: Number(b.partition),
                key: String(b.key),
                value: Number(b.value ?? 1),
                idemKey: b.idemKey || undefined,
              }),
            );
          }
          if (path === "/sample-events" && method === "POST") {
            const b = body as { perPartition?: number };
            const count = Math.max(1, Math.min(50, Number(b.perPartition ?? 3)));
            const created: unknown[] = [];
            const status = lab.status();
            for (const part of status.partitions) {
              for (let i = 0; i < count; i++) {
                created.push(
                  await lab.appendEvent({
                    partition: part.id,
                    key: ["alpha", "beta", "gamma"][i % 3]!,
                    value: i + 1,
                  }),
                );
              }
            }
            return send(res, 200, { created: created.length });
          }
          if (path === "/step" && method === "POST") {
            const b = body as { partition?: number };
            return send(res, 200, await lab.step(b.partition));
          }
          if (path === "/batch" && method === "POST") {
            const b = body as { count?: number };
            return send(res, 200, await lab.processBatch(Number(b.count ?? 5)));
          }
          if (path === "/start" && method === "POST") return send(res, 200, await lab.start());
          if (path === "/pause" && method === "POST") return send(res, 200, await lab.pause());
          if (path === "/checkpoint" && method === "POST") {
            return send(res, 200, await lab.checkpoint());
          }
          if (path === "/arm-crash" && method === "POST") {
            const b = body as { point: CrashPoint };
            return send(res, 200, await lab.armCrash(b.point));
          }
          if (path === "/recover" && method === "POST") {
            await lab.close();
            enginePromise = LabEngine.open({ dataDir });
            return send(res, 200, (await enginePromise).status());
          }
          if (path === "/migrate" && method === "POST") {
            const b = body as { toVersion?: ProcessorVersion };
            return send(res, 200, await lab.migrate({ toVersion: b.toVersion ?? 2 }));
          }
          if (path === "/force-version" && method === "POST") {
            const b = body as { version: ProcessorVersion };
            return send(res, 200, await lab.forceVersion(Number(b.version) as ProcessorVersion));
          }
          if (path === "/config" && method === "POST") {
            const b = body as {
              checkpointEvery?: number;
              checkpointIntervalMs?: number;
            };
            return send(
              res,
              200,
              await lab.setConfig({
                checkpointEvery: b.checkpointEvery,
                checkpointIntervalMs: b.checkpointIntervalMs,
              }),
            );
          }
          if (path === "/export" && method === "GET") {
            return send(res, 200, await lab.exportBundle());
          }
          if (path === "/reset" && method === "POST") {
            const fresh = await resetEngine();
            return send(res, 200, fresh.status());
          }
          return send(res, 404, { error: `未找到: ${method} ${path}` });
        } catch (err) {
          if (err instanceof CrashInjectedError) {
            return send(res, 409, {
              crashed: true,
              point: err.point,
              message: err.message,
            });
          }
          return send(res, 500, {
            error: err instanceof Error ? err.message : String(err),
          });
        }
      });
    },
  };
}

export { dataDir };
export type { ExportBundle };
