import { rmSync } from 'node:fs';
import { mkdtempSync } from 'node:fs';
import type { ServerResponse } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Connect, ViteDevServer } from 'vite';
import { CrashSimulation, EngineError } from '../src/core/engine';
import { FileStorage } from '../src/core/storage';
import { replayExport } from '../src/core/engine';
import type { FaultPoint, ProcessorVersion } from '../src/core/types';
import { BenchService } from './bench-service';

const DEFAULT_DATA_DIR = process.env.CP_BENCH_DATA_DIR ?? join(process.cwd(), '.bench-data');

export function createBenchService(dataDir = DEFAULT_DATA_DIR): BenchService {
  return new BenchService(dataDir);
}

function readJson(req: Connect.IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8');
      resolve(raw ? JSON.parse(raw) : {});
    });
    req.on('error', reject);
  });
}

function send(res: ServerResponse, status: number, body: unknown): void {
  res.statusCode = status;
  res.setHeader('content-type', 'application/json; charset=utf-8');
  res.end(JSON.stringify(body));
}

export function benchApiPlugin(service: BenchService) {
  return {
    name: 'checkpoint-bench-api',
    configureServer(server: ViteDevServer) {
      server.middlewares.use('/api', async (req, res, next) => {
        try {
          await handle(service, req, res);
        } catch (err) {
          if (res.writableEnded) return;
          if (err instanceof CrashSimulation) {
            send(res, 409, {
              error: err.message,
              crashed: true,
              fault: err.fault,
              snapshot: service.engine.snapshot(),
            });
            return;
          }
          if (err instanceof EngineError) {
            send(res, 409, { error: err.message });
          } else {
            next(err);
          }
        }
      });
    },
  };
}

async function handle(service: BenchService, req: Connect.IncomingMessage, res: ServerResponse) {
  const url = new URL(req.url ?? '/', 'http://localhost');
  const path = url.pathname;
  const engine = service.engine;
  const body = req.method === 'POST' ? ((await readJson(req)) as Record<string, unknown>) : {};

  if (req.method === 'GET' && path === '/state') {
    return send(res, 200, { snapshot: engine.snapshot(), faults: engine.listFaults(), runConfig: service.runConfig });
  }

  if (req.method === 'POST' && path === '/partitions') {
    engine.createPartition(String(body.partition));
    return send(res, 200, { ok: true, snapshot: engine.snapshot() });
  }

  if (req.method === 'POST' && path === '/events') {
    const event = engine.appendEvent(
      String(body.partition),
      String(body.key),
      Number(body.value ?? 1),
    );
    return send(res, 200, { ok: true, event, snapshot: engine.snapshot() });
  }

  if (req.method === 'POST' && path === '/start') return send(res, 200, { snapshot: service.start() });
  if (req.method === 'POST' && path === '/pause') return send(res, 200, { snapshot: service.pause() });

  if (req.method === 'POST' && path === '/consume/one') {
    const result = engine.consumeOne(
      body.partition ? String(body.partition) : undefined,
    );
    return send(res, 200, { result, snapshot: engine.snapshot() });
  }

  if (req.method === 'POST' && path === '/consume/batch') {
    const count = Math.max(1, Math.min(1000, Number(body.count ?? 5)));
    const processed = engine.consumeBatch(
      count,
      body.partition ? String(body.partition) : undefined,
    );
    return send(res, 200, { processed, snapshot: engine.snapshot() });
  }

  if (req.method === 'POST' && path === '/redeliver') {
    engine.redeliver(String(body.partition), Number(body.offset));
    return send(res, 200, { ok: true, snapshot: engine.snapshot() });
  }

  if (req.method === 'POST' && path === '/checkpoint') {
    const name = engine.checkpoint('manual');
    return send(res, 200, { name, snapshot: engine.snapshot() });
  }

  if (req.method === 'POST' && path === '/crash/restart') {
    engine.restart();
    return send(res, 200, { ok: true, snapshot: engine.snapshot() });
  }

  if (req.method === 'POST' && path === '/faults/arm') {
    service.armFault(body.fault as FaultPoint);
    return send(res, 200, { ok: true, faults: engine.listFaults() });
  }

  if (req.method === 'POST' && path === '/faults/disarm') {
    service.disarmFault(body.fault as FaultPoint);
    return send(res, 200, { ok: true, faults: engine.listFaults() });
  }

  if (req.method === 'POST' && path === '/run-config') {
    return send(res, 200, {
      runConfig: service.updateRunConfig({
        intervalMs: body.intervalMs === undefined ? undefined : Number(body.intervalMs),
        batchSize: body.batchSize === undefined ? undefined : Number(body.batchSize),
        checkpointEvery: body.checkpointEvery === undefined ? undefined : Number(body.checkpointEvery),
        autoCheckpoint: body.autoCheckpoint === undefined ? undefined : Boolean(body.autoCheckpoint),
      }),
    });
  }

  if (req.method === 'POST' && path === '/processor/deploy') {
    engine.deployVersion(Number(body.version) as ProcessorVersion);
    return send(res, 200, { ok: true, snapshot: engine.snapshot() });
  }

  if (req.method === 'POST' && path === '/processor/migrate') {
    const record = engine.migrate(
      body.version === undefined ? undefined : (Number(body.version) as ProcessorVersion),
    );
    return send(res, 200, { record, snapshot: engine.snapshot() });
  }

  if (req.method === 'GET' && path === '/export') {
    return send(res, 200, engine.exportExperiment());
  }

  if (req.method === 'POST' && path === '/replay') {
    const dir = mkdtempSync(join(tmpdir(), 'cp-bench-replay-'));
    try {
      const result = replayExport(
        new FileStorage(dir),
        body as unknown as Parameters<typeof replayExport>[1],
      );
      return send(res, 200, result);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }

  if (req.method === 'POST' && path === '/reset') {
    return send(res, 200, { ok: true, snapshot: service.reset() });
  }

  res.statusCode = 404;
  res.setHeader('content-type', 'application/json; charset=utf-8');
  res.end(JSON.stringify({ error: `unknown route ${req.method} ${path}` }));
}

export { CrashSimulation };
