import { parentPort, workerData } from 'worker_threads';
import * as fs from 'fs/promises';
import * as path from 'path';

async function searchInPartition(baseDir: string, databaseId: string, phone: string): Promise<any> {
  try {
    const prefix = phone.substring(0, 3);
    const dataPath = path.join(baseDir, databaseId, prefix, 'data.json');
    const data = JSON.parse(await fs.readFile(dataPath, 'utf-8'));
    return data[phone] || null;
  } catch {
    return null;
  }
}

async function run() {
  const { baseDir, databaseId, phone } = workerData;
  const result = await searchInPartition(baseDir, databaseId, phone);
  parentPort?.postMessage({ databaseId, result });
}

run().catch(error => {
  parentPort?.postMessage({ error: error.message });
});
