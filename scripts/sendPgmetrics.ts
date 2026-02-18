import 'dotenv/config'
import { spawnSync } from 'child_process'
import { readFileSync, unlinkSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'

// --- Test mode: no API requests, just validate env and run pgmetrics once then exit ---
const TEST_MODE =
  process.env.TEST_MODE === '1' ||
  process.env.TEST_MODE === 'true' ||
  process.argv.includes('--test')

// --- Config from .env (and argv for server_id) ---
const SERVER_ID = process.env.SERVER_ID ?? process.argv[2]
const API_BASE_URL = process.env.API_BASE_URL ?? 'https://metrics.ggpanel.site'

const PGPASSWORD = process.env.PG_PASSWORD ?? 'postgres'
const PGUSER = process.env.PG_USER ?? 'postgres'
const PGHOST = process.env.PG_HOST ?? '/var/run/postgresql'
const PGPORT = process.env.PG_PORT ?? '5432'

/** "all" = --all-dbs; else comma-separated list e.g. "db1,db2,db3" */
const PG_DATABASES = (process.env.PG_DATABASES ?? 'all').trim().toLowerCase()
const ALL_DBS = PG_DATABASES === 'all' || PG_DATABASES === ''

const PG_TIMEOUT = process.env.PG_TIMEOUT ?? '60'
const PG_OMIT = process.env.PG_OMIT ?? 'log'
const PG_SQL_LENGTH = process.env.PG_SQL_LENGTH ?? '10000'
const PG_STATEMENTS_LIMIT = process.env.PG_STATEMENTS_LIMIT ?? '10000'

if (!TEST_MODE && !SERVER_ID) {
  console.error('❌ Set SERVER_ID in .env or run: bun run sendPgmetrics.ts <server_id>')
  process.exit(1)
}
const serverId: string = SERVER_ID ?? 'test'

if (!PGPASSWORD && PGHOST !== '/var/run/postgresql') {
  console.error('❌ Set PGPASSWORD in .env when using remote host')
  process.exit(1)
}

const API_URL = `${API_BASE_URL}/pgmetrics`

if (TEST_MODE) {
  console.log('🧪 TEST MODE — no outbound API requests')
  console.log('   Validating env and running pgmetrics once, then exiting.')
  console.log('-----------------------------------')
  console.log('Env:')
  console.log('  PGUSER:', PGUSER)
  console.log('  PGHOST:', PGHOST)
  console.log('  PGPORT:', PGPORT)
  console.log('  PG_DATABASES:', ALL_DBS ? 'all (--all-dbs)' : PG_DATABASES)
  console.log('  PGPASSWORD:', PGPASSWORD ? '*** set ***' : '(not set)')
  console.log('  PG_TIMEOUT:', PG_TIMEOUT, 'PG_OMIT:', PG_OMIT)
  console.log('  PG_SQL_LENGTH:', PG_SQL_LENGTH, 'PG_STATEMENTS_LIMIT:', PG_STATEMENTS_LIMIT)
  console.log('-----------------------------------')
  const tmpFile = join(tmpdir(), `pgmetrics_test_${Date.now()}.json`)
  try {
    runPgmetrics(tmpFile)
    const raw = readFileSync(tmpFile, 'utf-8')
    const size = Buffer.byteLength(raw)
    const sizeMB = (size / 1024 / 1024).toFixed(2)
    console.log('✅ pgmetrics ran successfully')
    console.log('   Output file:', tmpFile)
    console.log('   Size:', sizeMB, 'MB')
    unlinkSync(tmpFile)
    console.log('   Temp file removed.')
  } catch (e) {
    try {
      unlinkSync(tmpFile)
    } catch {}
    console.error('❌ pgmetrics failed:', e instanceof Error ? e.message : e)
    process.exit(1)
  }
  console.log('✅ Test complete. Exiting.')
  process.exit(0)
}

console.log('▶ Starting pgmetrics sender...')
console.log('🆔 Server ID:', serverId)
console.log('📡 Target:', API_URL)
console.log('🐘 PG:', `${PGUSER}@${PGHOST}:${PGPORT}`, ALL_DBS ? '(all-dbs)' : `dbs: ${PG_DATABASES}`)
console.log('-----------------------------------')

async function shouldSend(): Promise<boolean> {
  try {
    const response = await fetch(`${API_BASE_URL}/request-upload`, {
      headers: { 'x-server-id': serverId },
    })
    if (response.status === 403) {
      const data = await response.json().catch(() => ({ error: 'Forbidden' }))
      const reason = (data as { reason?: string }).reason ? ` (${(data as { reason?: string }).reason})` : ''
      console.log(`❌ Pre-check failed: ${(data as { error?: string }).error ?? 'Forbidden'}${reason}`)
      console.log('⏸️  Upload blocked - user account issue. Waiting 30s before retry...')
      await new Promise((r) => setTimeout(r, 30000))
      return false
    }
    if (response.status === 404) {
      const data = await response.json().catch(() => ({ error: 'User not found' }))
      console.log(`❌ Pre-check failed: ${(data as { error?: string }).error ?? 'User not found'}`)
      console.log('⏸️  Upload blocked - user not found. Waiting 30s before retry...')
      await new Promise((r) => setTimeout(r, 30000))
      return false
    }
    if (response.status === 429) {
      const data = await response.json().catch(() => ({})) as { nextCollectionAt?: number }
      const waitTime = data.nextCollectionAt ? Math.max(0, data.nextCollectionAt - Date.now()) : 2500
      console.log(`⛔ Pre-check failed: Rate limit exceeded, waiting ... ${Math.ceil(waitTime / 1000)}s`)
      await new Promise((r) => setTimeout(r, waitTime))
      return false
    }
    if (!response.ok) {
      const data = await response.json().catch(() => ({ error: `HTTP ${response.status}` }))
      console.log(`⛔ Pre-check failed: ${(data as { error?: string }).error ?? `HTTP ${response.status}`}, waiting ... 2.5s`)
      await new Promise((r) => setTimeout(r, 2500))
      return false
    }
    return true
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : 'Unknown error'
    let waitTime = 2500
    try {
      const parsed = JSON.parse(errorMessage) as { nextCollectionAt?: number; error?: string }
      if (parsed?.nextCollectionAt) {
        waitTime = Math.max(0, parsed.nextCollectionAt - Date.now())
        console.log(`⛔ Pre-check failed: ${parsed.error ?? 'Rate limit exceeded'}, waiting ... ${Math.ceil(waitTime / 1000)}s`)
      } else {
        console.log(`⛔ Pre-check failed: ${errorMessage}, waiting ... 2.5s`)
      }
    } catch {
      console.log(`⛔ Pre-check failed: ${errorMessage}, waiting ... 2.5s`)
    }
    await new Promise((r) => setTimeout(r, waitTime))
    return false
  }
}

async function getUploadUrl(size: number): Promise<string | false> {
  try {
    const response = await fetch(`${API_BASE_URL}/request-upload-url`, {
      headers: {
        'x-server-id': serverId,
        'x-content-length': size.toString(),
      },
    })
    if (response.status === 403) {
      const data = await response.json().catch(() => ({ error: 'Forbidden' }))
      const reason = (data as { reason?: string }).reason ? ` (${(data as { reason?: string }).reason})` : ''
      console.log(`❌ Get upload url failed: ${(data as { error?: string }).error ?? 'Forbidden'}${reason}`)
      return false
    }
    if (response.status === 404) {
      const data = await response.json().catch(() => ({ error: 'User not found' }))
      console.log(`❌ Get upload url failed: ${(data as { error?: string }).error ?? 'User not found'}`)
      return false
    }
    if (!response.ok) {
      const data = await response.json().catch(() => ({ error: `HTTP ${response.status}` }))
      console.log(`⛔ Get upload url failed: ${(data as { error?: string }).error ?? `HTTP ${response.status}`}`)
      return false
    }
    const data = (await response.json()) as { url?: string }
    return data.url ?? false
  } catch (err) {
    console.log('⛔ Get upload url failed:', err instanceof Error ? err.message : 'Unknown error')
    return false
  }
}

async function uploadFile(url: string, body: string): Promise<boolean> {
  try {
    const response = await fetch(url, {
      method: 'PUT',
      body,
      headers: { 'Content-Type': 'application/json' },
    })
    return response.ok
  } catch (err) {
    console.log('⛔ Upload file failed:', err instanceof Error ? err.message : 'Unknown error')
    return false
  }
}

async function finishedUpload(url: string): Promise<boolean> {
  try {
    const pathname = new URL(url).pathname
    const key = pathname.substring(1).split('.')[0] ?? ''
    const bucket = url.split('/')[2]?.split('.')[0] ?? ''
    const response = await fetch(`${API_BASE_URL}/finished-upload`, {
      headers: {
        'x-server-id': serverId,
        'x-key': key,
        'x-bucket': bucket,
      },
    })
    return response.ok
  } catch (err) {
    console.log('⛔ Finished upload failed:', err instanceof Error ? err.message : 'Unknown error')
    return false
  }
}

function runPgmetrics(tmpFile: string): void {
  const args: string[] = [
    '-h', PGHOST,
    '-p', PGPORT,
    '-U', PGUSER,
    '-w',
    '--timeout', PG_TIMEOUT,
    '--omit', PG_OMIT,
    '--sql-length', PG_SQL_LENGTH,
    '--statements-limit', PG_STATEMENTS_LIMIT,
    '-f', 'json',
    '-o', tmpFile,
  ]
  if (ALL_DBS) {
    args.splice(args.indexOf('-w') + 1, 0, '--all-dbs')
  } else {
    const dbs = PG_DATABASES.split(',').map((d) => d.trim()).filter(Boolean)
    args.push(...dbs)
  }

  const env: NodeJS.ProcessEnv = { ...process.env }
  env.PGUSER = PGUSER
  env.PGHOST = PGHOST
  env.PGPORT = PGPORT
  if (PGPASSWORD) env.PGPASSWORD = PGPASSWORD

  const r = spawnSync('pgmetrics', args, {
    env,
    stdio: ['pipe', 'pipe', 'pipe'],
    encoding: 'utf-8',
  })
  if (r.status !== 0) {
    const stderr = r.stderr?.trim() || r.error?.message || 'pgmetrics failed'
    throw new Error(stderr)
  }
}

async function loop(): Promise<void> {
  while (true) {
    try {
      const ok = await shouldSend()
      if (!ok) {
        await new Promise((r) => setTimeout(r, 250))
        continue
      }

      console.log(
        '🚀 Backend said YES — collecting pgmetrics... sending for server_id:',
        serverId + ' at ' + new Date().toLocaleString()
      )

      const tmpFile = join(tmpdir(), `pgmetrics_${serverId}_${Date.now()}.json`)
      try {
        runPgmetrics(tmpFile)
      } catch (e) {
        try {
          unlinkSync(tmpFile)
        } catch {}
        throw e
      }

      const raw = readFileSync(tmpFile, 'utf-8')
      unlinkSync(tmpFile)
      const size = Buffer.byteLength(raw)
      const sizeMB = (size / 1024 / 1024).toFixed(2)
      console.log('Size:', sizeMB, 'MB for server_id:', serverId)

      const url = await getUploadUrl(size)
      if (!url) {
        await new Promise((r) => setTimeout(r, 1000))
        continue
      }
      console.log('URL:', url)

      const uploaded = await uploadFile(url, raw)
      console.log('Uploaded:', uploaded, 'for server_id:', serverId)

      const finished = await finishedUpload(url)
      console.log('Finished:', finished, 'for server_id:', serverId)

      await new Promise((r) => setTimeout(r, 1000))
    } catch (err) {
      console.log('⛔ Error:', err instanceof Error ? err.message : err)
      await new Promise((r) => setTimeout(r, 1000))
    }
  }
}

loop()
