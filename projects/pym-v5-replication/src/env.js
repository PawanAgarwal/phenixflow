const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { PROJECT_ROOT } = require('./config');

function parseEnvFileLine(rawLine) {
  const line = String(rawLine || '').trim();
  if (!line || line.startsWith('#')) return null;
  const splitIndex = line.indexOf('=');
  if (splitIndex <= 0) return null;
  const key = line.slice(0, splitIndex).replace(/^export\s+/, '').trim();
  let value = line.slice(splitIndex + 1).trim();
  if (
    (value.startsWith('"') && value.endsWith('"'))
    || (value.startsWith("'") && value.endsWith("'"))
  ) {
    value = value.slice(1, -1);
  }
  return key ? { key, value } : null;
}

function loadDotEnvIfExists(envPath, env = process.env) {
  if (!envPath || !fs.existsSync(envPath)) return { loaded: false, keys: [] };
  const keys = [];
  fs.readFileSync(envPath, 'utf8').split(/\r?\n/).forEach((line) => {
    const parsed = parseEnvFileLine(line);
    if (!parsed) return;
    if (env[parsed.key] === undefined) env[parsed.key] = parsed.value;
    keys.push(parsed.key);
  });
  return { loaded: true, keys };
}

function loadMassiveEnv(env = process.env) {
  const envPaths = [
    path.join(PROJECT_ROOT, '..', '..', '.env.local'),
    env.MASSIVE_ENV_FILE,
    path.join(os.homedir(), 'config', 'massive', '.env.local'),
  ].filter(Boolean).map((envPath) => path.resolve(envPath));
  return envPaths.map((envPath) => ({
    envPath,
    ...loadDotEnvIfExists(envPath, env),
  }));
}

module.exports = {
  parseEnvFileLine,
  loadDotEnvIfExists,
  loadMassiveEnv,
};
