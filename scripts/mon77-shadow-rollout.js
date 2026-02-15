const fs = require('node:fs');
const path = require('node:path');
const { runShadowRollout } = require('../src/shadow/rollout');

const fixturePath = path.resolve(__dirname, '../fixtures/packs/mon77-sessions.json');
const outputPath = path.resolve(__dirname, '../artifacts/mon-77/shadow-diff-report.json');

function main() {
  const sessions = JSON.parse(fs.readFileSync(fixturePath, 'utf8'));
  const report = runShadowRollout(sessions);

  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, JSON.stringify(report, null, 2));

  process.stdout.write(`Shadow diff report generated: ${outputPath}\n`);
  process.stdout.write(`Sessions processed: ${report.summary.totalSessions}\n`);
}

main();
