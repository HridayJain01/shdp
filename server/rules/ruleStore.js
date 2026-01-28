const fs = require("fs");
const path = require("path");

const rulesPath = path.join(__dirname, "rules.json");

function loadRules() {
  return JSON.parse(fs.readFileSync(rulesPath, "utf8"));
}

function saveRule(rule) {
  const rules = loadRules();
  rules.push(rule);
  fs.writeFileSync(rulesPath, JSON.stringify(rules, null, 2));
}

module.exports = { loadRules, saveRule };
