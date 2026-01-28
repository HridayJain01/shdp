const { loadRules, saveRule } = require("../rules/ruleStore");
const { generateCleaningRule } = require("../ai/geminiService");

async function healData(record) {
  const rules = loadRules();

  // 1️⃣ Try local rules first
  for (let rule of rules) {
    if (record[rule.field] === null || record[rule.field] === "") {
      record[rule.field] = "FIXED_BY_RULE";
      return { record, healedBy: "local-rule" };
    }
  }

  // 2️⃣ No rule worked → ask Gemini
  const newRule = await generateCleaningRule(record);

  // 3️⃣ Save new rule (self-healing moment)
  saveRule(newRule);

  // 4️⃣ Apply new rule
  if (record[newRule.field] == null) {
    record[newRule.field] = "FIXED_BY_GEMINI";
  }

  return { record, healedBy: "gemini" };
}

module.exports = { healData };
