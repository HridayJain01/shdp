const { GoogleGenerativeAI } = require("@google/generative-ai");

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

async function generateCleaningRule(dirtyRecord) {
  const model = genAI.getGenerativeModel({ model: "gemini-pro" });

  const prompt = `
You are a data quality engine.

Given this dirty data record:
${JSON.stringify(dirtyRecord, null, 2)}

Generate a JSON rule to clean it.
Rules format:
{
  "field": "field_name",
  "condition": "description",
  "action": "how to fix"
}

Return ONLY valid JSON.
`;

  const result = await model.generateContent(prompt);
  const response = result.response.text();

  return JSON.parse(response);
}

module.exports = { generateCleaningRule };
