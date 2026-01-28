import express from 'express';
import cors from 'cors';
require("dotenv").config();
import { detectDirtyData } from './detection.js';
import { applyLocalRules } from './rules.js';
import { healWithAI } from './ai-healing.js';

const app = express();
const PORT = 3001;

app.use(cors());
app.use(express.json());

const inMemoryStore = {
  rawData: [],
  processedData: [],
  rulebook: [
    { id: 1, name: 'Trim Whitespace', description: 'Remove leading/trailing whitespace from all fields', source: 'Human', active: true },
    { id: 2, name: 'Lowercase Email', description: 'Convert email addresses to lowercase', source: 'Human', active: true },
    { id: 3, name: 'Default Country', description: 'Replace empty country with "Unknown"', source: 'Human', active: true },
  ],
  logs: [],
  nextId: 1,
  nextRuleId: 4,
};

function addLog(stage, message, status = 'info', data = null) {
  const log = {
    id: Date.now() + Math.random(),
    timestamp: new Date().toISOString(),
    stage,
    message,
    status,
    data,
  };
  inMemoryStore.logs.push(log);
  return log;
}

app.post('/api/ingest', async (req, res) => {
  try {
    const rawData = req.body;
    const dataId = inMemoryStore.nextId++;

    inMemoryStore.logs = [];

    addLog('ingestion', 'Raw data received', 'success', rawData);

    inMemoryStore.rawData.push({ id: dataId, ...rawData });

    const dirtyFields = detectDirtyData(rawData);
    addLog('detection', `Detected ${dirtyFields.length} dirty fields: ${dirtyFields.join(', ')}`,
           dirtyFields.length > 0 ? 'warning' : 'success', { dirtyFields });

    let cleanedData = { ...rawData };
    let appliedRules = [];

    if (dirtyFields.length > 0) {
      const rulesResult = applyLocalRules(cleanedData, inMemoryStore.rulebook);
      cleanedData = rulesResult.data;
      appliedRules = rulesResult.appliedRules;

      if (appliedRules.length > 0) {
        addLog('rules', `Applied ${appliedRules.length} local rules`, 'success', { appliedRules });
      }

      const stillDirtyFields = detectDirtyData(cleanedData);

      if (stillDirtyFields.length > 0) {
        addLog('ai-healing', `${stillDirtyFields.length} fields still dirty, invoking AI healing...`, 'info', { stillDirtyFields });

        const aiResult = await healWithAI(cleanedData, rawData, stillDirtyFields);
        cleanedData = aiResult.healedData;

        if (aiResult.newRules && aiResult.newRules.length > 0) {
          aiResult.newRules.forEach(rule => {
            const newRule = {
              id: inMemoryStore.nextRuleId++,
              name: rule.name,
              description: rule.description,
              source: 'AI Generated',
              active: true,
              createdAt: new Date().toISOString(),
            };
            inMemoryStore.rulebook.push(newRule);
            addLog('rulebook', `New rule learned: ${rule.name}`, 'success', { rule: newRule });
          });
        }

        addLog('ai-healing', 'AI healing completed', 'success', { healedFields: aiResult.healedFields });
      }
    }

    const finalCheck = detectDirtyData(cleanedData);
    const isClean = finalCheck.length === 0;

    addLog('output', isClean ? 'Data is clean!' : 'Data processed (some issues remain)',
           isClean ? 'success' : 'warning', { cleanedData });

    inMemoryStore.processedData.push({
      id: dataId,
      original: rawData,
      cleaned: cleanedData,
      dirtyFields,
      appliedRules,
      isClean,
      processedAt: new Date().toISOString(),
    });

    res.json({
      success: true,
      dataId,
      original: rawData,
      cleaned: cleanedData,
      dirtyFields,
      appliedRules,
      isClean,
      logs: inMemoryStore.logs,
      rulebook: inMemoryStore.rulebook,
    });
  } catch (error) {
    console.error('Error processing data:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/rulebook', (req, res) => {
  res.json({ rules: inMemoryStore.rulebook });
});

app.get('/api/history', (req, res) => {
  res.json({ history: inMemoryStore.processedData });
});

app.delete('/api/reset', (req, res) => {
  inMemoryStore.rawData = [];
  inMemoryStore.processedData = [];
  inMemoryStore.logs = [];
  inMemoryStore.nextId = 1;
  res.json({ success: true, message: 'Storage reset' });
});

app.listen(PORT, () => {
  console.log(`Self-Healing Pipeline API running on http://localhost:${PORT}`);
});
