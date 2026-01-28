import { GoogleGenerativeAI } from '@google/generative-ai';

// Prefer an explicit API key if provided in the environment (e.g. GOOGLE_API_KEY).
// This lets developers use a simple API key instead of ADC service account files.
const client = new GoogleGenerativeAI({ apiKey: process.env.GEMINI_API_KEY });


function safeParseJsonMaybeWrapped(text) {
  // Try plain parse first
  try {
    return JSON.parse(text);
  } catch (e) {
    // Try to extract JSON object or array from the text
    const match = text.match(/\{[\s\S]*\}|\[[\s\S]*\]/);
    if (match) {
      try {
        return JSON.parse(match[0]);
      } catch (e2) {
        return null;
      }
    }
    return null;
  }
}

export async function healWithAI(data, originalData, dirtyFields) {
  // Build a clear instruction prompting a strict JSON response.
  const prompt = `You are a data-healing assistant. Given the ORIGINAL data, the CURRENT cleaned data, and a list of dirty fields, return a single valid JSON object (no extra commentary) with keys: \n\n` +
    `1) \"healedData\": the fully healed record (object)\n` +
    `2) \"healedFields\": array of field names that were changed\n` +
    `3) \"newRules\": array of objects with \"name\" and \"description\" for new transformation rules the system should learn\n\n` +
    `Input JSON: \nORIGINAL: ${JSON.stringify(originalData)}\nCLEANED: ${JSON.stringify(data)}\nDIRTY_FIELDS: ${JSON.stringify(dirtyFields)}\n\n` +
    `Produce only the JSON object. Use conservative changes and prefer deterministic transformations (e.g., extract numbers, set defaults, trim whitespace, lowercase email).`;

  try {
    const resp = await client.generateText({
      model: 'models/gemini-2.5-flash',
      prompt: { text: prompt },
      temperature: 0.0,
      maxOutputTokens: 800,
    });

    // Response shape from @google/genai: resp.candidates[0].output or resp[0].candidates[0].output
    let textOutput = '';
    if (Array.isArray(resp)) {
      // some versions return an array
      textOutput = resp[0]?.candidates?.[0]?.output || '';
    } else {
      textOutput = resp.candidates?.[0]?.output || resp.output || '';
    }

    const parsed = safeParseJsonMaybeWrapped(textOutput);
    if (parsed && typeof parsed === 'object') {
      return {
        healedData: parsed.healedData ?? data,
        healedFields: parsed.healedFields ?? [],
        newRules: parsed.newRules ?? [],
        aiModel: 'gemini-2.5-flash',
        rawModelOutput: textOutput,
      };
    }

    // Fallback to basic local heuristics if parsing failed
    console.warn('Gemini output could not be parsed as JSON; falling back to local heuristics. Output:', textOutput);
  } catch (err) {
    console.error('Error calling Gemini API:', err.message || err);
  }

  // --- Local deterministic fallback (keeps previous behavior) ---
  const healedData = { ...data };
  const healedFields = [];
  const newRules = [];

  if (dirtyFields.some(field => field.includes('salary'))) {
    if (typeof data.salary === 'string' && /[a-zA-Z]/.test(data.salary)) {
      const extracted = data.salary.replace(/[^0-9.-]/g, '');
      const number = parseFloat(extracted);

      if (!isNaN(number) && number >= 0) {
        healedData.salary = number;
        healedFields.push('salary');

        newRules.push({
          name: 'Extract Salary Number',
          description: 'Extract numeric value from salary field when it contains text',
        });
      }
    } else if (!data.salary || data.salary === '' || isNaN(parseFloat(data.salary))) {
      healedData.salary = 0;
      healedFields.push('salary');
    }
  }

  if (dirtyFields.some(field => field.includes('age'))) {
    if (!data.age || data.age === '' || isNaN(parseFloat(data.age))) {
      healedData.age = 0;
      healedFields.push('age');

      newRules.push({
        name: 'Set Default Age',
        description: 'Set age to 0 when missing or invalid',
      });
    } else {
      const age = parseFloat(data.age);
      if (age < 0) {
        healedData.age = Math.abs(age);
        healedFields.push('age');

        newRules.push({
          name: 'Fix Negative Age',
          description: 'Convert negative age values to positive',
        });
      } else if (age > 150) {
        healedData.age = 0;
        healedFields.push('age');
      }
    }
  }

  if (dirtyFields.some(field => field.includes('email'))) {
    if (!data.email || !data.email.includes('@')) {
      healedData.email = 'unknown@example.com';
      healedFields.push('email');

      newRules.push({
        name: 'Default Email',
        description: 'Use default email when invalid or missing',
      });
    }
  }

  if (dirtyFields.some(field => field.includes('name'))) {
    if (!data.name || data.name.trim() === '') {
      healedData.name = 'Unknown User';
      healedFields.push('name');

      newRules.push({
        name: 'Default Name',
        description: 'Use "Unknown User" when name is missing',
      });
    }
  }

  return {
    healedData,
    healedFields,
    newRules,
    aiModel: 'Gemini (fallback)',
  };
}
