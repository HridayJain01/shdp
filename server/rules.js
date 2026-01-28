export function applyLocalRules(data, rulebook) {
  let cleanedData = { ...data };
  const appliedRules = [];

  const activeRules = rulebook.filter(rule => rule.active);

  activeRules.forEach(rule => {
    let ruleApplied = false;

    if (rule.name === 'Trim Whitespace') {
      Object.keys(cleanedData).forEach(key => {
        if (typeof cleanedData[key] === 'string' && cleanedData[key] !== cleanedData[key].trim()) {
          cleanedData[key] = cleanedData[key].trim();
          ruleApplied = true;
        }
      });
    }

    if (rule.name === 'Lowercase Email') {
      if (cleanedData.email && typeof cleanedData.email === 'string') {
        const original = cleanedData.email;
        cleanedData.email = cleanedData.email.toLowerCase();
        if (original !== cleanedData.email) {
          ruleApplied = true;
        }
      }
    }

    if (rule.name === 'Default Country') {
      if (!cleanedData.country || cleanedData.country.trim() === '') {
        cleanedData.country = 'Unknown';
        ruleApplied = true;
      }
    }

    if (rule.name === 'Extract Salary Number' || rule.description.includes('extract number from salary')) {
      if (cleanedData.salary && typeof cleanedData.salary === 'string') {
        const extracted = cleanedData.salary.replace(/[^0-9.-]/g, '');
        const number = parseFloat(extracted);
        if (!isNaN(number)) {
          cleanedData.salary = number;
          ruleApplied = true;
        }
      }
    }

    if (rule.name === 'Set Default Age' || rule.description.includes('default age')) {
      if (!cleanedData.age || cleanedData.age === '' || isNaN(parseFloat(cleanedData.age))) {
        cleanedData.age = 0;
        ruleApplied = true;
      }
    }

    if (ruleApplied) {
      appliedRules.push({
        id: rule.id,
        name: rule.name,
        source: rule.source,
      });
    }
  });

  return { data: cleanedData, appliedRules };
}
