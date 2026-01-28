export function detectDirtyData(data) {
  const dirtyFields = [];

  if (!data.name || data.name.trim() === '') {
    dirtyFields.push('name (empty)');
  } else if (data.name !== data.name.trim()) {
    dirtyFields.push('name (whitespace)');
  }

  if (!data.email || data.email.trim() === '') {
    dirtyFields.push('email (empty)');
  } else {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(data.email)) {
      dirtyFields.push('email (invalid format)');
    } else if (data.email !== data.email.toLowerCase()) {
      dirtyFields.push('email (not lowercase)');
    }
  }

  if (data.age === undefined || data.age === null || data.age === '') {
    dirtyFields.push('age (empty)');
  } else {
    const age = typeof data.age === 'string' ? parseFloat(data.age) : data.age;
    if (isNaN(age)) {
      dirtyFields.push('age (not a number)');
    } else if (age < 0 || age > 150) {
      dirtyFields.push('age (unrealistic)');
    }
  }

  if (data.salary === undefined || data.salary === null || data.salary === '') {
    dirtyFields.push('salary (empty)');
  } else {
    if (typeof data.salary === 'string' && /[a-zA-Z]/.test(data.salary)) {
      dirtyFields.push('salary (contains text)');
    } else {
      const salary = typeof data.salary === 'string' ? parseFloat(data.salary.replace(/[^0-9.-]/g, '')) : data.salary;
      if (isNaN(salary)) {
        dirtyFields.push('salary (not a number)');
      } else if (salary < 0) {
        dirtyFields.push('salary (negative)');
      }
    }
  }

  if (!data.country || data.country.trim() === '') {
    dirtyFields.push('country (empty)');
  } else if (data.country !== data.country.trim()) {
    dirtyFields.push('country (whitespace)');
  }

  return dirtyFields;
}
