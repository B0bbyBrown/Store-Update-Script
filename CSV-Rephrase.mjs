//find latest csv for rephrasing
async function findLatestCSV(directoryPath) {
  const files = await fsPromises.readdir(directoryPath);
  const csvFiles = files.filter((file) => file.endsWith(".csv"));

  if (csvFiles.length === 0) {
    throw new Error("No CSV files found in the directory.");
  }

  let latestFile = null;
  let latestFileMtimeMs = 0;

  for (const file of csvFiles) {
    const filePath = path.join(directoryPath, file);
    const fileStat = await fsPromises.stat(filePath);
    if (!latestFile || fileStat.mtimeMs > latestFileMtimeMs) {
      latestFile = filePath;
      latestFileMtimeMs = fileStat.mtimeMs;
    }
  }

  return latestFile;
}

//rephrasing from excel html csv - common csv

async function rephraseCSV(filePath) {
  const results = [];
  const csvData = await readCSV(filePath);
  csvData.forEach((row) => {
    results.push({
      "Model Number": row.model,
      Price: row.price,
      Quantity: row.quantity,
    });
  });
  return results;
}

//export data to new
