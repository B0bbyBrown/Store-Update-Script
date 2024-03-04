import path from "path";
import {
  createReadStream,
  createWriteStream,
  promises as fsPromises,
} from "fs";
import csv from "csv-parser";

// Function to find the latest CSV file in a directory
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

// Function to read the CSV file
function readCSV(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    createReadStream(filePath)
      .pipe(csv())
      .on("data", (row) => {
        results.push(row);
      })
      .on("end", () => {
        resolve(results);
      })
      .on("error", (error) => {
        reject(error);
      });
  });
}

// Function to filter data based on "manufacturer" column (column K) and "TORK CRAFT" value
function filterData(csvData) {
  // Find the index of the "manufacturer" column (column K)
  const manufacturerColumnIndex = 10; // Column K (zero-based index)

  // Filter rows based on the value "TORK CRAFT" in the "manufacturer" column
  const filteredRows = csvData.filter((row, index) => {
    if (index === 0) return true; // Include the header row

    return row[manufacturerColumnIndex] === "TORK CRAFT";
  });

  // If no matching rows are found, return an empty array
  if (filteredRows.length === 0) {
    console.log(`No rows found with "TORK CRAFT" in column K.`);
    return [];
  }

  // Include the header row as the first element in the filteredData
  const filteredData = [csvData[0]];

  // Add rows with "TORK CRAFT" in column K to the filteredData
  filteredRows.forEach((row) => {
    filteredData.push(row);
  });

  return filteredData;
}

// Function to save filtered data to a new CSV file with its own folder structure
async function saveToCSV(filteredData, outputRootDirectory) {
  if (filteredData.length === 0) {
    throw new Error("No filtered data to save.");
  }

  const now = new Date();
  const year = now.getFullYear().toString().slice(-2);
  const month = ("0" + (now.getMonth() + 1)).slice(-2);
  const day = ("0" + now.getDate()).slice(-2);
  const hour = ("0" + now.getHours()).slice(-2);
  const minute = ("0" + now.getMinutes()).slice(-2);
  const folderPath = `${year}/${month}/${day}/${hour}/${minute}`;
  const filteredFolderPath = path.join(outputRootDirectory, folderPath);

  // Create directory if it doesn't exist
  await fsPromises.mkdir(filteredFolderPath, { recursive: true });

  const fileName = "filtered_data.csv"; // You can change the file name as needed
  const outputPath = path.join(filteredFolderPath, fileName);

  const csvWriter = createWriteStream(outputPath);
  csvWriter.write(`${Object.keys(filteredData[0]).join(",")}\n`);
  filteredData.forEach((row) => {
    csvWriter.write(`${Object.values(row).join(",")}\n`);
  });
  csvWriter.end();

  return outputPath;
}

// Main function
export async function main() {
  try {
    const unzippedDirectoryPath =
      "C:\\Bobby Brown\\Work-File\\Nova-Web-Solutions\\CCC\\New\\v1\\Store-Update-Script\\unzipped"; // Directory path containing unzipped CSV files
    const outputRootDirectory = "./filtered_data"; // Root directory for saving the filtered data
    const modelNumbers = ["TORK CRAFT"]; // Hardcoded model numbers

    // Find the latest CSV file in the directory
    const latestCSVFile = await findLatestCSV(unzippedDirectoryPath);
    console.log("Reading the latest CSV file...");

    // Read the latest CSV file
    const csvData = await readCSV(latestCSVFile);

    // Filter data based on hardcoded model numbers
    console.log("Filtering data...");
    const filteredData = filterData(csvData, modelNumbers);

    // Save filtered data to a new CSV file with its own folder structure
    console.log("Saving filtered data...");
    const outputPath = await saveToCSV(filteredData, outputRootDirectory);

    console.log("Filtered data saved to:", outputPath);
  } catch (error) {
    console.error("Error:", error);
  }
}

// Execute main function
main();
