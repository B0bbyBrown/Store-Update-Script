import { get } from "https";
import { Parse } from "unzipper";
import { existsSync, mkdirSync, createWriteStream, createReadStream } from "fs";
import { dirname as _dirname } from "path";
import csv from "csv-parser";
import { pipeline } from "stream";
import { promisify } from "util";
import { createGunzip } from "zlib";

// Function to generate folder name based on current date and time
function generateFolderPath() {
  const now = new Date();
  const year = now.getFullYear().toString().slice(-2);
  const month = ("0" + (now.getMonth() + 1)).slice(-2);
  const day = ("0" + now.getDate()).slice(-2);
  const hour = ("0" + now.getHours()).slice(-2);
  const minute = ("0" + now.getMinutes()).slice(-2);
  return `${year}/${month}/${day}/${hour}/${minute}`;
}

// Function to ensure directory exists
function ensureDirectoryExistence(filePath) {
  const dirname = _dirname(filePath);
  if (existsSync(dirname)) {
    return true;
  }
  ensureDirectoryExistence(dirname);
  mkdirSync(dirname);
}

// Function to download file from URL
async function downloadFile(url, dest) {
  const pipelineAsync = promisify(pipeline);
  console.log(`Starting the download of file from: ${url}`);
  try {
    const response = await new Promise((resolve, reject) => {
      get(url, (response) => {
        if (response.statusCode !== 200) {
          reject(
            new Error(
              `Failed to download file: HTTP status ${response.statusCode}`
            )
          );
          return;
        }
        resolve(response);
      }).on("error", (error) => {
        reject(new Error(`Failed to download file: ${error.message}`));
      });
    });

    console.log(`Download started. Saving to: ${dest}`);
    await pipelineAsync(response, createWriteStream(dest));
    console.log(`Download completed successfully. File saved to: ${dest}`);
  } catch (error) {
    console.error(`Failed to download file: ${error.message}`);
    throw new Error(`Failed to download file: ${error.message}`);
  }
}

// Function to unzip file
async function unzipFile(inputFilePath, outputFilePath) {
  return new Promise((resolve, reject) => {
    const gunzip = createGunzip();
    const readStream = createReadStream(inputFilePath);
    const writeStream = createWriteStream(outputFilePath);

    readStream.pipe(gunzip).pipe(writeStream);

    writeStream.on("finish", resolve);
    writeStream.on("error", reject);
  });
}

// Function to process CSV data
async function processCSV(filePath) {
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

// Main function
async function main() {
  try {
    const url = "https://www.vermontsales.co.za/exports_v2/products.csv.gz";
    const downloadPath = "./downloads";
    const unzipPath = "./unzipped";

    const fileName = generateFolderPath();
    const filePath = `${downloadPath}/${fileName}`;
    const outputFilePath = `${unzipPath}/products.csv`;

    // Ensure directory exists
    ensureDirectoryExistence(filePath);
    ensureDirectoryExistence(outputFilePath);

    // Download file
    await downloadFile(url, filePath);
    console.log(`File downloaded to: ${filePath}`);

    // Unzip the file
    console.log("Unzipping file...");
    await unzipFile(filePath, outputFilePath);
    console.log(
      `File unzipped successfully. Unzipped file saved to: ${outputFilePath}`
    );

    // Process the unzipped CSV file
    console.log("Processing the unzipped CSV file...");
    const csvData = await processCSV(outputFilePath);
    console.log("CSV file processed successfully.");
    console.log("CSV Data:", csvData);

    // Further processing of the CSV data can be done here
  } catch (error) {
    console.error("Error:", error);
  }
}

// Execute main function
main();
