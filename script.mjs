import { get } from "https";
import { Parse } from "unzipper";
import { existsSync, mkdirSync, createWriteStream, createReadStream } from "fs";
import { dirname as _dirname } from "path";
import csv from "csv-parser";
import { pipeline } from "stream";
import { promisify } from "util";
import { createGunzip } from "zlib";

// Function to generate file name based on current date and time
function generateFileName() {
  const now = new Date();
  const year = now.getFullYear().toString().slice(-2);
  const month = ("0" + (now.getMonth() + 1)).slice(-2);
  const day = ("0" + now.getDate()).slice(-2);
  const hour = ("0" + now.getHours()).slice(-2);
  const minute = ("0" + now.getMinutes()).slice(-2);
  return `${year}/${month}/${day}/${hour}/${minute}.csv.gz`;
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
  console.log(`Downloading file from: ${url}`);
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

// Main function
async function main() {
  try {
    const url = "https://www.vermontsales.co.za/exports_v2/products.csv.gz";
    const downloadPath = "./downloads";

    const fileName = generateFileName();
    const filePath = `${downloadPath}/${fileName}`;

    // Ensure directory exists
    ensureDirectoryExistence(filePath);

    // Download file
    await downloadFile(url, filePath);
    console.log(`File downloaded to: ${filePath}`);

    // Unzip the file
    const gunzip = createGunzip();
    const readStream = createReadStream(filePath);
    readStream.pipe(gunzip);

    const parseStream = Parse();
    gunzip.pipe(parseStream);

    parseStream.on("error", (error) => {
      console.error("Error unzipping file:", error);
    });

    parseStream.on("entry", (entry) => {
      const fileName = entry.path;
      const type = entry.type; // 'Directory' or 'File'

      console.log(`Entry: ${fileName}, Type: ${type}`);

      if (type === "File" && fileName === "products.csv") {
        // Process the CSV file
        entry
          .pipe(csv())
          .on("data", (row) => {
            console.log("Row:", row);
            // Process each row
          })
          .on("end", () => {
            console.log("CSV file processed successfully.");
          })
          .on("error", (error) => {
            console.error("Error processing CSV file:", error);
          });
      } else {
        entry.autodrain(); // Skip directories or other file types
      }
    });

    parseStream.on("finish", () => {
      console.log("File unzipped successfully.");
    });
  } catch (error) {
    console.error("Error:", error);
  }
}

// Execute main function
main();
