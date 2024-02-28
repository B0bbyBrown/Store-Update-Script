import { get } from "https";
import { Parse } from "unzipper";
import { existsSync, mkdirSync, createWriteStream, createReadStream } from "fs";
import { dirname } from "path";
import csv from "csv-parser";
import { pipeline } from "stream";
import { promisify } from "util";
import { createGunzip } from "zlib";
import { existsSync as _existsSync, mkdirSync as _mkdirSync } from "fs";
import { join } from "path";

//Generate File Path
function generateFilePath() {
  const now = new Date();
  const year = now.getFullYear().toString().slice(-2);
  const month = ("0" + (now.getMonth() + 1)).slice(-2);
  const day = ("0" + now.getDate()).slice(-2);
  const hour = ("0" + now.getHours()).slice(-2);
  const minute = ("0" + now.getMinutes()).slice(-2);

  // Construct folder path
  const folderPath = join(__dirname, `${year}/${month}/${day}/${hour}`);

  // Create folder recursively if it doesn't exist
  if (!_existsSync(folderPath)) {
    _mkdirSync(folderPath, { recursive: true });
  }

  // Construct file path without the file name
  const filePath = join(folderPath, `${minute}.csv.gz`);

  return filePath;
}

//Ensure Directory Existence
async function ensureDirectoryExistence(filePath, maxAttempts = 3) {
  const dirName = dirname(filePath);
  try {
    if (!existsSync(dirName)) {
      mkdirSync(dirName, { recursive: true });
      console.log(`Directory created: ${dirName}`);
    }
    return true;
  } catch (error) {
    console.error(`Failed to create directory: ${dirName}`, error);
    if (maxAttempts <= 0) throw error;
    return ensureDirectoryExistence(filePath, maxAttempts - 1);
  }
}

//Download File
async function downloadFile(url, dest, maxAttempts = 3) {
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
    await promisify(pipeline)(response, createWriteStream(dest));
    console.log(`Download completed successfully. File saved to: ${dest}`);
  } catch (error) {
    console.error(`Failed to download file: ${error.message}`);
    if (maxAttempts <= 0) throw error;
    return downloadFile(url, dest, maxAttempts - 1);
  }
}

//main
async function main() {
  try {
    const url = "https://www.vermontsales.co.za/exports_v2/products.csv.gz";
    const downloadPath = "./zipped-downloads";

    const filePath = generateFilePath();

    await ensureDirectoryExistence(filePath);

    await downloadFile(url, filePath);
    console.log(`File downloaded to: ${filePath}`);

    const gunzip = createGunzip();
    const readStream = createReadStream(filePath);
    const parseStream = readStream.pipe(gunzip).pipe(Parse());

    parseStream.on("entry", (entry) => {
      const fileName = entry.path;
      const type = entry.type;

      console.log(`Entry: ${fileName}, Type: ${type}`);

      if (type === "File" && fileName === "products.csv") {
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

    parseStream.on("error", (error) => {
      console.error("Error during unzipping:", error);
    });

    parseStream.on("finish", () => {
      console.log("File unzipped successfully.");
    });
  } catch (error) {
    console.error("Error:", error);
  }
}

main();
