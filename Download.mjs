import { get } from "https";
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

    // console.log(`Download started. Saving to: ${dest}`);
    await pipelineAsync(response, createWriteStream(dest));
    // console.log(`Download completed successfully. File saved to: ${dest}`);
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

// Main function
export async function main() {
  try {
    const url =
      "https://www.vermontsales.co.za/exports_v2/manufacturers.csv.gz";
    const downloadPath = "./downloads";
    const unzipPath = "./unzipped";

    // Generate file & path dynamically
    // console.log("Generating file path...");
    const fileName = generateFolderPath();
    // console.log(`File name generated: ${fileName}`);
    const filePath = `${downloadPath}/${fileName}`;
    // console.log(`File path generated: ${filePath}`);
    const outputFilePath = `${unzipPath}/products.csv`;
    // console.log(`Output file path generated: ${outputFilePath}`);

    // Ensure directory exists
    // console.log("Ensuring directory exists...");
    ensureDirectoryExistence(filePath);
    // console.log(`Directory exists: ${filePath}`);
    ensureDirectoryExistence(outputFilePath);

    // Download file
    // console.log("Downloading file from url");
    await downloadFile(url, filePath);
    // console.log(`File downloaded to: ${filePath}`);

    // Unzip the file
    // console.log("Unzipping file...");
    await unzipFile(filePath, outputFilePath);
    // console.log(
    //   `File unzipped successfully. Unzipped file saved to: ${outputFilePath}`
    // );
  } catch (error) {
    console.error("Error:", error);
  }
}

// Execute main function
main();
