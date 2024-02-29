import { main as mainDownload } from "./Download.mjs";
import { main as mainScraper } from "./Scraper.mjs";

// Main function
async function main() {
  try {
    await mainDownload();
    await mainScraper();
  } catch (error) {
    console.error("Error:", error);
  }
}

// Execute main function
main();
