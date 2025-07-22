const fs = require('fs').promises;
const path = require('path');
const { parse } = require('csv-parse');
const axios = require('axios');
const Bottleneck = require('bottleneck');

// Input variables
const csv_folder_path = 'files';
const hubspot_api_key = '';
const custom_event_name = '';

// Create a limiter: max 10 requests per second
const limiter = new Bottleneck({
    minTime: 100, // 100ms between requests (1000ms/10)
    maxConcurrent: 1
});

/**
 * Sends click data to HubSpot's Custom Event API
 * @param {string} eventName - The name of the custom event
 * @param {Date} eventDate - The date when the event occurred
 * @param {number} clicks - Number of clicks
 * @param {string} domain - The domain associated with the clicks
 */
async function sendToHubSpot(eventName, eventDate, clicks, domain) {
    try {
        // Set timestamp to noon on the extracted date
        const timestamp = new Date(eventDate);
        timestamp.setHours(12, 0, 0, 0);

        const requestData = {
            eventName: eventName,
            occurredAt: timestamp.toISOString(),
            properties: {
                clicks: clicks,
                domain: domain
            }
        };

        const response = await limiter.schedule(() =>
            axios.post(
                'https://api.hubapi.com/events/v3/send',
                requestData,
                {
                    headers: {
                        'Authorization': `Bearer ${hubspot_api_key}`,
                        'Content-Type': 'application/json'
                    }
                }
            )
        );

        console.log(`Successfully sent data to HubSpot for ${domain} with ${clicks} clicks on ${eventDate}`);
        return response.data;

    } catch (error) {
        console.error(`Error sending data to HubSpot for ${domain}:`, error.response?.data || error.message);
        throw error;
    }
}

/**
 * Extracts date from filename using regex pattern
 * @param {string} filename - The CSV filename
 * @returns {string|null} - Extracted date or null if not found
 */
function extractDateFromFilename(filename) {
    // Matches pattern like ".YYYY-MM-DD_YYYY-MM-DD"
    const dateRegex = /\.(\d{4}-\d{2}-\d{2})_\d{4}-\d{2}-\d{2}/;
    const match = filename.match(dateRegex);
    return match ? match[1] : null;
}

/**
 * Parses a CSV file and returns the data as an array of objects
 * @param {string} filePath - Path to the CSV file
 * @returns {Promise<Array>} - Parsed CSV data
 */
async function parseCSVFile(filePath) {
    return new Promise((resolve, reject) => {
        const results = [];
        const fileContent = require('fs').createReadStream(filePath);
        
        fileContent
            .pipe(parse({
                columns: true, // Use first row as column headers
                skip_empty_lines: true,
                trim: true
            }))
            .on('data', (row) => {
                results.push(row);
            })
            .on('end', () => {
                resolve(results);
            })
            .on('error', (error) => {
                reject(error);
            });
    });
}

/**
 * Processes a single CSV file
 * @param {string} filePath - Path to the CSV file
 * @param {string} filename - Name of the CSV file
 */
async function processCSVFile(filePath, filename) {
    try {
        console.log(`Processing file: ${filename}`);
        
        // Extract date from filename
        const extractedDate = extractDateFromFilename(filename);
        if (!extractedDate) {
            console.warn(`Could not extract date from filename: ${filename}`);
            return;
        }

        // Parse the CSV file
        const csvData = await parseCSVFile(filePath);
        console.log(`Found ${csvData.length} rows in ${filename}`);

        // Process each row
        for (const row of csvData) {
            try {
                // Check if clicks column exists and has value greater than 0
                const clicks = parseInt(row.clicks || row.Clicks || 0);
                const domain = row.domain || row.Domain || '';

                if (clicks > 0 && domain) {
                    console.log(`Processing row: Domain=${domain}, Clicks=${clicks}, Date=${extractedDate}`);
                    
                    // Send data to HubSpot
                    await sendToHubSpot(custom_event_name, extractedDate, clicks, domain);
                    
                    // Add small delay to avoid rate limiting
                    await new Promise(resolve => setTimeout(resolve, 100));
                } else if (clicks <= 0) {
                    //console.log(`Skipping row for domain ${domain}: clicks (${clicks}) <= 0`);
                } else {
                    //console.log(`Skipping row: missing domain information`);
                }
            } catch (rowError) {
                //console.error(`Error processing row in ${filename}:`, rowError.message);
                // Continue processing other rows even if one fails
            }
        }

        //console.log(`Completed processing file: ${filename}`);

    } catch (error) {
        //console.error(`Error processing CSV file ${filename}:`, error.message);
    }
}

/**
 * Main function that processes all CSV files in the specified folder
 */
async function processAllCSVFiles() {
    try {
        console.log(`Starting to process CSV files from: ${csv_folder_path}`);

        // Read all files in the directory
        const files = await fs.readdir(csv_folder_path);
        
        // Filter to include only CSV files
        const csvFiles = files.filter(file => 
            path.extname(file).toLowerCase() === '.csv'
        );

        if (csvFiles.length === 0) {
            console.log('No CSV files found in the specified directory.');
            return;
        }

        console.log(`Found ${csvFiles.length} CSV files to process`);

        // Process each CSV file
        for (const csvFile of csvFiles) {
            const filePath = path.join(csv_folder_path, csvFile);
            await processCSVFile(filePath, csvFile);
        }

        console.log('Completed processing all CSV files');

    } catch (error) {
        console.error('Error in main processing function:', error.message);
        
        if (error.code === 'ENOENT') {
            console.error(`Directory not found: ${csv_folder_path}`);
        } else if (error.code === 'EACCES') {
            console.error(`Permission denied accessing directory: ${csv_folder_path}`);
        }
    }
}

/**
 * Entry point - Execute the main function
 */
async function main() {
    try {
        await processAllCSVFiles();
    } catch (error) {
        console.error('Script execution failed:', error.message);
        process.exit(1);
    }
}

// Run the script
main();
