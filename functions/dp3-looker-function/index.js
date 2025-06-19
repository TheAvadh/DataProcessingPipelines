import { BigQuery } from '@google-cloud/bigquery';

const bigQuery = new BigQuery();
const DATASET_ID = 'dp3_bigquery_dataset';
const SOURCE_TABLE = 'f4783458_d011_7022_9f33_9211cfa86b94_dp3_table';
const TARGET_TABLE = 'dp3_wordcloud_table';

export const updateWordCloudData = async (event, context) => {
    try {
        // Query the latest reference_code
        const queryLatestReferenceCode = `
            SELECT reference_code
            FROM \`${DATASET_ID}.${SOURCE_TABLE}\`
            ORDER BY reference_code DESC
            LIMIT 1
        `;

        const [latestReferenceResults] = await bigQuery.query(queryLatestReferenceCode);
        if (latestReferenceResults.length === 0) {
            console.log('No data found.');
            return;
        }

        const latestReferenceCode = latestReferenceResults[0].reference_code;
        console.log(`Latest Reference Code: ${latestReferenceCode}`);

        // Query word counts for the latest reference_code
        const queryWordCounts = `
            SELECT word, SUM(count) AS total_count
            FROM \`${DATASET_ID}.${SOURCE_TABLE}\`
            WHERE reference_code = @reference_code
            GROUP BY word
            ORDER BY total_count DESC
        `;

        const options = {
            query: queryWordCounts,
            params: { reference_code: latestReferenceCode },
            location: 'US',
        };

        const [wordCountResults] = await bigQuery.query(options);

        console.log(`Fetched ${wordCountResults.length} rows for reference_code ${latestReferenceCode}`);

        // Clear existing rows in the target table
        const truncateTableQuery = `
            DELETE FROM \`${DATASET_ID}.${TARGET_TABLE}\` WHERE TRUE
        `;
        await bigQuery.query({ query: truncateTableQuery, location: 'US' });
        console.log(`Cleared all rows from ${TARGET_TABLE}`);

        // Insert new data
        const targetTable = bigQuery.dataset(DATASET_ID).table(TARGET_TABLE);
        await targetTable.insert(wordCountResults);
        console.log(`Inserted word count data into ${TARGET_TABLE}`);
    } catch (error) {
        console.error('Error updating word cloud data:', error.message);
    }
};
