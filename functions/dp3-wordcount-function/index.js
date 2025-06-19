import { Storage } from '@google-cloud/storage';
import { BigQuery } from '@google-cloud/bigquery';

const storage = new Storage();
const bigQuery = new BigQuery();

const DESTINATION_BUCKET_NAME = 'dp3-processed-data-bucket';
const DATASET_ID = 'dp3_bigquery_dataset';

export const processFile = async (event, context) => {
    try {
        const fileName = event.name;
        const bucketName = event.bucket;

        console.log(`Processing file: ${fileName} from bucket: ${bucketName}`);

        const bucket = storage.bucket(bucketName);
        const file = bucket.file(fileName);

        // Get metadata (including user ID)
        const [metadata] = await file.getMetadata();
        const userId = metadata.metadata?.userId;
        if (!userId) {
            console.error('User ID not found in file metadata.');
            return;
        }

        console.log(`Processing file for user: ${userId}`);

        // Download the file content
        const [fileContents] = await file.download();
        const text = fileContents.toString();

        // Generate word counts
        const wordCounts = text.split(/\s+/).reduce((counts, word) => {
            word = word.toLowerCase();
            counts[word] = (counts[word] || 0) + 1;
            return counts;
        }, {});

        // Create a table for the user in BigQuery (if it doesn't exist)
        const sanitizedUserId = userId.replace(/-/g, '_'); // Replace hyphens with underscores
        const tableId = `${sanitizedUserId}_dp3_table`;
        const [tables] = await bigQuery.dataset(DATASET_ID).getTables();
        const tableExists = tables.some((table) => table.id === tableId);

        if (!tableExists) {
            await bigQuery.dataset(DATASET_ID).createTable(tableId, {
                schema: [
                    { name: 'reference_code', type: 'STRING' },
                    { name: 'word', type: 'STRING' },
                    { name: 'count', type: 'INTEGER' },
                ],
            });
            console.log(`Table ${tableId} created.`);
        }

        // Insert word count data into BigQuery
        const referenceCode = `${userId}-${Date.now()}`;
        const rows = Object.entries(wordCounts).map(([word, count]) => ({
            reference_code: referenceCode,
            word,
            count,
        }));

        await bigQuery.dataset(DATASET_ID).table(tableId).insert(rows);
        console.log('Data inserted into BigQuery.');

        // Save the processed file to the destination bucket
        const destinationBucket = storage.bucket(DESTINATION_BUCKET_NAME);
        const destinationFileName = `${fileName.replace('.txt', '')}-result.json`;
        await destinationBucket.file(destinationFileName).save(JSON.stringify(wordCounts));
        console.log(`Result file saved: ${destinationFileName}`);
    } catch (error) {
        console.error('Error processing file:', error.message);
    }
};
