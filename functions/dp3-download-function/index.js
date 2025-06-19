import { Storage } from '@google-cloud/storage';
const storage = new Storage();

const bucketName = 'dp3-backup-bucket'; 

export const getPdfFile = async (req, res) => {
    try {
        // Hardcoded PDF file name
        const fileName = 'Untitled_Report.pdf';  // Fix: added string quotes

        if (!fileName) {
            return res.status(400).send('File name is required');
        }

        // Access the specific file in the Cloud Storage bucket
        const bucket = storage.bucket(bucketName);
        const file = bucket.file(fileName);

        const [exists] = await file.exists();
        if (!exists) {
            return res.status(404).send('File not found');
        }

        // Add CORS headers
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Methods', 'GET, POST');
        res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

        // Set headers for downloading the file as PDF
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename=${fileName}`);

        // Pipe the file data to the response
        file.createReadStream().pipe(res);
    } catch (error) {
        console.error('Error retrieving PDF file:', error);
        res.status(500).send('Error retrieving the file');
    }
};
