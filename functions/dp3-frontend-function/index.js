import { Storage } from '@google-cloud/storage';
import jwt from 'jsonwebtoken';
import cors from 'cors';

const storage = new Storage();
const BUCKET_NAME = 'dp3-raw-files-bucket';

// Initialize CORS middleware
const corsMiddleware = cors({ origin: true });

export const uploadFileWithMetadata = async (req, res) => {
    corsMiddleware(req, res, async () => {
        try {
            // Handle preflight OPTIONS request
            if (req.method === 'OPTIONS') {
                res.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
                res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
                res.set('Access-Control-Allow-Origin', '*');  // Allow all origins
                res.status(204).send('');
                return;
            }

            // Check for JWT token and extract the user ID
            const authHeader = req.headers.authorization;
            if (!authHeader || !authHeader.startsWith('Bearer ')) {
                return res.status(401).json({ error: 'Unauthorized: Missing or invalid token.' });
            }

            const token = authHeader.split(' ')[1];
            const decodedToken = jwt.decode(token, { complete: true });
            if (!decodedToken) {
                return res.status(401).json({ error: 'Unauthorized: Invalid token.' });
            }

            const userId = decodedToken.payload.sub;
            console.log(`Authenticated User ID: ${userId}`);

            // Get the uploaded file
            const file = req.body;
            if (!file || !file.file_name || !file.data) {
                return res.status(400).json({ error: 'Invalid file payload.' });
            }

            const bucket = storage.bucket(BUCKET_NAME);
            const fileName = `${userId}_${Date.now()}_${file.file_name}`; // Unique file name based on user ID
            const fileBuffer = Buffer.from(file.data, 'utf-8');  // Treat data as UTF-8 text

            // Upload the file with metadata
            const fileUpload = bucket.file(fileName);
            await fileUpload.save(fileBuffer, {
                contentType: 'text/plain',  // Assuming the file is text-based
                metadata: {
                    metadata: {
                        userId: userId, // Custom metadata to store the user ID
                    },
                },
            });
            console.log(`File uploaded to Cloud Storage with metadata: ${fileName}`);

            // Set CORS headers for the response
            res.set('Access-Control-Allow-Origin', '*');  // Allow all origins
            res.status(200).json({ message: 'File uploaded successfully.' });
        } catch (error) {
            console.error('Error uploading file:', error.message);
            res.status(500).json({ error: 'Internal server error.' });
        }
    });
};
