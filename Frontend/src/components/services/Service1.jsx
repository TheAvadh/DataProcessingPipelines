import React, { useEffect, useState } from 'react';
import { Card, Row, Col, Button, Table, Spinner, Toast, ToastContainer } from 'react-bootstrap';
import { useAuth } from '../../context/authContext';
import FeedbackComponent from '../FeedbackComponent';
import FeedbackTableComponent from '../FeedbackTableComponent';

const Service1 = ({ service }) => {
    const [uploadedFile, setUploadedFile] = useState(null);
    const [referenceCode, setReferenceCode] = useState('');
    const [processingStatus, setProcessingStatus] = useState('');
    const [downloadUrl, setDownloadUrl] = useState(null);
    const [loading, setLoading] = useState(false);
    const [toastMessage, setToastMessage] = useState('');
    const [showToast, setShowToast] = useState(false);
    const [previousResults, setPreviousResults] = useState([]);
    const { numberOfFiles, setNumberOfFiles } = useAuth();
    const userEmail = localStorage.getItem('userEmail');
    const [feedbackRefresh, setFeedbackRefresh] = useState(0);


    const showToastMessage = (message) => {
        setToastMessage(message);
        setShowToast(true);
    };

    const handleFileUpload = (event) => {
        const file = event.target.files[0];
        if (file) {
            setUploadedFile(file);
            setDownloadUrl(null);
            setReferenceCode('');
            setProcessingStatus('');
            showToastMessage('File selected successfully.');
        }
    };

    const handleProcessFile = async () => {

        if (!uploadedFile) {
            showToastMessage('Please upload a file first.');
            return;
        }

        setLoading(true);

        try {
            const payload = {
                file_name: uploadedFile.name,
                file_data: btoa(await uploadedFile.text()),
                user_email: userEmail,
            };

            const response = await fetch('https://2tn3ehho7sba6dnavpexebsn7m0soppt.lambda-url.us-east-1.on.aws/', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });

            const result = await response.json();
            setReferenceCode(result.reference_code);
            setProcessingStatus('In Progress');
            setNumberOfFiles(numberOfFiles + 1);
            showToastMessage('File processing started successfully.');

        } catch (error) {
            console.error('Error during file processing:', error);
            showToastMessage('Error during file processing.');
        } finally {
            setLoading(false);
        }
    };

    const handleCheckStatus = async () => {
        if (!referenceCode) {
            showToastMessage('No reference code available. Please process a file first.');
            return;
        }

        try {
            const response = await fetch('https://dzt55vkjhcmjtn54dw2k2ckaee0ksnrb.lambda-url.us-east-1.on.aws/', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ reference_code: referenceCode }),
            });

            const result = await response.json();
            setProcessingStatus(result.processing_status);

            if (result.processing_status === 'Succeeded') {
                setDownloadUrl(result.public_download_url);
                showToastMessage('File processing completed. Ready for download.');
                fetchPreviousResults();
            } else {
                showToastMessage('File is still processing. Please check again later.');
            }
        } catch (error) {
            console.error('Error during status check:', error);
            showToastMessage('Error checking file status.');
        }
    };

    const handleFeedbackSubmitted = () => {
        setFeedbackRefresh(prev => prev + 1);
    };

    const fetchPreviousResults = async () => {
        try {
            const response = await fetch(`https://hqwomdwdhx7b3fmpgmawhugpr40gtvmh.lambda-url.us-east-1.on.aws/?user_email=${encodeURIComponent(userEmail)}`);
            const results = await response.json();
            setPreviousResults(results.processing_results);
        } catch (error) {
            console.error('Error fetching previous results:', error);
        }
    };

    useEffect(() => {
        fetchPreviousResults();
    }, []);

    const formatDate = (dateString) => {
        // Remove microseconds and keep the timestamp in a format that JS can handle
        const formattedDateString = dateString.split('.')[0];  // Split and remove microseconds
        const date = new Date(formattedDateString);  // Create Date object

        return date.toLocaleString();  // Format the date to a readable string
    };

    return (
        <Card>
            <Card.Header className="bg-primary text-white">
                <h3>{service.name}</h3>
            </Card.Header>
            <Card.Body>
                <Row className="mb-3">
                    <Col md={6}>
                        <input
                            type="file"
                            accept=".json"
                            onChange={handleFileUpload}
                            className="form-control"
                        />
                    </Col>
                    <Col md={6} className="d-flex justify-content-end align-items-center">
                        <Button
                            variant="success"
                            onClick={handleProcessFile}
                            disabled={!uploadedFile || loading}
                            className="mx-2"
                        >
                            {loading ? <Spinner animation="border" size="sm" /> : 'Process JSON to CSV'}
                        </Button>
                        <Button
                            variant="info"
                            onClick={handleCheckStatus}
                            disabled={!referenceCode}
                            className="mx-2"
                        >
                            Check Status
                        </Button>
                        <Button
                            variant={downloadUrl ? 'success' : 'secondary'}
                            href={downloadUrl || '#'}
                            target="_blank"
                            disabled={!downloadUrl}
                            className="mx-2 d-flex align-items-center"
                        >
                            Download File
                        </Button>

                    </Col>
                </Row>

                <h5 className="mt-4">Previous Processing Results</h5>
                <Table bordered hover>
                    <thead>
                    <tr>
                        <th>Reference Code</th>
                        <th>File Name</th>
                        <th>Date</th>
                        <th>Status</th>
                        <th>Download Link</th>
                    </tr>
                    </thead>
                    <tbody>
                    {previousResults.length > 0 ? (
                        previousResults.map((result, index) => (
                            <tr key={index}>
                                <td>{result.reference_code}</td>
                                <td>{result.file_name}</td>
                                <td>{formatDate(result.processing_date)}</td>
                                <td>{result.processing_status}</td>
                                <td>
                                    {result.public_download_url ? (
                                        <a href={result.public_download_url} target="_blank" rel="noopener noreferrer">
                                            Download
                                        </a>
                                    ) : (
                                        'N/A'
                                    )}
                                </td>
                            </tr>
                        ))
                    ) : (
                        <tr>
                            <td colSpan="3" className="text-center">
                                No previous results found.
                            </td>
                        </tr>
                    )}
                    </tbody>
                </Table>
                <div className="mt-4">
                    <FeedbackComponent 
                        userId={userEmail}
                        processId={referenceCode || 'unknown'} 
                        serviceId={1}
                        onFeedbackSubmitted={handleFeedbackSubmitted} 
                    />
                    <FeedbackTableComponent 
                        service={1} 
                        refreshTrigger={feedbackRefresh}
                    />
                </div>
                <ToastContainer position="top-end" className="p-3">
                    <Toast
                        onClose={() => setShowToast(false)}
                        show={showToast}
                        delay={2000}
                        autohide={true}
                    >
                        <Toast.Header>
                            <strong className="me-auto">Notification</strong>
                        </Toast.Header>
                        <Toast.Body>{toastMessage || 'Default Message'}</Toast.Body>
                    </Toast>
                </ToastContainer>

            </Card.Body>


        </Card>
    );
};

export default Service1;
