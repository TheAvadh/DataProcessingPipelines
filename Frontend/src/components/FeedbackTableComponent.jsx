import React, { useEffect, useState } from 'react';
import { Table, Spinner, Alert } from 'react-bootstrap';
import { getFeedback } from '../apis/FeedbackApi';

const FeedbackTableComponent = ({ service, refreshTrigger }) => {   
  const [feedbacks, setFeedbacks] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);


  const fetchFeedbacks = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await getFeedback();
      console.log("Response:", response);
      if (response.data && response.data.body) {
        console.log("Feedbacks:", response.data.body);
        setFeedbacks(response.data.body);
      } else {
        setFeedbacks([]);
      }
    } catch (error) {
      console.error("Error fetching feedback data:", error);
      setError(`Error fetching feedback data: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchFeedbacks();
  }, [service, refreshTrigger]);

  return (
    <div className="feedback-table-container">
      <h3>Customer Feedbacks</h3>

      {loading && (
        <div className="text-center">
          <Spinner animation="border" variant="primary" />
          <p>Loading feedbacks...</p>
        </div>
      )}

      {error && <Alert variant="danger">{error}</Alert>}

      {!loading && !error && (
        feedbacks.length > 0 ? (
          <Table bordered hover responsive>
            <thead>
              <tr>
                <th>User Email</th>
                <th>Feedback</th>
                <th>Polarity</th>
                <th>Sentiment</th>
                <th>Timestamp</th>
              </tr>
            </thead>
            <tbody>
              {feedbacks.map((feedback) => (
                feedback.service === Number(service) && (
                <tr key={feedback.FeedbackId}>
                  <td>{feedback.UserId}</td>
                  <td>{feedback.Feedback}</td>
                  <td>{feedback.Polarity}</td>
                  <td>{feedback.Sentiment.toFixed(2)}</td>
                  <td>{new Date(feedback.Date).toLocaleString()}</td>
                </tr>
                )
              ))}
            </tbody>
          </Table>
        ) : (
          <p>No feedbacks available.</p>
        )
      )}
    </div>
  );
};

export default FeedbackTableComponent;
