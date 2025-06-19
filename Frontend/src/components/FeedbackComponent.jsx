import React, { useState } from "react";
import { postFeedback } from "../apis/FeedbackApi"; // Ensure this points to your API file
import "bootstrap/dist/css/bootstrap.min.css";

const FeedbackComponent = ({ userId, processId, serviceId, onFeedbackSubmitted }) => {  
  const [showModal, setShowModal] = useState(false);
  const [feedback, setFeedback] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  const handleFeedbackSubmit = async () => {
    if (!feedback.trim()) {
      alert("Feedback cannot be empty!");
      return;
    }

    setIsSubmitting(true);
    const feedbackData = {  
      userId,
      process: processId,
      feedback,
      service: serviceId
    };

    try {
      await postFeedback(feedbackData);
      alert("Feedback submitted successfully!");
      setFeedback("");
      setShowModal(false);
      if (onFeedbackSubmitted) {
        onFeedbackSubmitted();
      }
    } catch (error) {
      console.error("Error submitting feedback:", error);
      alert("Failed to submit feedback. Please try again later.");
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div>
      {/* Feedback Button */}
      <button
        className="btn btn-primary"
        onClick={() => setShowModal(true)}
      >
        Give Feedback
      </button>

      {/* Feedback Modal */}
      {showModal && (
        <div
          className="modal show"
          style={{ display: "block", backgroundColor: "rgba(0,0,0,0.5)" }}
          tabIndex="-1"
        >
          <div className="modal-dialog">
            <div className="modal-content">
              <div className="modal-header">
                <h5 className="modal-title">Feedback</h5>
                <button
                  type="button"
                  className="btn-close"
                  onClick={() => setShowModal(false)}
                  aria-label="Close"
                ></button>
              </div>
              <div className="modal-body">
                <textarea
                  value={feedback}
                  onChange={(e) => setFeedback(e.target.value)}
                  rows="5"
                  placeholder="Write your feedback here..."
                  className="form-control"
                ></textarea>
              </div>
              <div className="modal-footer">
                <button
                  type="button"
                  className="btn btn-success"
                  onClick={handleFeedbackSubmit}
                  disabled={isSubmitting}
                >
                  {isSubmitting ? "Submitting..." : "Submit"}
                </button>
                <button
                  type="button"
                  className="btn btn-secondary"
                  onClick={() => setShowModal(false)}
                >
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default FeedbackComponent;
