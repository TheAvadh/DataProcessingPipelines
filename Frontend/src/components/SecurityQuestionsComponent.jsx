import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { getQuestions, postUser, postAnswers } from '../apis/AuthApis';
import { toast } from 'react-toastify';
import 'bootstrap/dist/css/bootstrap.min.css';

const SecurityQuestionsComponent = (data) => {
  const navigate = useNavigate();
  const email = data.email;
  const name = data.name;

  const [questions, setQuestions] = useState([]);
  const [selectedQuestion, setSelectedQuestion] = useState(null);
  const [answer, setAnswer] = useState('');
  const [message, setMessage] = useState(null);

  useEffect(() => {
    fetchQuestions();
  }, []);

  const fetchQuestions = async () => {
    try {
      const response = await getQuestions();
      setQuestions(response.data.body);
    } catch (error) {
      console.error('Failed to fetch security questions:', error);
      setMessage('Failed to fetch security questions. Please try again.');
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      await postAnswers({
        email: email,
        ans: answer
      });

      await postUser({
        email: email,
        queId: selectedQuestion,
        name: name,
        role: "user"
      });
      
      navigate('/');
      toast.success('Security answer submitted successfully');
    } catch (error) {
      console.error('Failed to submit security answer:', error);
      setMessage('Failed to submit security answer. Please try again.');
    }
  };

  return (
    <div className="container-fluid security-questions-background d-flex align-items-center vh-100">
      <div className="row justify-content-center w-100">
        <div className="col-md-6 col-lg-4">
          <div className="card shadow-lg border-0 rounded-lg">
            <div className="card-header bg-primary text-white text-center">
              <h2 className="my-2">Set Security Question</h2>
            </div>
            <div className="card-body">
              <form onSubmit={handleSubmit}>
                <div className="form-group mb-3">
                  <label htmlFor="question" className="form-label fw-bold">Security Question</label>
                  <select
                    id="question"
                    className="form-select"
                    value={selectedQuestion || ''}
                    onChange={(e) => setSelectedQuestion(parseInt(e.target.value))}
                    required
                  >
                    <option value="">Select a question</option>
                    {questions.map((q, idx) => (
                      <option key={idx} value={q.id}>{q.question}</option>
                    ))}
                  </select>
                </div>
                
                <div className="form-group mb-3">
                  <label htmlFor="answer" className="form-label fw-bold">Your Answer</label>
                  <input
                    type="text"
                    id="answer"
                    className="form-control"
                    value={answer}
                    onChange={(e) => setAnswer(e.target.value)}
                    placeholder="Enter your answer"
                    required
                  />
                </div>

                {message && <p className="text-danger">{message}</p>}

                <div className="d-grid">
                  <button type="submit" className="btn btn-primary btn-block">
                    Submit Answer
                  </button>
                </div>
              </form>
            </div>
            <div className="card-footer text-center py-3 bg-light">
              <span className="text-muted small">
                Need help? <a href="#" className="text-primary">Contact Support</a>
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SecurityQuestionsComponent;
