import React, { useState, useContext } from 'react';
import { Form, Button, Alert, Container, Row, Col, Card } from 'react-bootstrap';
import { AuthContext } from '../context/authContext';
import { useNavigate } from 'react-router-dom';
import 'bootstrap/dist/css/bootstrap.min.css';

const ForgotPasswordComponent = () => {
  const { forgotPassword, confirmPassword } = useContext(AuthContext);
  const [email, setEmail] = useState('');
  const [code, setCode] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [stage, setStage] = useState('request');
  const [message, setMessage] = useState(null);
  const [error, setError] = useState(null);
  const navigate = useNavigate();

  const handleForgotPassword = async () => {
    try {
      await forgotPassword(email);
      setStage('confirm');
      setMessage('Verification code sent to your email.');
      setError(null);
    } catch (err) {
      setError('Error sending verification code. Please try again.');
      setMessage(null);
    }
  };

  const handleConfirmPassword = async () => {
    try {
      await confirmPassword(email, code, newPassword);
      setMessage('Password reset successfully.');
      setError(null);
      navigate('/login');
    } catch (err) {
      setError('Error resetting password. Please try again.');
      setMessage(null);
    }
  };

  return (
    <Container>
      <Row className="vh-100 d-flex justify-content-center align-items-center">
        <Col md={8} lg={6} xs={12}>
          <Card className="shadow">
            <Card.Body>
            <h2 className="mb-4">Forgot Password</h2>
              {message && <Alert variant="success">{message}</Alert>}
              {error && <Alert variant="danger">{error}</Alert>}
              {stage === 'request' ? (
                <Form>
                  <Form.Group controlId="formEmail">
                    <Form.Label>Email address</Form.Label>
                    <Form.Control
                      type="email"
                      placeholder="Enter email"
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
                    />
                  </Form.Group>
                  <Button variant="primary" onClick={handleForgotPassword} className="mt-3">
                    Send Verification Code
                  </Button>
                </Form>
              ) : (
                <Form>
                  <Form.Group controlId="formCode">
                    <Form.Label>Verification Code</Form.Label>
                    <Form.Control
                      type="text"
                      placeholder="Enter code"
                      value={code}
                      onChange={(e) => setCode(e.target.value)}
                    />
                  </Form.Group>
                  <Form.Group controlId="formNewPassword" className="mt-3">
                    <Form.Label>New Password</Form.Label>
                    <Form.Control
                      type="password"
                      placeholder="Enter new password"
                      value={newPassword}
                      onChange={(e) => setNewPassword(e.target.value)}
                    />
                  </Form.Group>
                  <Button variant="primary" onClick={handleConfirmPassword} className="mt-3">
                    Reset Password
                  </Button>
                </Form>
              )}
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </Container>
  );
};

export default ForgotPasswordComponent;
