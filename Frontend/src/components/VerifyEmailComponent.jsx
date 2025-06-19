import React, { useState, useEffect } from 'react';
import { Col, Button, Row, Container, Card, Alert, Form } from "react-bootstrap";
import { useNavigate, useLocation } from 'react-router-dom';
import { CognitoUser } from 'amazon-cognito-identity-js';
import UserPool from '../utils/UserPool';
import { useAuth } from '../context/authContext';
import toast from 'react-hot-toast';

const VerifyEmailComponent = () => {
    const navigate = useNavigate();
    const [message, setMessage] = useState(null);
    const [otp, setOtp] = useState('');
    const location = useLocation();
    const email = location.state?.email || '';
    const password = location.state?.password || '';
    const { authenticate, setStatus } = useAuth();

    useEffect(() => {
        if (!email || !password) {
            setMessage("No user found. Please sign up or log in.");
            toast.error("No user found. Please sign up or log in.");
        }
    }, [email, password]);

    const resendOTP = () => {
        if (!email) {
            setMessage("Email is missing. Please sign up again.");
            toast.error("Email is missing. Please sign up again.");
            return;
        }

        setMessage("Sending OTP...");
        const cognitoUser = new CognitoUser({ Username: email, Pool: UserPool });
        
        cognitoUser.resendConfirmationCode((err, result) => {
            if (err) {
                console.error(err);
                setMessage("Failed to resend OTP. Please try again.");
                toast.error("Failed to resend OTP. Please try again.");
            } else {
                console.log(result);
                setMessage("OTP has been resent to your email.");
                toast.success("OTP has been resent to your email.");
            }
        });
    };

    const verifyOTP = async () => {
        if (!email || !otp) {
            setMessage("Email or OTP is missing. Please try again.");
            toast.error("Email or OTP is missing. Please try again.");
            return;
        }

        setMessage("Verifying OTP...");
        const cognitoUser = new CognitoUser({ Username: email, Pool: UserPool });
        
        try {
            await new Promise((resolve, reject) => {
                cognitoUser.confirmRegistration(otp, true, (err, result) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(result);
                    }
                });
            });

            await authenticate(email, password);
            setStatus(true);
            setMessage("Email Verified. You are now logged in.");
            toast.success("Email Verified. You are now logged in.");
            navigate('/securityquestions', { state: { email, password } });
            
        } catch (err) {
            console.error(err);
            setMessage("Invalid OTP. Please try again.");
            toast.error("Invalid OTP. Please try again.");
        }
    };

    return (
        <Container>
            <Row className="vh-100 d-flex justify-content-center align-items-center">
                <Col md={8} lg={6} xs={12}>
                    <Card className="shadow">
                        <Card.Body>
                            <h2 className="fw-bold mb-2 text-uppercase">Verify Your Email</h2>
                            <p>Please enter the OTP sent to your email to verify your account.</p>
                            {message && <Alert variant={message.includes("Failed") || message.includes("Invalid") ? "danger" : "success"}>{message}</Alert>}
                            
                            <Form.Group className="mb-3">
                                <Form.Label>Enter OTP</Form.Label>
                                <Form.Control
                                    type="text"
                                    placeholder="Enter OTP"
                                    value={otp}
                                    onChange={(e) => setOtp(e.target.value)}
                                />
                            </Form.Group>
                            
                            <Button variant="primary" onClick={resendOTP} className="me-2">Resend OTP</Button>
                            <Button 
                                variant="secondary" 
                                onClick={verifyOTP}
                            >
                                Verify OTP
                            </Button>
                        </Card.Body>
                    </Card>
                </Col>
            </Row>
        </Container>
    );
};

export default VerifyEmailComponent;
