import React, { useContext, useEffect, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { getUser, checkAnswer, getOneQuestion, getMathSkill, verifyMathSkill } from '../apis/AuthApis';
import { useAuth } from '../context/authContext';
import 'bootstrap/dist/css/bootstrap.min.css';
import { Container, Row, Col, Card, Button, Form } from 'react-bootstrap';
import { ToastContainer, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

const LoginVerifyComponent = () => {
    const { setRole, setStatus, authenticate } = useAuth();
    const [question, setQuestion] = useState('');
    const [userDetails, setUserDetails] = useState('');
    const [answer, setAnswer] = useState('');
    const [mathQuestion, setMathQuestion] = useState('');
    const [mathAnswer, setMathAnswer] = useState('');
    const location = useLocation();
    const email = location.state?.email || '';
    const password = location.state?.password || '';
    const navigate = useNavigate();

    useEffect(() => {
        getUserDetails();
        generateMathCaptcha();
    }, []);

    const getUserDetails = async () => { 
        try {
            const response = await getUser(email);
            const queId = response.data.queId;
            const role = response.data.role;
            setUserDetails(role);
            const data = await getOneQuestion(queId);
            setQuestion(data.data.question);
        } catch (error) {
            console.error('Failed to fetch user details:', error);
        }
    };

    const generateMathCaptcha = async () => {
        const operator = ['+', '-', '*'];
        const num1 = Math.floor(Math.random() * 10);
        const num2 = Math.floor(Math.random() * 10);
        const op = operator[Math.floor(Math.random() * operator.length)];
        setMathQuestion(`${num1} ${op} ${num2}`);
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        try {
            // Check the security question answer first
            const response = await checkAnswer({
                email,
                ans: answer,
            });

            if (response.data.statusCode === 200) {
                // Now verify the math skill answer
                const data = { 
                    captchaString: mathQuestion,
                    userAnswer: mathAnswer 
                }
                const mathResponse = await verifyMathSkill(data); 

                if (mathResponse.data.body.isCorrect) {
                    await authenticate(email, password);
                    setRole(userDetails);
                    setStatus(true);
                    navigate('/dashboard');
                    toast.success('Login successful!');  // Success toast notification
                } else {
                    toast.error('Math skill answer is incorrect');
                }
            } else {
                toast.error('Login failed due to incorrect security question answer');
            }
        } catch (error) {
            console.error("Login verification failed:", error);
            toast.error('Login failed');
        }
    };

    return (
        <Container fluid className="login-verify-background vh-100 d-flex align-items-center">
            <ToastContainer 
                position="top-right"
                autoClose={3000}
                hideProgressBar={false}
                newestOnTop={false}
                closeOnClick
                rtl={false}
                pauseOnFocusLoss
                draggable
                pauseOnHover
            />
            <Row className="w-100 justify-content-center">
                <Col md={6} lg={4}>
                    <Card className="shadow-lg border-0 rounded-lg">
                        <Card.Header className="bg-primary text-white text-center">
                            <h2 className="my-2">Login Verification</h2>
                        </Card.Header>
                        <Card.Body>
                            <Form onSubmit={handleSubmit}>
                                <Form.Group className="mb-3" controlId="question">
                                    <Form.Label className="fw-bold">Security Question</Form.Label>
                                    <p className="text-muted mb-2">{question}</p>
                                    <Form.Control
                                        type="text"
                                        placeholder="Enter your answer"
                                        value={answer}
                                        onChange={(e) => setAnswer(e.target.value)}
                                        required
                                        className="form-control"
                                    />
                                </Form.Group>
                                
                                {/* Math Skills Section */}
                                <Form.Group className="mb-3 math-skill-section" controlId="mathSkill">
                                    <Form.Label className="fw-bold">Math Skill Question</Form.Label>
                                    <p className="math-question text-muted mb-2">{mathQuestion}</p>
                                    <Form.Control
                                        type="text"
                                        placeholder="Enter your answer"
                                        value={mathAnswer}
                                        onChange={(e) => setMathAnswer(e.target.value)}
                                        required
                                        className="form-control"
                                    />
                                </Form.Group>

                                <div className="d-grid">
                                    <Button 
                                        variant="primary" 
                                        type="submit" 
                                        className="btn-block"
                                    >
                                        Verify and Login
                                    </Button>
                                </div>
                            </Form>
                        </Card.Body>
                        <Card.Footer className="text-center py-3 bg-light">
                            <span className="text-muted small">
                                Need help? <a href="#" className="text-primary">Contact Support</a>
                            </span>
                        </Card.Footer>
                    </Card>
                </Col>
            </Row>
        </Container>
    );
};

export default LoginVerifyComponent;
