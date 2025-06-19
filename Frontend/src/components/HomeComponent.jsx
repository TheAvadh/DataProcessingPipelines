import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Button, Card, Alert } from 'react-bootstrap';
import 'bootstrap/dist/css/bootstrap.min.css';
import { useAuth } from '../context/authContext';
import { useNavigate } from 'react-router-dom';


const HomeComponent = () => {
  const [popup, setPopup] = useState({ show: false, text: '' });
  const { status, logout, setStatus, getSession } = useAuth();
  const [guestEmail, setGuestEmail] = useState('');
  const [guestPopup, setGuestPopup] = useState(false);
  const navigate = useNavigate();
  const handleLogout = () => {
    console.log("Logging out");
    logout();
    navigate('/');
  }

  useEffect(() => {
    getSession();
    // Check if Dialogflow Messenger is already loaded
    if (!window.dfMessengerLoaded) {
      // Mark as loaded to prevent further loads
      window.dfMessengerLoaded = true;

      // Load Dialogflow Messenger styles
      const link = document.createElement('link');
      link.rel = 'stylesheet';
      link.href = 'https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/themes/df-messenger-default.css';
      document.head.appendChild(link);

      // Load Dialogflow Messenger script
      const script = document.createElement('script');
      script.src = 'https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/df-messenger.js';
      script.async = true;
      script.crossOrigin = 'anonymous';

      script.onload = () => {
        // Create the df-messenger component once the script is loaded
        const dfMessenger = document.createElement('df-messenger');
        dfMessenger.setAttribute('location', 'us-central1');
        dfMessenger.setAttribute('project-id', 'quickdataprocessor');
        dfMessenger.setAttribute('agent-id', '6d2bd33a-2fd5-40af-beb2-d1b92f9ee4cc'); // Replace with your actual agent ID
        dfMessenger.setAttribute('language-code', 'en');
        
        // Apply some custom styling via attributes
        dfMessenger.setAttribute('chat-icon', 'https://your-logo-url.com/logo.png'); // Optional, replace with your icon URL
        dfMessenger.setAttribute('allow', 'microphone'); // Optional for voice queries

        // Append df-messenger to the body
        document.body.appendChild(dfMessenger);
      };

      document.body.appendChild(script);
    }
  }, []);

  const features = [
    {
      title: "JSON to CSV Conversion",
      text: "Automatically convert JSON files to CSV format using AWS Glue.",
      iconClass: "fas fa-file-code",
      tech: "AWS Glue | DynamoDB",
      route: "/dashboard/service1"
    },
    {
      title: "Named Entity Recognition",
      text: "Extract named entities from text files using AWS Lambda.",
      iconClass: "fas fa-project-diagram",
      tech: "AWS Lambda | DynamoDB",
      route: "/dashboard/service2"
    },
    {
      title: "Word Cloud Generation",
      text: "Generate interactive word clouds from text files using GCP Looker Studio for powerful visualization.",
      iconClass: "fas fa-cloud",
      tech: "GCP Looker Studio",
      route: "/dashboard/service3"
    },
    {
      title: "Chat",
      text: "Chat with our QDP Agent.",
      iconClass: "fas fa-comment-dots",
      tech: "GCP Pub/Sub",
      route: "/dashboard/service4"
    }
  ];

  const handleGuestPopup = () => {
    localStorage.setItem('userEmail', guestEmail);
    setGuestPopup(false);
    navigate('/dashboard');
  }

  const handleMouseEnter = (text) => {
    setPopup({ show: true, text });
  };

  const handleMouseLeave = () => {
    setPopup({ show: false, text: '' });
  };

  return (
    <Container fluid className="homepage-container vh-100 d-flex flex-column">
      <Row className="flex-grow-1 h-100">
        {/* Left Side - Main Content */}
        <Col md={8} className="bg-light d-flex flex-column align-items-center justify-content-center">
          <div className="text-center">
            <h1 className="display-4 mb-4 text-primary">Quick Data Processor</h1>
            <p className="lead mb-5 text-muted">
              Transform your data with powerful cloud-based processing tools
            </p>

            <Row className="features-grid justify-content-center">
              {features.map((feature, index) => (
                <Col md={4} className="mb-4" key={index}>
                  <Card
                    className="h-100 border-0 shadow-sm"
                    style={{ cursor: 'pointer', transition: 'transform 0.2s, box-shadow 0.2s' }}
                    onMouseEnter={() => handleMouseEnter(feature.text)}
                    onMouseLeave={handleMouseLeave}
                    onMouseOver={(e) => {
                      e.currentTarget.style.transform = 'scale(1.05)';
                      e.currentTarget.style.boxShadow = '0 4px 15px rgba(0, 0, 0, 0.2)';
                    }}
                    onMouseOut={(e) => {
                      e.currentTarget.style.transform = 'scale(1)';
                      e.currentTarget.style.boxShadow = '0 2px 5px rgba(0, 0, 0, 0.1)';
                    }}
                    onClick={() => navigate(feature.route)} 
                  >
                    <Card.Body className="text-center">
                      <i className={`${feature.iconClass} text-primary mb-3`} style={{ fontSize: '3rem' }}></i>
                      <Card.Title>{feature.title}</Card.Title>
                      <Card.Text>{feature.text}</Card.Text>
                      <div className="text-muted small mt-2">
                        <i className="fab fa-aws me-1"></i>{feature.tech}
                      </div>
                    </Card.Body>
                  </Card>
                </Col>
              ))}
            </Row>

            {/* Why Choose Us Section */}
            <section className="why-choose-us my-5">
              <h2 className="text-primary mb-4">Why Choose Us?</h2>
              <Row>
                <Col md={4} className="mb-3">
                  <Card className="border-0 shadow-sm">
                    <Card.Body className="text-center">
                      <i className="fas fa-rocket text-primary mb-3" style={{ fontSize: '2.5rem' }}></i>
                      <Card.Title>Fast Processing</Card.Title>
                      <Card.Text>
                        Experience rapid data transformation with our optimized cloud-based infrastructure.
                      </Card.Text>
                    </Card.Body>
                  </Card>
                </Col>
                <Col md={4} className="mb-3">
                  <Card className="border-0 shadow-sm">
                    <Card.Body className="text-center">
                      <i className="fas fa-lock text-primary mb-3" style={{ fontSize: '2.5rem' }}></i>
                      <Card.Title>Secure Storage</Card.Title>
                      <Card.Text>
                        We prioritize your data security with top-tier encryption and secure storage solutions.
                      </Card.Text>
                    </Card.Body>
                  </Card>
                </Col>
                <Col md={4} className="mb-3">
                  <Card className="border-0 shadow-sm">
                    <Card.Body className="text-center">
                      <i className="fas fa-thumbs-up text-primary mb-3" style={{ fontSize: '2.5rem' }}></i>
                      <Card.Title>Easy to Use</Card.Title>
                      <Card.Text>
                        A user-friendly interface designed to make data processing accessible for everyone.
                      </Card.Text>
                    </Card.Body>
                  </Card>
                </Col>
              </Row>
            </section>
          </div>
        </Col>

        {/* Right Side - Login/Signup */}
        <Col md={4} className="bg-white d-flex align-items-center justify-content-center">
          <div className="text-center w-75">
            <div className="mb-5">
              <img 
                src="/images/logo.png" 
                alt="Quick Data Processor Logo" 
                className="mb-4"
                style={{ maxWidth: '150px', borderRadius: '50%' }}
              />
              <h2 className="mb-4">Welcome to Quick Data Processor</h2>
            </div>
            {
              !status ? (
                <div className="d-grid gap-2">
                  <Button 
                variant="primary" 
                size="lg" 
                className="w-100"
                onClick={() => window.location.href = '/login'}
              >
                Login
              </Button>
              
              <Button 
                variant="outline-primary" 
                size="lg" 
                className="w-100"
                onClick={() => window.location.href = '/signup'}
              >
                Sign Up
              </Button>
              <Button 
                variant="outline-primary" 
                size="lg" 
                className="w-100"
                onClick={() => setGuestPopup(true)}
              >
                Enter as guest
              </Button>
             </div>
            ) : (
              <>
                <div className="d-grid gap-2">
                  <Button 
                    variant="primary" 
                    size="lg" 
                    className="w-100"
                    onClick={handleLogout}
                  >
                    Logout
                  </Button>
                </div>
              </>
            )}
            
            {guestPopup && (
              <div className="position-fixed top-0 start-0 w-100 h-100 d-flex align-items-center justify-content-center" style={{backgroundColor: 'rgba(0,0,0,0.5)', backdropFilter: 'blur(5px)', zIndex: 1000}}>
                <div className="card shadow-lg" style={{maxWidth: '400px', width: '90%'}}>
                  <div className="card-header bg-primary text-white">
                    <h5 className="card-title mb-0">Guest Login</h5>
                  </div>
                  <div className="card-body p-4">
                    <div className="form-floating mb-3">
                      <input 
                        type="email" 
                        className="form-control" 
                        id="guestEmail"
                        placeholder="name@example.com"
                        value={guestEmail}
                        onChange={(e) => setGuestEmail(e.target.value)}
                      />
                      <label htmlFor="guestEmail">Email address</label>
                    </div>
                    <div className="d-grid">
                      <Button 
                        variant="primary" 
                        size="lg"
                        onClick={handleGuestPopup}
                      >
                        Continue as Guest
                      </Button>
                    </div>
                  </div>
                </div>
              </div>
            )}
            <div className="mt-4 text-muted small">
              <p>
                By continuing, you agree to our 
                <a href="#" className="ms-1 text-primary">Terms of Service</a>
              </p>
            </div>
          </div>
        </Col>
      </Row>

      {/* Footer */}
      <footer className="bg-dark text-light text-center py-3 mt-auto">
        <Container>
          <Row>
            <Col>
              <p className="mb-1">&copy; {new Date().getFullYear()} Quick Data Processor. All rights reserved.</p>
              <p className="mb-0">
                <a href="/privacy-policy" className="text-light me-3">Privacy Policy</a>
                <a href="/terms-of-service" className="text-light">Terms of Service</a>
              </p>
            </Col>
          </Row>
        </Container>
      </footer>
      
    </Container>

    
  );
};

export default HomeComponent;
