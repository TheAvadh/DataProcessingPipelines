import React, { useState, useEffect } from 'react';
import { Container, Card, Row, Col, Button, Spinner, Form } from 'react-bootstrap';
import 'bootstrap/dist/css/bootstrap.min.css';

const Service4 = () => {
    const [queries, setQueries] = useState([]); 
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [chats, setChats] = useState([]); 
    const [layout, setLayout] = useState("user"); 

    const userEmail = localStorage.getItem("userEmail");

    useEffect(() => {
        // Check if the user is admin
        if (userEmail === "bhavyadave500@gmail.com") {
            setLayout("admin");
            fetchUnansweredQueries();
        } else {
            setLayout("user");
            fetchChats();
        }
    }, [userEmail]);

    

    const fetchUnansweredQueries = async () => {
        try {
            
            const response = await fetch(
                "https://us-central1-serverless-442917.cloudfunctions.net/unanswered_queries",
                {
                    method: "GET", 
                    // headers: {
                    //     "Content-Type": "application/json",
                    //     "Access-Control-Allow-Origin": "*",
                    //     "Access-Control-Allow-Methods": "*",
                    //     "Access-Control-Allow-Headers": "Content-Type, Authorization",
                    // },
                }
            );
            if (!response.ok) {
                throw new Error(`HTTP error! Status: ${response.status}`);
            }
            const data = await response.json();
            setQueries(data); // Update state with the fetched queries
        } catch (err) {
            console.error("Error fetching unanswered queries:", err);
            setError("Failed to fetch unanswered queries. Please try again later.");
        } finally {
            setLoading(false); // Set loading to false after the request is complete
        }
    };

    

    // Fetch user-specific chats
    const fetchChats = async () => {
        try {
            const response = await fetch(
                "https://us-central1-serverless-442917.cloudfunctions.net/retrieve_chats",
                {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json"
                        // "Access-Control-Allow-Origin": "*",
                        // "Access-Control-Allow-Methods": "*",
                        // "Access-Control-Allow-Headers": "Content-Type, Authorization",
                    },
                    body: JSON.stringify({ email: userEmail }),
                }
            );
    
            // Check for successful response
            if (!response.ok) {
                const errorDetails = await response.text(); // Get the error body (if available)
                console.error("Error response:", errorDetails);
                throw new Error(`HTTP error! Status: ${response.status} - ${errorDetails}`);
            }
    
            // Parse the response data
            const data = await response.json();
            setChats(data); // Update state with chats data
    
        } catch (err) {
            console.error("Error fetching chats:", err);
    
            // Check if it's a network error or HTTP error
            if (err.message.includes("Failed to fetch")) {
                setError("Network error. Please check your connection and try again.");
            } else {
                setError("Failed to retrieve chats. Please try again later.");
            }
        } finally {
            setLoading(false); // Set loading to false when the request is done
        }
    };
    

    // Handle response submission for admin
    const handleSubmit = async (email, query, responseText) => {
        try {
            const response = await fetch(
                "https://us-central1-serverless-442917.cloudfunctions.net/agent_response",
                {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json"
                        
                    },
                    body: JSON.stringify({ email, query, response: responseText }),
                }
                
            );
            if (!response.ok) {
                throw new Error(`HTTP error! Status: ${response.status}`);
            }
            const result = await response.json();
            alert(`Response submitted successfully for query: "${query}"`);
            fetchUnansweredQueries();
        } catch (err) {
            console.error("Error submitting response:", err);
            alert("Failed to submit the response. Please try again.");
        }
    };

    if (loading) {
        return (
            <div className="d-flex justify-content-center align-items-center vh-100">
                <Spinner animation="border" />
            </div>
        );
    }

    if (error) {
        return (
            <Container className="vh-100 d-flex flex-column justify-content-center align-items-center">
                <h3 className="text-danger">Error</h3>
                <p>{error}</p>
            </Container>
        );
    }

    // Admin Layout
    if (layout === "admin") {
        return (
            <Container className="py-5">
                <h1 className="text-center mb-4">Answer the Queries</h1>
                {queries && queries.length > 0 ? (
                    queries.map((queryObj, index) => (
                        <Card key={index} className="mb-4">
                            <Card.Body>
                                <h5>Query from {queryObj.email}</h5>
                                <p>{queryObj.query}</p>
                                <Form
                                    onSubmit={(e) => {
                                        e.preventDefault();
                                        const responseText = e.target.elements[`response-${index}`].value;
                                        handleSubmit(queryObj.email, queryObj.query, responseText);
                                    }}
                                >
                                    <Form.Group controlId={`response-${index}`}>
                                        <Form.Label>Response</Form.Label>
                                        <Form.Control
                                            type="text"
                                            placeholder="Enter your response here"
                                            required
                                        />
                                    </Form.Group>
                                    <Button variant="primary" type="submit" className="mt-3">
                                        Submit
                                    </Button>
                                </Form>
                            </Card.Body>
                        </Card>
                    ))
                ) : (
                    <p className="text-center text-muted">No queries are available at the moment.</p>
                )}
            </Container>
        );
    }
    // User Layout
    return (
        <Container className="py-5">
            <h1 className="text-center mb-4">Chat</h1>
            <Card className="shadow">
                <Card.Body>
                    {chats.length > 0 ? (
                        <Row>
                            {chats.map((chat, index) => (
                                <Col key={index} md={12} className="mb-3">
                                    <div className="p-3 rounded shadow-sm bg-light">
                                        <strong>Query:</strong> {chat.query}
                                    </div>
                                    <div className="p-3 rounded shadow-sm bg-white mt-2">
                                        <strong>Response:</strong> {chat.response}
                                    </div>
                                </Col>
                            ))}
                        </Row>
                    ) : (
                        <p className="text-center text-muted">No chats found.</p>
                    )}
                </Card.Body>
            </Card>
        </Container>
    );
};

export default Service4;
