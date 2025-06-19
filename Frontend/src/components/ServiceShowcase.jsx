import React from 'react';
import { Row, Col, Card, Button } from 'react-bootstrap';
import * as Icons from 'react-icons/fa';

const ServiceShowcase = ({ services, onServiceSelect }) => {
    return (
        <div>
            <h1 className="mb-4 text-center">Quick Data Processing Services</h1>
            <Row>
                {services.map(service => {
                    const ServiceIcon = Icons[service.icon];
                    return (
                        <Col md={4} key={service.id} className="mb-4">
                            <Card className="h-100 shadow-sm">
                                <Card.Body className="text-center">
                                    <ServiceIcon size={50} className="mb-3 text-primary" />
                                    <Card.Title>{service.name}</Card.Title>
                                    <Card.Text>{service.description}</Card.Text>
                                    <div>
                                        <h6>Key Benefits:</h6>
                                        <ul className="list-unstyled">
                                            {service.benefits.map((benefit, index) => (
                                                <li key={index} className="mb-2">
                                                    <Icons.FaCheck className="text-success me-2" />
                                                    {benefit}
                                                </li>
                                            ))}
                                        </ul>
                                    </div>
                                    <Button
                                        variant="outline-primary"
                                        onClick={() => onServiceSelect(service)}
                                    >
                                        Explore Service
                                    </Button>
                                </Card.Body>
                            </Card>
                        </Col>
                    );
                })}
            </Row>
        </div>
    );
};

export default ServiceShowcase;