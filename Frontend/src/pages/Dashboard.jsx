import React, { useState } from 'react';
import { Container } from 'react-bootstrap';
import Sidebar from '../components/Sidebar';
import Header from '../components/Header';
import ServiceShowcase from '../components/ServiceShowcase';
import { Outlet, useNavigate } from 'react-router-dom';

const Dashboard = () => {
    const [activeService, setActiveService] = useState(null);
    const navigate = useNavigate();

    const services = [
        {
            id: 1,
            name: 'JSON to CSV Converter',
            path: '/dashboard/service1',
            icon: 'FaFileCsv',
            description: 'Easily convert JSON data to CSV format with just a click.',
            benefits: ['Fast processing', 'User-friendly interface', 'High accuracy'],
        },
        {
            id: 2,
            name: 'Named Entity Extractor',
            path: '/dashboard/service2',
            icon: 'FaSearch',
            description: 'Extract named entities like people, organizations, and locations from text.',
            benefits: ['Advanced NLP processing', 'Detailed entity classification', 'Cloud-based'],
        },
        {
            id: 3,
            name: 'Word Cloud Generator',
            path: '/dashboard/service3',
            icon: 'FaCloud',
            description: 'Generate beautiful word clouds from your text data.',
            benefits: ['Customizable designs', 'High-speed generation', 'Download options'],
        },
    ];

    const handleServiceSelect = (service) => {
        setActiveService(service);
        navigate(service.path);
    };

    return (
        <div className="d-flex vh-100">
            <Sidebar
                services={services}
                activeService={activeService}
                onServiceSelect={handleServiceSelect}
            />
            <div className="flex-grow-1 d-flex flex-column">
                <Header title="Quick Data Processor" />
                <Container fluid className="flex-grow-1 overflow-auto p-4 bg-light">
                    {activeService === null ? (
                        <ServiceShowcase
                            services={services}
                            onServiceSelect={handleServiceSelect}
                        />
                    ) : (
                        <Outlet />
                    )}
                </Container>
            </div>
        </div>
    );
};

export default Dashboard;
