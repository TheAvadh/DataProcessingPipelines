import React from 'react';
import { Navbar, Button } from 'react-bootstrap';
import { useNavigate } from 'react-router-dom';

const Header = ({ title }) => {
    const navigate = useNavigate();

    const handleLogout = () => {
        localStorage.clear();
        sessionStorage.clear();

        navigate('/');
    };

    return (
        <Navbar bg="primary" variant="dark" className="px-4 py-2">
            <Navbar.Brand className="fw-bold" style={{ cursor: 'pointer' }} onClick={() => navigate('/dashboard')}>
                {title}
            </Navbar.Brand>
            <Navbar.Toggle />
            <Navbar.Collapse className="justify-content-end">
                <Button variant="outline-light" onClick={handleLogout}>
                    Logout
                </Button>
            </Navbar.Collapse>
        </Navbar>
    );
};

export default Header;
