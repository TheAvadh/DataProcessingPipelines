import React from 'react'
import { Navigate } from 'react-router-dom'
import { useAuth } from '../context/authContext';
import { useEffect } from 'react';

const Protected = ({children}) => {
    const { getSession, status, loading } = useAuth();
    useEffect(() => {
        getSession();
    }, []);

    if (loading) {
        return <div className='d-flex justify-content-center align-items-center vh-100'>Loading...</div>
    }

    return status ? children : <Navigate to="/login" />
}

export default Protected
