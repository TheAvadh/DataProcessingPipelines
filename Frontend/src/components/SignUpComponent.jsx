import React, { useState } from 'react';
import UserPool from '../utils/UserPool';
import toast from 'react-hot-toast';
import OTPComponent from './OTPComponent';
import VerifyEmailComponent from './VerifyEmailComponent';

const SignUpComponent = () => {
    const [formData, setFormData] = useState({
        name: "",
        email: "",
        password: "",
        confirmpassword: ""
    });
    const [otp, setOtp] = useState(false);

    // Function to validate email format
    const validateEmail = (email) => {
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        return emailRegex.test(email);
    };

    const handleSubmit = (e) => {
        e.preventDefault();

        // Check if email format is valid
        if (!validateEmail(formData.email)) {
            toast.error("Please enter a valid email address.");
            return;
        }

        // Proceed with sign-up if email is valid
        UserPool.signUp(formData.email, formData.password, [], [], (err, data) => {
            if (err) {
                console.error(err);
                toast.error("Sign-up failed. Please try again.");
            } else {
                toast.success("User created successfully");
                setOtp(true);
            }
        });
    };

    return (
        <div>
            {!otp ? (
                <div className="container-fluid signup-background">
                    <div className="row justify-content-center align-items-center vh-100">
                        <div className="col-md-6 col-lg-4">
                            <div className="card shadow-lg border-0 rounded-lg">
                                <div className="card-header bg-primary text-white text-center">
                                    <h2 className="my-2">Quick Data Processor - Sign Up</h2>
                                </div>
                                <div className="card-body">
                                    {!otp ? (
                                        <form onSubmit={handleSubmit}>
                                            <div className="form-group mb-3">
                                                <label htmlFor="name" className="form-label">Name</label>
                                                <input
                                                    type="text"
                                                    className="form-control"
                                                    name="name"
                                                    value={formData.name}
                                                    onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                                                    placeholder="Enter name"
                                                    required
                                                />
                                            </div>
                                            <div className="form-group mb-3">
                                                <label htmlFor="email" className="form-label">Email Address</label>
                                                <input
                                                    type="email"
                                                    className="form-control"
                                                    name="email"
                                                    value={formData.email}
                                                    onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                                                    placeholder="Enter email"
                                                    required
                                                />
                                            </div>
                                            <div className="form-group mb-3">
                                                <label htmlFor="password" className="form-label">Password</label>
                                                <input
                                                    type="password"
                                                    className="form-control"
                                                    name="password"
                                                    value={formData.password}
                                                    onChange={(e) => setFormData({ ...formData, password: e.target.value })}
                                                    placeholder="Enter password"
                                                    required
                                                />
                                            </div>
                                            <div className="form-group mb-3">
                                                <label htmlFor="confirmpassword" className="form-label">Confirm Password</label>
                                                <input
                                                    type="password"
                                                    className="form-control"
                                                    name="confirmpassword"
                                                    value={formData.confirmpassword}
                                                    onChange={(e) => setFormData({ ...formData, confirmpassword: e.target.value })}
                                                    placeholder="Confirm password"
                                                    required
                                                />
                                            </div>
                                            <div className="d-grid">
                                                <button type="submit" className="btn btn-primary btn-block">
                                                    Sign Up
                                                </button>
                                            </div>
                                        </form>
                                    ) : (
                                        <OTPComponent 
                                            email={formData.email} 
                                            password={formData.password} 
                                            name={formData.name} 
                                        />
                                    )}
                                </div>
                                <div className="card-footer text-center py-3 bg-light">
                                    <span className="text-muted small">
                                        Already have an account? <a href="/login">Log In</a>
                                    </span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            ) : (
                <div>
                    <OTPComponent email={formData.email} password={formData.password} name={formData.name} />
                </div>
            )}
        </div>
    );
}

export default SignUpComponent;
