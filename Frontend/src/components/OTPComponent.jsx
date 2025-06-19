import { AuthenticationDetails, CognitoUser } from 'amazon-cognito-identity-js';
import React from 'react';
import UserPool from '../utils/UserPool';
import { useNavigate } from 'react-router-dom';
import toast from 'react-hot-toast';
import 'bootstrap/dist/css/bootstrap.min.css';

const OTPComponent = (data) => {
    const [formData, setFormData] = React.useState({
        otp: ""
    });
    const navigate = useNavigate();
    const email = data.email;
    const name = data.name;

    const handleSubmit = (e) => {
        e.preventDefault();
        console.log(formData.otp, data.email, data.password, data.name);

        const user = new CognitoUser({
            Username: data.email,
            Pool: UserPool,
        });

        // Confirm registration with OTP
        user.confirmRegistration(formData.otp, true, (err, result) => {
            if (err) {
                console.error('OTP confirmation failed:', err);
                toast.error("Invalid OTP. Please try again.");
                return;
            }
            console.log('OTP confirmed:', result);

            const authDetails = new AuthenticationDetails({
                Username: data.email,
                Password: data.password
            });

            // Authenticate user after OTP confirmation
            user.authenticateUser(authDetails, {
                onSuccess: (data) => {
                    console.log('Authentication successful:', data);
                    toast.success("OTP verified successfully");
                    navigate('/securityquestions', { state: { email: email, name: name } });
                },
                onFailure: (err) => {
                    console.error('Authentication failed:', err);
                    toast.error("Authentication failed. Please check your credentials.");
                },
                newPasswordRequired: (data) => {
                    console.log('New password required:', data);
                }
            });
        });
    };

    return (
        <div className="container-fluid otp-background d-flex align-items-center vh-100">
            <div className="row justify-content-center w-100">
                <div className="col-md-4">
                    <div className="card shadow-lg border-0 rounded-lg">
                        <div className="card-header bg-primary text-white text-center">
                            <h2 className="my-2">OTP Verification</h2>
                        </div>
                        <div className="card-body">
                            <form onSubmit={handleSubmit}>
                                <div className="form-group mb-3">
                                    <label htmlFor="otp" className="form-label fw-bold">Enter OTP</label>
                                    <input
                                        type="text"
                                        id="otp"
                                        className="form-control"
                                        value={formData.otp}
                                        onChange={(e) => setFormData({ ...formData, otp: e.target.value })}
                                        placeholder="Enter OTP"
                                        required
                                    />
                                </div>
                                <div className="d-grid">
                                    <button type="submit" className="btn btn-primary">
                                        Submit
                                    </button>
                                </div>
                            </form>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default OTPComponent;
