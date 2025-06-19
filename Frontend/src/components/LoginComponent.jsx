import React, { useState } from 'react';
import { CognitoUser, AuthenticationDetails } from 'amazon-cognito-identity-js';
import UserPool from '../utils/UserPool';
import { useNavigate } from 'react-router-dom';
import 'bootstrap/dist/css/bootstrap.min.css';

const LoginComponent = () => {
  const [formData, setFormData] = useState({
    email: '',
    password: ''
  });
  const [error, setError] = useState(null);
  const navigate = useNavigate();

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData(prevState => ({
      ...prevState,
      [name]: value
    }));
    // Clear error when user starts typing
    setError(null);
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    const user = new CognitoUser({
      Username: formData.email,
      Pool: UserPool
    });
    const authDetails = new AuthenticationDetails({
      Username: formData.email,
      Password: formData.password
    });
    user.authenticateUser(authDetails, {
      onSuccess: (data) => {
        console.log('onSuccess:', data);

        // Extract and save the JWT token (idToken or accessToken)
        const token = data.getIdToken().getJwtToken();
        localStorage.setItem('userToken', token);

        navigate('/loginverify', { state: { email: formData.email, password: formData.password } });
      },
      onFailure: (err) => {
        console.error('onFailure:', err);
        setError(err.message || 'Login failed. Please check your credentials.');
      },
      newPasswordRequired: (data) => {
        console.log('newPasswordRequired:', data);
        // Handle new password required scenario
        navigate('/new-password', { state: { user, data } });
      }
    });
  };

  return (
    <div className="container-fluid login-background">
      <div className="row justify-content-center align-items-center vh-100">
        <div className="col-md-6 col-lg-4">
          <div className="card shadow-lg border-0 rounded-lg">
            <div className="card-header bg-primary text-white text-center">
              <h2 className="my-2">Quick Data Processor</h2>
            </div>
            <div className="card-body">
              <form onSubmit={handleSubmit}>
                {error && (
                  <div className="alert alert-danger" role="alert">
                    {error}
                  </div>
                )}
                <div className="form-group mb-3">
                  <label htmlFor="email" className="form-label">Email Address</label>
                  <input
                    type="email"
                    className="form-control"
                    id="email"
                    name="email"
                    value={formData.email}
                    onChange={handleChange}
                    placeholder="Enter email"
                    required
                  />
                </div>
                <div className="form-group mb-3">
                  <label htmlFor="password" className="form-label">Password</label>
                  <input
                    type="password"
                    className="form-control"
                    id="password"
                    name="password"
                    value={formData.password}
                    onChange={handleChange}
                    placeholder="Enter password"
                    required
                  />
                </div>
                <div className="d-grid">
                  <button 
                    type="submit" 
                    className="btn btn-primary btn-block"
                  >
                    Login
                  </button>
                </div>
                <div className="text-center mt-3">
                  <a href="/forgot-password" className="text-muted small">
                    Forgot Password?
                  </a>
                </div>
              </form>
            </div>
            <div className="card-footer text-center py-3 bg-light">
              <span className="text-muted small">
                New to Quick Data Processor? <a href="/signup">Sign Up</a>
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default LoginComponent;