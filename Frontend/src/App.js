import { BrowserRouter as Router, Route, Routes } from 'react-router-dom';
import { Toaster } from 'react-hot-toast';
import Home from './pages/Home';
import Login from './pages/Login';
import SignUp from './pages/SignUp';
import ForgotPassword from './pages/ForgotPassword';
import LoginVerify from './pages/LoginVerify';
import VerifyEmail from './pages/VerifyEmail';
import SecurityQuestions from './pages/SecurityQuestions';
import DummyBot from './pages/DummyBot';
import Dashboard from './pages/Dashboard';
import Service1 from './components/services/Service1';
import Service2 from './components/services/Service2';
import Service3 from './components/services/Service3';
import Service4 from './components/services/Service4';
import Analytics from './pages/Analytics';
import FeedbackTable from './pages/FeedbackTable';
import { Auth } from './context/authContext';
import Protected from './utils/Protected';
import ServiceShowcase from "./components/ServiceShowcase";

function App() {
    return (
        <Auth>
            <Router>
                <Routes>
                    <Route path="/" element={<Home />} />
                    <Route path="/login" element={<Login />} />
                    <Route path="/signup" element={<SignUp />} />
                    <Route path="/forgot-password" element={<ForgotPassword />} />
                    <Route path="/loginverify" element={<LoginVerify />} />
                    <Route path="/verifyemail" element={<VerifyEmail />} />
                    <Route path="/securityquestions" element={<SecurityQuestions />} />
                    <Route path="/dummybot" element={<DummyBot />} />
                    <Route path="/analytics" element={<Analytics />} />
                    <Route path="/feedbacktable" element={<FeedbackTable />} />
                    <Route path="/dashboard" element={<Dashboard />}>
                        <Route
                            path="service1"
                            element={<Protected><Service1 service={{ name: 'JSON to CSV Converter' }} /></Protected>}
                        />
                        <Route
                            path="service2"
                            element={<Protected><Service2 service={{ name: 'Named Entity Extractor' }} /></Protected>}
                        />
                        <Route
                            path="service3"
                            element={<Service3 service={{ name: 'Word Cloud Generator' }} />}
                        />
                        <Route
                            path="service4"
                            element={<Service4 service={{ name: 'Chat' }} />}
                        />
                    </Route>
                </Routes>
                <Toaster />
            </Router>
        </Auth>
    );
}

export default App;
