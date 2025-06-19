import React from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';

const AnalyticsComponent = () => {
    return (
        <div className="container-fluid d-flex justify-content-center align-items-center vh-100 vw-100 p-0 m-0">
            <iframe
                src="https://lookerstudio.google.com/embed/reporting/2ba4a3ab-1ec2-42f7-9df0-84e5aad2a970/page/dl9VE"
                style={{
                    border: 0,
                    width: '100%',
                    height: '100%',
                    overflow: 'hidden',
                }}
                sandbox="allow-storage-access-by-user-activation allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox"
            ></iframe>
        </div>
    );
};

export default AnalyticsComponent;

