import React, { useState } from 'react';
import { initKeycloak, keycloak } from '../App';

const ReportPage: React.FC = () => {

  const downloadReport = async () => {
    const response = await fetch(`http://localhost:8081/reports`, {
      credentials: 'include'
    });
  };

  return (
  <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
    <button
      onClick={async () => {
        initKeycloak();
        keycloak.login();
      }}
      className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
    >
      Login
    </button>
    <button
      onClick={downloadReport}
      className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
    >Download Report
    </button>
  </div>
  );
};

export default ReportPage;