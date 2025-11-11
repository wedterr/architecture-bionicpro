import React from 'react';
import { ReactKeycloakProvider } from '@react-keycloak/web';
import Keycloak, { KeycloakConfig } from 'keycloak-js';
import ReportPage from './components/ReportPage';

const keycloakConfig: KeycloakConfig = {
  url: process.env.REACT_APP_KEYCLOAK_URL,
  realm: process.env.REACT_APP_KEYCLOAK_REALM||"",
  clientId: process.env.REACT_APP_KEYCLOAK_CLIENT_ID||"",

};

export const keycloak = new Keycloak(keycloakConfig);
export function initKeycloak() {
  keycloak.init({
    onLoad: "check-sso",
    pkceMethod: "S256",
    enableLogging: true,
    checkLoginIframe: false,
    flow: 'standard'
  });
}

const App: React.FC = () => {
  return (
    <ReactKeycloakProvider authClient={keycloak}>
      <div className="App">
        <ReportPage />
      </div>
    </ReactKeycloakProvider>
  );
};

export default App;