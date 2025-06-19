import React, { createContext, useState, useContext } from "react";
import { CognitoUser, AuthenticationDetails } from "amazon-cognito-identity-js";
import UserPool from "../utils/UserPool";

const AuthContext = createContext();

const Auth = ({ children }) => {
  const [role, setRole] = useState("");
  const [status, setStatus] = useState(false);
  const [numberOfFiles, setNumberOfFiles] = useState(0);
  const [guestEmail, setGuestEmail] = useState("");
  const [email, setEmail] = useState("");
  

  const authenticate = async (email, password) => {
    return new Promise((resolve, reject) => {
      const user = new CognitoUser({
        Username: email,
        Pool: UserPool,
      });

      const authDetails = new AuthenticationDetails({
        Username: email,
        Password: password,
      });

      user.authenticateUser(authDetails, {
        onSuccess: (data) => {
          console.log("onSuccess:", data);
          localStorage.setItem('userEmail', email);
          resolve();
        },
        onFailure: (err) => {
          console.error("onFailure:", err);
          reject(err);
        },
        newPasswordRequired: (data) => {
          console.log("newPasswordRequired:", data);
          resolve();
        },
      });
    });
  };

  const getSession = async () => {
    return new Promise((resolve, reject) => {
      const user = UserPool.getCurrentUser();
      if (user) {
        user.getSession((err, session) => {
          if (err) {
            setStatus(false);
            reject(err);
          } else {
            setStatus(true);
            resolve(session);
          }
        });
      } else {
        // reject(true);
      }
    });
  };

  const logout = () => {
    const user = UserPool.getCurrentUser();
    if (user) {
      user.signOut();
      setStatus(false);
      localStorage.removeItem('userEmail');
      console.log("Logged out");
    }
  };  

  const forgotPassword = async (email) => {
    return new Promise((resolve, reject) => {
      const user = new CognitoUser({
        Username: email,
        Pool: UserPool,
      });

      user.forgotPassword({
        onSuccess: () => {
          console.log("Code sent successfully");
          resolve();
        },
        onFailure: (err) => {
          console.error("Error in sending code:", err);
          reject(err);
        },
      });
    });
  };

  const confirmPassword = async (email, code, newPassword) => {
    return new Promise((resolve, reject) => {
      const user = new CognitoUser({
        Username: email,
        Pool: UserPool,
      });

      user.confirmPassword(code, newPassword, {
        onSuccess: () => {
          console.log("Password reset successfully");
          resolve();
        },
        onFailure: (err) => {
          console.error("Error in resetting password:", err);
          reject(err);
        },
      });
    });
  };

  return (
    <AuthContext.Provider
      value={{
        authenticate,
        getSession,
        logout,
        forgotPassword,
        confirmPassword,
        status,
        setStatus,
        role,
        setRole,
        numberOfFiles,
        setNumberOfFiles,
        email,
        setEmail,
        guestEmail,
        setGuestEmail,
      }}
    > 
      {children}
    </AuthContext.Provider>
  );
};

const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
};

export { Auth, AuthContext, useAuth };



