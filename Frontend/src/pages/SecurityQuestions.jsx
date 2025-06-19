import React, { useEffect } from 'react'
import SecurityQuestionsComponent from '../components/SecurityQuestionsComponent'
import { useLocation } from 'react-router-dom';

const SecurityQuestions = () => {
  const location = useLocation();
  const email = location.state?.email || '';
  const name = location.state?.name || '';
  useEffect(() =>{
    console.log(location);
  console.log(email, name); 
  }, [location]);
  return (
    <>
      <SecurityQuestionsComponent email={email} name={name}/>
    </>
  )
}

export default SecurityQuestions
