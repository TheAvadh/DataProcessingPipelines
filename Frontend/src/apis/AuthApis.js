import { getApi, postApi } from "./index"; 

const BASE_URL = process.env.REACT_APP_BASE_URL;


export const getUser = async (email) => { 
    const url = `${BASE_URL}/login/getUser?email=${email}`;
    return getApi(url);
};


export const postUser = async (data) => {
    console.log(data);
    const url = `${BASE_URL}/signup/postuser`;
    return postApi(url, data);
};


export const getQuestions = async () => {
    const url = `${BASE_URL}/signup/allques`;
    return getApi(url);
}


export const getOneQuestion = async (id) => {
    const url = `${BASE_URL}/login/oneques?id=${id}`;
    return getApi(url);
}


export const checkAnswer = async (data) => {
    const url = `${BASE_URL}/login/checkans`;
    return postApi(url, data);
}


export const postAnswers = async (data) => {
    console.log(data);
    const url = `${BASE_URL}/signup/postans`;
    return postApi(url, data);
}

export const verifyMathSkill = async (data) => {
    const url = `${BASE_URL}/login/verifyMathSkill`;
    return postApi(url, data);
};
