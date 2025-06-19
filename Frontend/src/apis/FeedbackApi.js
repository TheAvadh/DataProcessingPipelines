import { getApi, postApi } from "./index";

const BASE_URL = process.env.REACT_APP_BASE_URL;


export const getFeedback = async () => {
    const url = `${BASE_URL}/login/getFeedback`;
    return getApi(url);
};



export const postFeedback = async (data) => {
    console.log("Posting feedback data:", data);
    const url = `${BASE_URL}/login/postFeedback`;
    return postApi(url, data);
};
