import axios from 'axios';

const getApi =(url)=>{
    return axios.get(url)
}

const postApi =(url, data)=>{
    return axios.post(url, data)
}

export {getApi, postApi}