import axios, { AxiosInstance} from 'axios';

const initialization = (): AxiosInstance => {
    const axiosInstance = axios.create();
    return axiosInstance;
};

export default initialization;