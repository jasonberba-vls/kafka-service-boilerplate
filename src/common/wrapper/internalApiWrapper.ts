import { Global, Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import initializeAxios from './axiosInstance';
import rTracer from 'cls-rtracer';

@Injectable()
export class InternalApiWrapper {
    axiosInstance = initializeAxios();;
    private REQUEST_API_TOKEN: string;
    constructor(configService: ConfigService) {
        this.REQUEST_API_TOKEN = configService.get('REQUEST_API_TOKEN');
    }
    
    get = <T>(url: string, queryParams?: object): Promise<T | any> => {
        this.axiosInstance.defaults.headers.common['x-request-id'] = rTracer.id() as any;
        this.axiosInstance.defaults.headers.common['x-request-api-token'] = this.REQUEST_API_TOKEN as any;
        return this.axiosInstance.get<T>(url, { params: queryParams });
    };

    post = <T>(url: string, body: object, queryParams?: object): Promise<T | any> => {
        this.axiosInstance.defaults.headers.common['x-request-id'] = rTracer.id() as any;
        this.axiosInstance.defaults.headers.common['x-request-api-token'] = this.REQUEST_API_TOKEN as any;
        return this.axiosInstance.post<T>(url, body, { params: queryParams });
    };

    put = <T>(url: string, body: object, queryParams?: object): Promise<T | any> => {
        this.axiosInstance.defaults.headers.common['x-request-id'] = rTracer.id() as any;
        this.axiosInstance.defaults.headers.common['x-request-api-token'] = this.REQUEST_API_TOKEN as any;
        return this.axiosInstance.put<T>(url, body, { params: queryParams });
    };
}