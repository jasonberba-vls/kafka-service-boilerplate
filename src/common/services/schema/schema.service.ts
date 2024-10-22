import { Inject, Injectable } from '@nestjs/common';
import { InternalApiWrapper } from '../../../common/wrapper/internalApiWrapper';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class SchemaService {
  private SCHEMA_API_URL: string;
  constructor(private internalApiWrapper: InternalApiWrapper, 
                private configService: ConfigService) {
    this.SCHEMA_API_URL = this.configService.get('SCHEMA_API_URL');
  }

  async getEventSchema(schemaId: Number):  Promise<any> {
    const schemaApi = `${this.SCHEMA_API_URL}/ids/contentIds/${schemaId}`;
    let returnData: any;
  
      await this.internalApiWrapper
        .get(schemaApi)
        .then(function (response) {
          returnData = response.data;
        })
        .catch(function (error) {
          throw error;
        });

      return returnData;
  }
}
