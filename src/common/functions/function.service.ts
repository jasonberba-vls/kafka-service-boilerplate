import dayjs from "dayjs";
// import { IPairMappingObject } from "src/resources/status-checker/status-checker.model";

export class FunctionService {
    merchantURL: string;
    constructor(merchantURL: string)
    {
        this.merchantURL = merchantURL;
    }
    
    getBankCodeIFSC(...params): string {
        return `${params[0].substring(0,4)}`;
    }

    getCallbackURL(...params): string {
        return `${this.merchantURL}/api/callback/withdrawal/${params[0]}`;
    }

    getCallbackRedirectURL(...params): string {
      return `${this.merchantURL}/api/callback/deposit/redirect/${params[0]}`;
    }

    getTransferURL(...params): string {
      return `${this.merchantURL}/landingpage/transfer`;
    }

    getSuccessURL(...params): string {
      return `${this.merchantURL}/landingpage/success`;
    }

    getFailedURL(...params): string {
      return `${this.merchantURL}/landingpage/failed`;
    }

    getTimeStamp(...params): string {
      return Date.now().toString();
    }

    getRandomId(...params): string {
      var result = '';
      var characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
      var charactersLength = characters.length;
      for ( var i = 0; i < params[0]; i++ ) {
          result += characters.charAt(Math.floor(Math.random() * charactersLength));
      }
      return result;
    }

    getDateToday(...params): string {
      return new Date().toISOString();
    }

    getDateIST(...params): string {
      const istDate = new Date(params[0]).toLocaleString('en-US', {timeZone: 'Asia/Kolkata'});
      return dayjs(istDate).format(params[1]);
    }

    getDateNowIST(...params): string {
      const istDate = new Date().toLocaleString('en-US', {timeZone: 'Asia/Kolkata'});
      return dayjs(istDate).format(params[0]);
    }

    getTurboPayQueryPayload(...params): string {
      //Initialize Body
      var body = {};
      body['orderId'] = params[0];
  
      //Compose Payload
      var payload = {};
      payload['timestamp'] = this.getTimeStamp();
      payload['body'] = body;
      
      return Buffer.from(JSON.stringify(payload)).toString('base64');
    }

    decodeToBase64(...params): string {
      return new Buffer(params[0], 'base64').toString();
    }

    compareJSONValues(...params): boolean {
      const arrayParams0: { key: string; value: any }[] = [];
            Object.entries(params[0]).forEach(([key, value]) => {
                if (value != undefined && value != null && value != ''){
                    arrayParams0.push({ key: key, value: value });
                  }
                }
            );

      var param1 = JSON.stringify(arrayParams0.sort((a, b) => a.key > b.key ? 1 : -1));

      const arrayParams1: { key: string; value: any }[] = [];
            Object.entries((params[1])).forEach(([key, value]) => {
                if (value != undefined && value != null && value != ''){
                    arrayParams1.push({ key: key, value: value });
                  }
                }
            );

      var param2 = JSON.stringify(arrayParams1.sort((a, b) => a.key > b.key ? 1 : -1));

      return param1 == param2;
    }

    convertToJason(...params): any {
      return JSON.parse(params[0]);
    }

    getDateTodayV2(...params) {
      var d = new Date(params[0]),
          month = '' + (d.getMonth() + 1),
          day = '' + d.getDate(),
          year = d.getFullYear();
  
      if (month.length < 2) 
          month = '0' + month;
      if (day.length < 2) 
          day = '0' + day;
  
      return [year, month, day].join('-');
    }

    concatinateValues(...params): string {
      var retValue = '';
      params.forEach(paramValue => {
        retValue += paramValue;
      });

      return retValue;
    }

    encodeToBase64(...params): string {
      return Buffer.from(params[0]).toString('base64');
    }

    // preparePairingSet(...params): any {
    //   var retValue: IPairMappingObject[] = [];

    //   //Prepare Key Value Pairing
    //   params.forEach(paramValue => {
    //     const pairField = paramValue.split('.')[0];
    //     const pairValue = paramValue.split('.')[1];

    //     let pairObject : IPairMappingObject =  {
    //       pairField : pairField,
    //       pairValue : pairValue
    //     }

    //     retValue.push(pairObject);
    //   });
  
    //   return retValue;
    // }
}