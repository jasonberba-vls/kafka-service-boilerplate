export const exceutionContext_SuccessfulRequest: any = {
    switchToHttp: () => ({
      getRequest: () => ({
        url: '/validUrl',
        body: {
            merchantCode: "validMerchantCode"
        },
        headers: {
        "cf-connecting-ip":"1.1.1.1",
        "x-forwarded-for":"1.1.1.1"
        }
      }),
    }),
  }
  
export const exceutionContext_SuccessfulRequest_HealthEndpoint: any = {
    switchToHttp: () => ({
      getRequest: () => ({
        url: '/health',
        body: {
            merchantCode: "validMerchantCode"
        },
        headers: {
        "cf-connecting-ip":"1.1.1.1",
        "x-forwarded-for":"1.1.1.1"
        }
      }),
    }),
  }

  export const exceutionContext_InvalidRequest_NoMerchantCode: any = {
    switchToHttp: () => ({
      getRequest: () => ({
        url: '/validUrl',
        body: {
            //merchantCode: "validMerchantCode"
        },
        headers: {
        "cf-connecting-ip":"1.1.1.1",
        "x-forwarded-for":"1.1.1.1"
        }
      }),
    }),
  }

export const exceutionContext_InvalidMerchantCodeRequest: any = {
    switchToHttp: () => ({
      getRequest: () => ({
        url: '/validUrl',
        body: {
            merchantCode: "ERRORMerchantCode"
        },
        headers: {
        "cf-connecting-ip":"1.1.1.1",
        "x-forwarded-for":"1.1.1.1"
        }
      }),
    }),
  }

export const exceutionContext_InvalidIPRequest: any = {
    switchToHttp: () => ({
      getRequest: () => ({
        url: '/validUrl',
        body: {
            merchantCode: "validMerchantCode"
        },
        headers: {
        "cf-connecting-ip":"6.9.6.9",
        "x-forwarded-for":"6.9.6.9"
        }
      }),
    }),
  }

export const exceutionContext_SuccessfulRequest_CryptoEndpoint: any = {
    switchToHttp: () => ({
      getRequest: () => ({
        url: '/api/crypto/coins/withdrawal/359837950',
        params: { id: '359837950' },
        body: {
            merchantCode: "validMerchantCode"
        },
        headers: {
        "cf-connecting-ip":"1.1.1.1",
        "x-forwarded-for":"1.1.1.1"
        }
      }),
    }),
  }