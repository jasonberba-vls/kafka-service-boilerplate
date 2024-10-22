export const DynamicSettingType = {
    Request: 1,
    Callback: 2,
    Query: 3
  };
  
  export const DepositTransactionStatus = {
    PENDING: 1,
    PROCESSING: 2,
    SUCCESSFUL: 3,
    FAILED: 4,
    OVERDUE: 5,
    HOLD: 6
  };
  
  export const WithdrawalTransactionStatus = {
    PENDING: 1,
    PROCESSING: 2,
    SUCCESSFUL: 3,
    FAILED: 4,
    OVERDUE: 5,
    HOLD: 6
  };
  
  export const dynamicFieldTypes = {
    transactionDetails: 'transactionDetails',
    internal: 'internal',
    fixed: 'fixed',
    merchantSecretSetting: 'merchantSecretSetting',
    merchantSecretSettings: 'merchantSecretSettings',
    secretSettings: 'secretSettings',
    merchantSettings: 'merchantSettings',
    merchantSetting: 'merchantSetting',
    dataStorage: 'dataStorage',
    request: 'request',
    len4request: 'len4request',
    extraData: "extraData",
    vendorExtraData: "vendorExtraData",
    body: "body",
    responseSection: "responseSection",
    header: "header",
    steps: 'steps',
    pairMappingStorage: 'pairMappingStorage',
  };
  
  export const withdrawalStepType = {
    httpCall: 'httpCall',
    bodyValue: 'bodyValue'
  };
  
  export const withdrawalStepBodyType = {
    arrayJson: 'arrayJson'
  };
  
  export const Hashing = {
    Sha256: 'SHA256',
    Sha512: 'SHA512',
    MD5: 'MD5',
    HMAC_SHA256: 'HMAC_SHA256',
    HMAC_SHA512: 'HMAC_SHA512',
    AesEcbPKCS5Padding: 'AesEcbPKCS5Padding',
    Aes256Cbc: 'Aes256Cbc',
    Aes256: 'Aes256',
    PayfairHashing: 'PayfairHashing',
    Sha1WithRsa: 'Sha1WithRsa',
    AesCbcPKCS5Padding: 'AesCbcPKCS5Padding',
    DuniaPayHashing: 'DuniaPayHashing',
    AirpayRequestHashing: 'AirpayRequestHashing',
  };
  
  export const FormatType = {
    JSON: 'json',
    Xml: 'xml'
  };
  
  export const ValidationType = {
    Normal: 'Normal',
    Alphabetical: 'Alphabetical'
  };
  
  export const CompositionDefaultField = {
    SecretKey: 'SecretKey',
    Response: 'Response'
  };
  
  export const ReservedWords = {
    CallbackURL: 'CallbackURL',
    TimeStamp: 'TimeStamp',
    SignValue: 'SignValue',
    RequestAmount: 'requestAmount'
  };
  
  export const InternalType = {
    StatusResponse: 'Status',
    MessageResponse: 'Message'
  };
  
  export const FunctionChecker = {
    InternalStatusChecker: 'InternalStatusChecker',
    AmountIsLessThan1: 'AmountIsLessThan1',
    CurrencyChecker: 'CurrencyChecker',
    VendorReferenceChecker: 'VendorReferenceChecker'
  };
  
  export const GatewayCategory = {
    BANK: "1",
    ONLINE: "2"
  };
  
  export const SectionLocation = {
    Response: "response",
    ONLINE: "2"
  };
  
  export const ResponseType = {
    Redirect: "Redirect",
    Url: "Url",
    MultipleStepsUrl: "MultipleStepsUrl",
    Html: "Html",
    Data: "Data",
    MultipleStepsRedirect: "MultipleStepsRedirect",
    XurpayManual: "XurpayManual",
    XurpayDummy: "XurpayDummy",
    DataUrl: "DataUrl",
    DataHtml: "DataHtml",
  };