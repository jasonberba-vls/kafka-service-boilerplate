import { ProcessedErrorReturn, ResponseLoggingType, HandledErrorForLogging, HandledErrorForResponse } from "../types/reponses";

function processRequestError(error: any): ProcessedErrorReturn {
    let returnData: ProcessedErrorReturn = new ProcessedErrorReturn();

    if (error.response?.data) {
        if (HandledErrorForLogging.includes(error.response.data)) {
            returnData.responseLoggingType = ResponseLoggingType.Message;
            returnData.message = '';
        }
        else if (HandledErrorForResponse.includes(error.response.data)) {
            returnData.responseLoggingType = ResponseLoggingType.Message;
            returnData.message = error.response.data;
        }
    }
    else {
        returnData.responseLoggingType = ResponseLoggingType.Error;
        returnData.message = '';
    }

    return returnData;
}

export {
    processRequestError
};
