const partitionMapping : any = {
    "PartitionMapping":  [
            {
                "Partition": 0,
                "MerchantIdList": [1,2,3]
            },
            {
                "Partition": 1,
                "MerchantIdList": [4,5,6]
            }
        ]
  }

function merchantPartitioner(kafkaMessage: any): number {
    let returnValue: any
    let message: any = kafkaMessage.message;
    let partitionCount: any = kafkaMessage.partitionMetadata.length;

    // console.log('merchantPartitioner kafkaMessage.message : ', message);
    // console.log('merchantPartitioner partitionCount : ', partitionCount);

    // Logic will be based on merchantId being passed as the message Key
    if (message.key) {
        //console.log('merchantPartitioner partitionMapping', partitionMapping);

        // Find the appropriate partition based on the merchantId
        for (const mapping of partitionMapping.PartitionMapping) {
            if (mapping.MerchantIdList.includes(message.key)) {
                return mapping.Partition; // Return the corresponding partition
            }
        }
    }

    // Round Robin Logic
    returnValue = Math.floor(Math.random() * partitionCount);;
    // console.log('returnValue', returnValue)
    return returnValue;
};

export {
    merchantPartitioner,
};